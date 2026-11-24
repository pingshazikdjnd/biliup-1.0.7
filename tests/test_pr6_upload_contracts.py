import asyncio
import copy
import io
import json
import os
import sys
import tempfile
import time
import types
import unittest
from unittest.mock import MagicMock, patch

import aiohttp
import requests

from biliup.config import config
from biliup.engine.upload import UploadBase


def _bootstrap_biliup():
    orig_get_running_loop = asyncio.get_running_loop
    temp_loop = asyncio.new_event_loop()

    def _safe_get_running_loop():
        try:
            return orig_get_running_loop()
        except RuntimeError:
            return temp_loop

    asyncio.get_running_loop = _safe_get_running_loop
    try:
        if 'streamers' not in config:
            config['streamers'] = {}
        import biliup.app as app_mod
        import biliup.uploader as uploader_mod
        import biliup.plugins.biliuprs as biliuprs_mod
        import biliup.plugins.stream_gears as sg_mod
        import biliup.plugins.bili_webup as webup_mod
        return app_mod, uploader_mod, biliuprs_mod, sg_mod, webup_mod
    finally:
        asyncio.get_running_loop = orig_get_running_loop
        temp_loop.close()


app_mod, uploader_mod, biliuprs_mod, sg_mod, webup_mod = _bootstrap_biliup()


class TestPR6UploadContracts(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._config_snapshot = {
            k: (v if k == 'PluginInfo' else copy.deepcopy(v))
            for k, v in config.data.items()
        }
        self._upload_filename_snapshot = list(app_mod.context.get('upload_filename', []))
        self._tmpdir = tempfile.TemporaryDirectory()

    def tearDown(self):
        self._tmpdir.cleanup()
        app_mod.context['upload_filename'] = self._upload_filename_snapshot
        config.data.clear()
        config.data.update(self._config_snapshot)

    def _base_data(self):
        return {
            'name': 'streamer_a',
            'format_title': 'My Stream Title',
            'url': 'https://live.test/room',
        }

    def test_interaction_flags_propagated_across_plugins(self):
        file_item = UploadBase.FileInfo(video='demo.flv', danmaku=None)

        # 1. biliuprs -> upload_args + stream_gears_upload -> stream_gears.upload(bool)
        captured_process_kwargs = {}

        class FakeConn:
            def __init__(self):
                self.closed = False

            def poll(self):
                return False

            def close(self):
                self.closed = True

        class FakeProcess:
            def __init__(self, target=None, daemon=None, kwargs=None):
                captured_process_kwargs['target'] = target
                captured_process_kwargs['kwargs'] = dict(kwargs or {})
                self.exitcode = 0

            def start(self):
                pass

            def join(self):
                pass

        fake_ctx = types.SimpleNamespace(Process=FakeProcess)
        parent_conn, child_conn = FakeConn(), FakeConn()

        up_rs = biliuprs_mod.BiliWeb(
            'streamer_a',
            self._base_data(),
            dtime=3600,
            up_close_reply=1,
            up_selection_reply=0,
            up_close_danmu=1,
        )
        with patch.object(biliuprs_mod.mp, 'Pipe', return_value=(parent_conn, child_conn)), \
             patch.object(biliuprs_mod.mp, 'get_context', return_value=fake_ctx):
            res = up_rs.upload([file_item])

        self.assertEqual(res, [file_item])
        self.assertTrue(parent_conn.closed)
        self.assertTrue(child_conn.closed)
        upload_kwargs = captured_process_kwargs['kwargs']
        self.assertIs(upload_kwargs['up_close_reply'], True)
        self.assertIs(upload_kwargs['up_selection_reply'], False)
        self.assertIs(upload_kwargs['up_close_danmu'], True)
        self.assertIsNotNone(upload_kwargs['dtime'])

        sg_calls = []
        with patch.object(biliuprs_mod.stream_gears, 'upload', side_effect=lambda *a, **kw: sg_calls.append(kw)):
            biliuprs_mod.stream_gears_upload(
                upload_kwargs['ex_conn'],
                upload_kwargs['lines'],
                **{k: v for k, v in upload_kwargs.items() if k not in ('ex_conn', 'lines')},
            )
        self.assertIs(sg_calls[0]['up_close_reply'], True)
        self.assertIs(sg_calls[0]['up_selection_reply'], False)
        self.assertIs(sg_calls[0]['up_close_danmu'], True)

        # 2. stream_gears plugin 兼容 str 列表与 FileInfo 列表并透传 bool 互动字段
        sg_plugin = sg_mod.BiliWeb(
            'streamer_a',
            self._base_data(),
            up_close_reply=1,
            up_selection_reply=1,
            up_close_danmu=0,
        )
        sg_calls.clear()
        with patch.object(sg_mod.stream_gears, 'upload', side_effect=lambda *a, **kw: sg_calls.append(kw)):
            sg_plugin.upload(['manual_clip.mp4'])
            sg_plugin.upload([file_item])
        self.assertEqual(sg_calls[0]['video_path'], ['manual_clip.mp4'])
        self.assertEqual(sg_calls[1]['video_path'], ['demo.flv'])
        self.assertIs(sg_calls[0]['up_close_reply'], True)
        self.assertIs(sg_calls[0]['up_selection_reply'], True)
        self.assertIs(sg_calls[0]['up_close_danmu'], False)

        # 3. bili_webup.BiliWeb user 可选且透传互动字段至 Data
        captured_videos = []

        class FakeWebBili:
            def __init__(self, video):
                captured_videos.append(video)

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def login(self, *args):
                pass

            def upload_file(self, filepath, lines, threads):
                return {'title': 'clip', 'filename': 'fn'}

            def submit(self, submit_api):
                return {'code': 0, 'data': {'aid': 1}}

        web_plugin = webup_mod.BiliWeb(
            'streamer_a',
            self._base_data(),
            up_close_reply=1,
            up_selection_reply=0,
            up_close_danmu=1,
        )
        self.assertEqual(web_plugin.user, {})
        with patch.object(webup_mod, 'BiliBili', FakeWebBili):
            web_plugin.upload([file_item])
        self.assertIs(captured_videos[0].up_close_reply, True)
        self.assertIs(captured_videos[0].up_selection_reply, False)
        self.assertIs(captured_videos[0].up_close_danmu, True)

    def test_biliuprs_season_matches_non_first_archive_with_dtime(self):
        cookie_path = os.path.join(self._tmpdir.name, 'cookie.json')
        with open(cookie_path, 'w', encoding='utf-8') as f:
            json.dump({'cookie_info': {'cookies': [{'name': 'bili_jct', 'value': 'csrf_1'}]}}, f)

        now_ts = int(time.time())
        archives_payload = {
            'code': 0,
            'data': {
                'arc_audits': [
                    {
                        'Archive': {'aid': 1001, 'title': 'Unrelated Stream', 'ctime': now_ts},
                        'cid_list': [2001],
                        'Videos': [],
                    },
                    {
                        'Archive': {'aid': 1002, 'title': 'My Stream Title', 'ctime': now_ts - 600},
                        'cid_list': [2002],
                        'Videos': [],
                    },
                    {
                        'Archive': {'aid': 1004, 'title': 'My Stream Title', 'ctime': now_ts - 8},
                        'cid_list': [],
                        'Videos': [{}],
                    },
                    {
                        'Archive': {'aid': 1003, 'title': 'My Stream Title', 'ctime': now_ts - 5},
                        'cid_list': [2003],
                        'Videos': [],
                    },
                ]
            },
        }

        added_seasons = []
        session_headers = {}

        class FakeSession:
            def __init__(self):
                self.cookies = requests.cookies.RequestsCookieJar()
                self.headers = session_headers

            def get(self, url, params=None, timeout=None):
                return types.SimpleNamespace(json=lambda: archives_payload)

        class FakeSeasonBili:
            def __init__(self, video):
                self._BiliBili__session = FakeSession()
                self._BiliBili__bili_jct = None

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def add_to_season(self, section_id, episodes):
                added_seasons.append((section_id, episodes, dict(session_headers)))
                return {'code': 0}

        class FakeConn:
            def poll(self):
                return False

            def close(self):
                pass

        class FakeProcess:
            def __init__(self, **kwargs):
                self.exitcode = 0

            def start(self):
                pass

            def join(self):
                pass

        up = biliuprs_mod.BiliWeb(
            'streamer_a',
            self._base_data(),
            dtime=7200,
            user_cookie=cookie_path,
            section_id=5566,
        )
        file_item = UploadBase.FileInfo(video='demo.flv', danmaku=None)
        with patch.object(biliuprs_mod.mp, 'Pipe', return_value=(FakeConn(), FakeConn())), \
             patch.object(biliuprs_mod.mp, 'get_context', return_value=types.SimpleNamespace(Process=FakeProcess)), \
             patch.object(biliuprs_mod.time, 'sleep'), \
             patch.object(webup_mod, 'BiliBili', FakeSeasonBili):
            res = up.upload([file_item])

        self.assertEqual(res, [file_item])
        self.assertEqual(len(added_seasons), 1)
        sec_id, episodes, hdrs = added_seasons[0]
        self.assertEqual(sec_id, 5566)
        self.assertEqual(
            episodes,
            [{'aid': 1003, 'cid': 2003, 'title': 'My Stream Title', 'charging_pay': 0}],
        )
        self.assertEqual(hdrs.get('Origin'), 'https://member.bilibili.com')
        self.assertIn('member.bilibili.com', hdrs.get('Referer', ''))

    async def test_bili_webup_probe_chunk_and_upos_raise_on_exhaustion(self):
        bili = webup_mod.BiliBili(webup_mod.Data())
        session = bili._BiliBili__session

        # 1. probe 跳过非 200 和连接异常，命中后续正常线路
        probe_resp = types.SimpleNamespace(
            json=lambda: {
                'probe': {'get': True},
                'lines': [
                    {'probe_url': '//line1.example/probe', 'query': 'q1'},
                    {'probe_url': '//line2.example/probe', 'query': 'q2'},
                    {'probe_url': '//line3.example/probe', 'query': 'q3'},
                ],
            }
        )

        def fake_request(method, url, **kwargs):
            if 'line1' in url:
                return types.SimpleNamespace(status_code=503)
            if 'line2' in url:
                raise requests.ConnectionError('line2 down')
            return types.SimpleNamespace(status_code=200)

        with patch.object(session, 'get', return_value=probe_resp), \
             patch.object(session, 'request', side_effect=fake_request):
            best = bili.probe()
        self.assertEqual(best['query'], 'q3')
        self.assertIn('cost', best)

        # 全线路失败抛 RuntimeError
        with patch.object(session, 'get', return_value=probe_resp), \
             patch.object(session, 'request', return_value=types.SimpleNamespace(status_code=500)):
            with self.assertRaises(RuntimeError):
                bili.probe()

        # 2. _upload 10 次分片重试失败后抛 RuntimeError
        chunk_attempts = 0

        async def always_fail_chunk(*args, **kwargs):
            nonlocal chunk_attempts
            chunk_attempts += 1
            raise aiohttp.ClientError('chunk failed')

        with self.assertRaises(RuntimeError):
            await bili._upload({}, io.BytesIO(b'123456'), 4, always_fail_chunk, tasks=1)
        self.assertEqual(chunk_attempts, 10)

        # 3. upos 合并重试耗尽后抛 RuntimeError
        merge_attempts = 0

        def fake_upos_post(url, params=None, json=None, headers=None, timeout=None):
            nonlocal merge_attempts
            if 'uploads' in url:
                return types.SimpleNamespace(json=lambda: {'upload_id': 'uid_1'})
            merge_attempts += 1
            return types.SimpleNamespace(json=lambda: {'OK': 0, 'error': 'merge denied'})

        ret = {
            'chunk_size': 4,
            'auth': 'tok',
            'endpoint': '//upos.example',
            'biz_id': 1,
            'upos_uri': 'upos://ugcupos/test.mp4',
        }
        buf = io.BytesIO(b'1234')
        buf.name = 'test.mp4'
        with patch.object(bili, '_upload', side_effect=lambda *a, **kw: asyncio.sleep(0)), \
             patch.object(session, 'post', side_effect=fake_upos_post), \
             patch.object(webup_mod.time, 'sleep') as sleep_mock:
            with self.assertRaises(RuntimeError):
                await bili.upos(buf, 4, ret, tasks=1)
        self.assertEqual(merge_attempts, 6)
        self.assertEqual(sleep_mock.call_count, 5)
        bili.close()

    def test_credits_to_desc_v2_handles_fewer_placeholders(self):
        credits = [
            {'username': 'alice', 'uid': 1},
            {'username': 'bob', 'uid': 2},
        ]
        plugins = [
            biliuprs_mod.BiliWeb('s', self._base_data(), description='hi @credit tail', credits=credits),
            sg_mod.BiliWeb('s', self._base_data(), description='hi @credit tail', credits=credits),
            webup_mod.BiliWeb('s', self._base_data(), description='hi @credit tail', credits=credits),
        ]
        for pl in plugins:
            desc_v2 = pl.creditsToDesc_v2()
            self.assertEqual(
                desc_v2,
                [
                    {'raw_text': 'hi ', 'biz_id': '', 'type': 1},
                    {'raw_text': 'alice', 'biz_id': '1', 'type': 2},
                    {'raw_text': '  tail', 'biz_id': '', 'type': 1},
                ],
            )

    def test_biliup_uploader_config_merge_empty_reject_and_lock_cleanup(self):
        # 1. 空文件列表直接返回 None
        self.assertIsNone(uploader_mod.biliup_uploader([], {'name': 'tpl1'}))

        # 2. 合并全局 config、忽略 data 中的 None、锁定 upload_filename 并异常清理
        config.data.update({
            'lines': 'qn',
            'threads': 6,
            'submit_api': 'client',
            'description': 'global desc',
            'dynamic': 'global dyn',
            'dolby': 1,
            'hires': 1,
            'no_reprint': 1,
            'charging_pay': 1,
            'is_only_self': 1,
        })
        busy_key = os.path.normpath('dir/busy')
        app_mod.context['upload_filename'] = [busy_key]

        captured = {}

        class FakeManualPlugin:
            def __init__(
                self,
                index,
                data,
                lines='AUTO',
                threads=3,
                submit_api='web',
                description='default desc',
                dynamic='default dyn',
                **kwargs,
            ):
                captured['index'] = index
                captured['data'] = dict(data)
                captured['lines'] = lines
                captured['threads'] = threads
                captured['submit_api'] = submit_api
                captured['description'] = description
                captured['dynamic'] = dynamic

            def upload(self, file_list):
                captured['files'] = list(file_list)
                captured['locked_during_upload'] = list(app_mod.context['upload_filename'])
                if len(file_list) > 0 and 'boom' in str(file_list[0]):
                    raise RuntimeError('manual upload boom')
                return file_list

        params = {
            'name': 'tpl1',
            'uploader': 'fake_manual',
            'lines': '',
            'threads': None,
            'submit_api': '',
            'description': '',
            'dynamic': '',
        }
        expected_free = UploadBase.FileInfo(video='./free.mp4', danmaku=None)
        with patch.dict(uploader_mod.Plugin.upload_plugins, {'fake_manual': FakeManualPlugin}):
            res = uploader_mod.biliup_uploader(['dir/busy.mp4', './free.mp4', 'free.mkv'], dict(params))
            self.assertEqual(res, [expected_free])
            self.assertIsInstance(captured['files'][0], UploadBase.FileInfo)
            self.assertEqual(captured['lines'], 'qn')
            self.assertEqual(captured['threads'], 6)
            self.assertEqual(captured['submit_api'], 'client')
            self.assertEqual(captured['description'], '')
            self.assertEqual(captured['dynamic'], '')
            self.assertEqual(captured['data']['dolby'], 1)
            self.assertEqual(json.loads(captured['data']['extra_fields']), {'is_only_self': 1})
            self.assertEqual(captured['locked_during_upload'], [busy_key, 'free'])
            self.assertEqual(app_mod.context['upload_filename'], [busy_key])

            # 异常后也要清理本次注册的锁文件，保留原有 locked 文件
            res_err = uploader_mod.biliup_uploader(['dir/boom.mp4'], dict(params))
            self.assertIsNone(res_err)
            self.assertEqual(app_mod.context['upload_filename'], [busy_key])

    def test_upload_base_start_skips_file_concurrently_claimed_before_lock(self):
        app_mod.context['upload_filename'] = []
        file_claimed = UploadBase.FileInfo(video='./race.mp4', danmaku=None)
        file_ok = UploadBase.FileInfo(video='ok.mp4', danmaku=None)
        seen_uploads = []

        class RaceUploader(UploadBase):
            def upload(self, file_list):
                seen_uploads.append(list(file_list))
                return file_list

        def _fake_file_list(index):
            # 模拟 file_list 扫描完成后、start() 进锁占位前，手动上传并发占用了 race.mp4（不同路径写法）
            app_mod.context['upload_filename'].append('race')
            return [file_claimed, file_ok]

        up = RaceUploader('race_streamer', {'format_title': 'race_title'})
        with patch.object(UploadBase, 'file_list', side_effect=_fake_file_list):
            res = up.start()

        self.assertEqual(res, [file_ok])
        self.assertEqual(seen_uploads, [[file_ok]])
        self.assertEqual(app_mod.context['upload_filename'], ['race'])


if __name__ == '__main__':
    unittest.main()
