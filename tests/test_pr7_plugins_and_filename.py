import asyncio
import copy
import json
import os
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from biliup.config import config


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
        import biliup.handler as handler_mod
        import biliup.common.util as util_mod
        from biliup.engine.download import DownloadBase
        import biliup.plugins.bilibili as bilibili_mod
        import biliup.plugins.huya as huya_mod
        import biliup.plugins.douyin as douyin_mod
        import biliup.plugins.kuaishou as kuaishou_mod
        import biliup.plugins.twitcasting as twitcasting_mod
        import biliup.plugins.missevan as missevan_mod
        import biliup.plugins.kilakila as kilakila_mod
        import biliup.plugins.nico as nico_mod
        import biliup.plugins.youtube as youtube_mod
        import biliup.plugins.douyu as douyu_mod
        return (
            app_mod,
            handler_mod,
            util_mod,
            DownloadBase,
            bilibili_mod,
            huya_mod,
            douyin_mod,
            kuaishou_mod,
            twitcasting_mod,
            missevan_mod,
            kilakila_mod,
            nico_mod,
            youtube_mod,
            douyu_mod,
        )
    finally:
        asyncio.get_running_loop = orig_get_running_loop
        temp_loop.close()


(
    app_mod,
    handler_mod,
    util_mod,
    DownloadBase,
    bilibili_mod,
    huya_mod,
    douyin_mod,
    kuaishou_mod,
    twitcasting_mod,
    missevan_mod,
    kilakila_mod,
    nico_mod,
    youtube_mod,
    douyu_mod,
) = _bootstrap_biliup()


class TestPR7SchedulerAndFilename(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._orig_cwd = os.getcwd()
        self._tmpdir = tempfile.TemporaryDirectory()
        os.chdir(self._tmpdir.name)
        self._config_snapshot = {
            k: (v if k == 'PluginInfo' else copy.deepcopy(v))
            for k, v in config.data.items()
        }
        self._pending_snapshot = set(app_mod.context.get('pending_download_urls', set()))
        self._plugin_info_snapshot = app_mod.context.get('PluginInfo')
        config.data.clear()
        config.data.update({
            'streamers': {},
            'url_upload_count': {},
            'upload_filename': [],
            'file_upload_count': {},
            'sync_downloader_map': {},
            'pending_download_urls': set(),
        })

    async def asyncTearDown(self):
        os.chdir(self._orig_cwd)
        self._tmpdir.cleanup()
        app_mod.context['pending_download_urls'] = set(self._pending_snapshot)
        if self._plugin_info_snapshot is not None:
            app_mod.context['PluginInfo'] = self._plugin_info_snapshot
        else:
            app_mod.context.pop('PluginInfo', None)
        config.data.clear()
        config.data.update(self._config_snapshot)

    async def test_singleton_check_prevents_reentrancy_and_resets_on_failure(self):
        url = 'https://example.com/room/1'
        name = 'streamer_a'
        app_mod.context['PluginInfo'] = SimpleNamespace(
            url_status={url: 0},
            inverted_index={url: name},
        )
        config.data['streamers'] = {name: {'url': url}}

        check_calls = 0

        class FakePlatform:
            def __init__(self, fname, u):
                self.fname = fname
                self.url = u

            async def acheck_stream(self, is_check=True):
                nonlocal check_calls
                check_calls += 1
                return True

            def should_record(self):
                return True

        sent_events = []
        with patch.object(app_mod.event_manager, 'send_event', side_effect=lambda ev: sent_events.append(ev)):
            await app_mod.singleton_check(FakePlatform, name, url)
            # Pending state blocks duplicate check/dispatch before pre_processor runs
            await app_mod.singleton_check(FakePlatform, name, url)

        self.assertEqual(check_calls, 1)
        pre_events = [e for e in sent_events if e.type_ == handler_mod.PRE_DOWNLOAD]
        self.assertEqual(len(pre_events), 1)
        self.assertIn(url, app_mod.context['pending_download_urls'])

        # If send_event(PRE_DOWNLOAD) raises, pending_download_urls is cleaned up
        app_mod.context['pending_download_urls'].clear()

        def _send_fail(ev):
            if ev.type_ == handler_mod.PRE_DOWNLOAD:
                raise RuntimeError('queue full')

        with patch.object(app_mod.event_manager, 'send_event', side_effect=_send_fail):
            with self.assertRaises(RuntimeError):
                await app_mod.singleton_check(FakePlatform, name, url)
        self.assertNotIn(url, app_mod.context['pending_download_urls'])

        # pre_processor transitions pending -> url_status = 1 atomically
        app_mod.context['pending_download_urls'].add(url)
        gen = handler_mod.pre_processor.__wrapped__(name, url)
        ev = next(gen)
        self.assertEqual(ev.type_, handler_mod.DOWNLOAD)
        self.assertEqual(app_mod.context['PluginInfo'].url_status[url], 1)
        self.assertNotIn(url, app_mod.context['pending_download_urls'])

        # Duplicate pre_processor sees url_status == 1 and exits early
        self.assertEqual(list(handler_mod.pre_processor.__wrapped__(name, url)), [])

        # process() resets url_status and discards pending in finally
        app_mod.context['pending_download_urls'].add(url)
        with patch.object(handler_mod, 'biliup_download', side_effect=RuntimeError('dl fail')):
            with patch.object(handler_mod, 'check_timerange', return_value=True):
                list(handler_mod.process.__wrapped__(name, url))
        self.assertEqual(app_mod.context['PluginInfo'].url_status[url], 0)
        self.assertNotIn(url, app_mod.context['pending_download_urls'])

        # If preprocessor fails, pre_processor resets url_status instead of leaking 1
        config.data['streamers'][name]['preprocessor'] = 'echo fail'
        with patch.object(handler_mod, 'processor', side_effect=RuntimeError('prep fail')):
            with patch.object(handler_mod, 'check_timerange', return_value=True):
                with self.assertRaises(RuntimeError):
                    list(handler_mod.pre_processor.__wrapped__(name, url))
        self.assertEqual(app_mod.context['PluginInfo'].url_status[url], 0)

    async def test_shot_advances_index_in_finally_when_exception_occurs(self):
        event = SimpleNamespace(url_list=['url_1', 'url_2'])
        app_mod.context['PluginInfo'] = SimpleNamespace(
            inverted_index={'url_1': 'A', 'url_2': 'B'},
            url_status={'url_1': 0, 'url_2': 0},
        )
        visited = []

        async def _fake_singleton_check(ev, name, url):
            visited.append(url)
            if url == 'url_1':
                raise RuntimeError('network glitch')
            raise asyncio.CancelledError()

        with patch.object(app_mod, 'singleton_check', side_effect=_fake_singleton_check):
            with patch.object(app_mod.asyncio, 'sleep', new=AsyncMock()):
                with self.assertRaises(asyncio.CancelledError):
                    await app_mod.shot(event)

        self.assertEqual(visited, ['url_1', 'url_2'])

    def test_gen_download_filename_escapes_percent_signs(self):
        class DummyDL(DownloadBase):
            async def acheck_stream(self, is_check=False):
                return True

        dl = DummyDL('主播100%测试', 'https://example.com/1', suffix='flv')
        dl.room_title = '标题%s%Y%q'
        dl.filename_prefix = '{streamer}-{title}-%Y'

        fmt_name = dl.gen_download_filename(is_fmt=True)
        self.assertIn('主播100%测试', fmt_name)
        self.assertIn('标题%s%Y%q', fmt_name)

        raw_tmpl = dl.gen_download_filename(is_fmt=False)
        self.assertIn('100%%', raw_tmpl)
        self.assertIn('%%s%%Y%%q', raw_tmpl)

        dl_default = DummyDL('foo%z', 'https://example.com/2', suffix='flv')
        dl_default.filename_prefix = ''
        fmt_default = dl_default.gen_download_filename(is_fmt=True)
        self.assertTrue(fmt_default.startswith('foo%z'))


class TestPR7Plugins(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._orig_cwd = os.getcwd()
        self._tmpdir = tempfile.TemporaryDirectory()
        os.chdir(self._tmpdir.name)
        self._config_snapshot = {
            k: (v if k == 'PluginInfo' else copy.deepcopy(v))
            for k, v in config.data.items()
        }
        self._penalty_snapshot = copy.deepcopy(bilibili_mod._BILIUP_PENALTY_STATE)
        self._douyu_cache_snapshot = dict(douyu_mod._REAL_RID_CACHE)
        config.data.clear()
        config.data['streamers'] = {}
        bilibili_mod._BILIUP_PENALTY_STATE.clear()
        douyu_mod._REAL_RID_CACHE.clear()

    async def asyncTearDown(self):
        os.chdir(self._orig_cwd)
        self._tmpdir.cleanup()
        bilibili_mod._BILIUP_PENALTY_STATE.clear()
        bilibili_mod._BILIUP_PENALTY_STATE.update(self._penalty_snapshot)
        douyu_mod._REAL_RID_CACHE.clear()
        douyu_mod._REAL_RID_CACHE.update(self._douyu_cache_snapshot)
        config.data.clear()
        config.data.update(self._config_snapshot)

    async def test_bilibili_dynamic_override_penalty_transcode_and_parsers(self):
        config.data.update({
            'bili_liveapi': 'https://global.api/',
            'bili_fallback_api': 'https://global.fb/',
            'bili_hls_transcode_timeout': 60,
            'bili_qn': 10000,
            'bili_protocol': 'hls_fmp4',
            'bili_wait_transcode': True,
            'bili_wait_transcode_timeout': 120,
        })
        bili = bilibili_mod.Bililive('up', 'https://live.bilibili.com/12345')
        # Simulate Downloader override mutation applied after __init__
        bili.__dict__.update({
            'bili_liveapi': 'https://override.api/',
            'bili_fallback_api': 'https://override.fb/',
            'bili_hls_transcode_timeout': 10,
            'bili_qn': '400',
        })
        self.assertEqual(bili.bili_api_list, ['https://override.api', 'https://override.fb'])
        self.assertEqual(bili.bili_hls_timeout, 10)

        # Penalty state Level 0 -> Level 1 reverses API order -> Level 2 keeps official first
        bili._Bililive__real_room_id = 12345
        called_apis = []

        async def _fake_get_play_info(api, qn):
            called_apis.append(api)
            return None

        with patch.object(bili, 'get_play_info', side_effect=_fake_get_play_info):
            await bili.aget_stream(400, 'stream', 0)
            self.assertEqual(called_apis, ['https://override.api', 'https://override.fb'])

            called_apis.clear()
            bilibili_mod._BILIUP_PENALTY_STATE['12345'] = {'level': 1, 'last_time': 1000}
            await bili.aget_stream(400, 'stream', 0)
            self.assertEqual(called_apis, ['https://override.fb', 'https://override.api'])

            called_apis.clear()
            bilibili_mod._BILIUP_PENALTY_STATE['12345'] = {'level': 2, 'last_time': 1000}
            await bili.aget_stream(400, 'stream', 0)
            self.assertEqual(called_apis, ['https://override.api', 'https://override.fb'])

        # hls_fmp4 timeout fallback returning empty dict must return False cleanly
        bilibili_mod._BILIUP_PENALTY_STATE.clear()
        room_json = {
            'code': 0,
            'data': {
                'room_info': {
                    'live_status': 1,
                    'cover': 'c',
                    'title': 't',
                    'room_id': 12345,
                    'uid': 99,
                    'live_start_time': 100,
                    'special_type': 0,
                }
            }
        }
        fake_resp = MagicMock()
        fake_resp.json.return_value = room_json
        fake_resp.raise_for_status = MagicMock()
        with patch.object(bilibili_mod.wbi, 'sign'):
            with patch.object(bilibili_mod.client, 'get', new=AsyncMock(return_value=fake_resp)):
                with patch.object(bili, 'update_wbi', new=AsyncMock()):
                    with patch.object(bili, 'check_login_status', new=AsyncMock(return_value=1)):
                        with patch.object(bili, 'aget_stream', new=AsyncMock(return_value={})):
                            with patch.object(bilibili_mod.asyncio, 'sleep', new=AsyncMock()):
                                self.assertFalse(await bili.acheck_stream(is_check=False))

            # bili_wait_transcode preserved & bili_qn string coerced to int 400
            bili2 = bilibili_mod.Bililive('up2', 'https://live.bilibili.com/12346')
            bili2.__dict__.update({
                'bili_qn': '400',
                'bili_protocol': 'stream',
                'bili_wait_transcode': True,
                'bili_wait_transcode_timeout': 120,
            })
            room_json['data']['room_info']['room_id'] = 12346
            stream_attempts = [
                {10000: {'tx': {'url': {'host': 'https://h', 'base_url': '/b?', 'extra': 'e'}}}},
                {400: {'tx': {'url': {'host': 'https://h', 'base_url': '/b400?', 'extra': 'e'}}}},
            ]
            with patch.object(bilibili_mod.client, 'get', new=AsyncMock(return_value=fake_resp)):
                with patch.object(bili2, 'update_wbi', new=AsyncMock()):
                    with patch.object(bili2, 'check_login_status', new=AsyncMock(return_value=1)):
                        with patch.object(bili2, 'aget_stream', new=AsyncMock(side_effect=stream_attempts)):
                            with patch.object(bilibili_mod.asyncio, 'sleep', new=AsyncMock()) as sleep_mock:
                                ok = await bili2.acheck_stream(is_check=False)
                                self.assertTrue(ok)
                                sleep_mock.assert_awaited_once_with(15)
                                self.assertEqual(bili2.raw_stream_url, 'https://h/b400?e')

        # parse_stream_url supports both http:// and https:// (plus queryless URLs)
        parsed_http = bili.parse_stream_url('http://cdn.example.com/live-bvc/1/live_123.m3u8?cdn=tx&suffix=origin')
        self.assertEqual(parsed_http['url']['host'], 'http://cdn.example.com')
        self.assertEqual(parsed_http['url']['base_url'], '/live-bvc/1/live_123.m3u8?')
        self.assertEqual(parsed_http['url']['extra'], 'cdn=tx&suffix=origin')

        parsed_no_q = bili.parse_stream_url('http://cdn.example.com/live-bvc/1/live_123.flv')
        self.assertEqual(parsed_no_q['url']['host'], 'http://cdn.example.com')
        self.assertEqual(parsed_no_q['url']['base_url'], '/live-bvc/1/live_123.flv')
        self.assertEqual(parsed_no_q['url']['extra'], '')

        # parse_master_m3u8 handles empty input and missing BILI-QN
        with self.assertRaises(ValueError):
            bili.parse_master_m3u8('')
        m3u8_no_qn = (
            '#EXTM3U\n'
            '#EXT-X-STREAM-INF:CODECS="avc1.640028"\n'
            'https://cdn.example.com/stream.m3u8?cdn=tx\n'
        )
        self.assertEqual(bili.parse_master_m3u8(m3u8_no_qn), {})

    async def test_huya_none_cdn_empty_streams_and_missing_fallback_cdn(self):
        config.data['huya_cdn'] = None
        hy = huya_mod.Huya('hy', 'https://www.huya.com/12345')
        self.assertEqual(hy.huya_cdn, '')

        profile = {
            'live': True,
            'room_title': 'live room',
            'gid': 1,
            'streams_info': {},
            'bitrate_info': [],
            'max_bitrate': 0,
        }
        with patch.object(hy, 'get_room_profile', new=AsyncMock(return_value=profile)):
            with patch.object(hy, 'build_stream_urls', new=AsyncMock(return_value={})):
                self.assertFalse(await hy.acheck_stream(is_check=False))

        hy.huya_cdn = 'TX'
        hy.huya_cdn_fallback = True
        with patch.object(hy, 'get_room_profile', new=AsyncMock(return_value=profile)):
            with patch.object(
                hy,
                'build_stream_urls',
                new=AsyncMock(side_effect=[{'TX': 'https://tx.example/s.flv'}, {'AL': 'https://al.example/s.flv'}]),
            ):
                with patch.object(hy, 'add_ratio', return_value='https://tx.example/s.flv'):
                    with patch.object(hy, 'acheck_url_healthy', new=AsyncMock(return_value='https://tx.example/s.flv')):
                        self.assertFalse(await hy.acheck_stream(is_check=False))

    async def test_douyin_quality_fallback_when_offsets_are_none(self):
        for configured_q, streams, expected_q in [
            ('origin', {'sd': {'main': {'flv': 'http://sd'}}}, 'http://sd'),
            ('md', {'hd': {'main': {'flv': 'http://hd'}}}, 'http://hd'),
            ('origin', {}, None),
        ]:
            config.data['douyin_quality'] = configured_q
            dy = douyin_mod.Douyin('dy', 'https://live.douyin.com/12345')
            room_info = {
                'data': {
                    'user': {'sec_uid': 'u1'},
                    'data': [{
                        'status': 2,
                        'id_str': '999',
                        'title': 'dy room',
                        'cover': {'url_list': ['']},
                        'owner': {'web_rid': '12345'},
                        'stream_url': {
                            'live_core_sdk_data': {
                                'pull_data': {
                                    'stream_data': json.dumps({'data': streams})
                                }
                            }
                        },
                    }]
                }
            }
            with patch.object(dy, 'get_web_room_info', new=AsyncMock(return_value=room_info)):
                with patch.object(douyin_mod.DouyinUtils, 'get_ttwid', return_value='t'):
                    ok = await dy.acheck_stream(is_check=False)
            if expected_q is None:
                self.assertFalse(ok)
            else:
                self.assertTrue(ok)
                self.assertEqual(dy.raw_stream_url, expected_q.replace('http://', 'https://'))

    async def test_kuaishou_decorator_cookie_and_async_sleep(self):
        from biliup.engine import Plugin
        import biliup.plugins as plugins_pkg

        plugin_loader = Plugin(plugins_pkg)
        self.assertIs(plugin_loader.inspect_checker('https://live.kuaishou.com/u/123'), kuaishou_mod.Kuaishou)
        self.assertIs(plugin_loader.inspect_checker('https://livev.m.chenzhongtech.com/fw/live/123'), kuaishou_mod.Kuaishou)

        config.data['user'] = {'kuaishou_cookie': 'did=nested_cookie'}
        ks = kuaishou_mod.Kuaishou('ks', 'https://live.kuaishou.com/u/abc')
        self.assertEqual(ks.fake_headers['Cookie'], 'did=nested_cookie')

        resp1 = MagicMock(text='')
        resp2 = MagicMock(text='主播尚未开播')
        with patch.object(kuaishou_mod.biliup.common.util.client, 'get', new=AsyncMock(side_effect=[resp1, resp2])):
            with patch.object(kuaishou_mod.asyncio, 'sleep', new=AsyncMock()) as sleep_mock:
                ok = await ks.acheck_stream(is_check=True)
                self.assertFalse(ok)
                sleep_mock.assert_awaited_once()

    def test_twitcasting_password_reads_both_nested_and_top_level(self):
        config.data['user'] = {'twitcasting_password': 'secret_nested'}
        tc1 = twitcasting_mod.Twitcasting('tc1', 'https://twitcasting.tv/user1')
        self.assertEqual(tc1.twitcasting_password, 'secret_nested')

        config.data.clear()
        config.data['twitcasting_password'] = 'secret_top'
        tc2 = twitcasting_mod.Twitcasting('tc2', 'https://twitcasting.tv/user2')
        self.assertEqual(tc2.twitcasting_password, 'secret_top')

    async def test_missevan_url_branches_and_missing_id(self):
        # 'live' branch skips www HTML request and extracts /456
        me_live = missevan_mod.Missevan('me', 'https://fm.missevan.com/live/456')
        api_resp = MagicMock()
        api_resp.json.return_value = {'code': 0, 'info': {'room': {'status': {'open': 1}, 'name': 'R', 'channel': {'flv_pull_url': 'http://flv'}}}}
        with patch.object(missevan_mod.biliup.common.util.client, 'get', new=AsyncMock(return_value=api_resp)) as get_mock:
            self.assertTrue(await me_live.acheck_stream())
            get_mock.assert_awaited_once()
            self.assertIn('/live/456', get_mock.call_args.args[0])

        # 'www' branch parses data-id
        me_www = missevan_mod.Missevan('me2', 'https://www.missevan.com/user/1')
        page_resp = MagicMock(status_code=200, text='<div data-id="999"></div>')
        with patch.object(missevan_mod.biliup.common.util.client, 'get', new=AsyncMock(side_effect=[page_resp, api_resp])) as get_mock:
            self.assertTrue(await me_www.acheck_stream())
            self.assertIn('/live/999', get_mock.call_args_list[1].args[0])

        # Missing room ID returns False early
        me_bad = missevan_mod.Missevan('me3', 'https://fm.missevan.com/live/notfound')
        with patch.object(missevan_mod.biliup.common.util.client, 'get', new=AsyncMock()) as get_mock:
            self.assertFalse(await me_bad.acheck_stream())
            get_mock.assert_not_awaited()

    async def test_kilakila_returns_false_on_exceptions(self):
        kk = kilakila_mod.Kilakila('kk', 'https://live.kilakila.cn/PcLive/index/detail?id=123')
        bad_json_resp = MagicMock(status_code=200, text='<html>err</html>')
        bad_json_resp.json.side_effect = json.JSONDecodeError('err', '<html>err</html>', 0)
        with patch.object(kilakila_mod.client, 'get', new=AsyncMock(return_value=bad_json_resp)):
            self.assertFalse(await kk.acheck_stream())

        with patch.object(kilakila_mod.client, 'get', new=AsyncMock(side_effect=RuntimeError('timeout'))):
            self.assertFalse(await kk.acheck_stream())

    async def test_nico_is_check_and_split_cli_args_and_async_sleep(self):
        config.data['user'] = {
            'niconico-email': 'a@b.com',
            'niconico-password': 'pw',
            'niconico-user-session': 'sess',
            'niconico-purge-credentials': True,
        }
        nico = nico_mod.Nico('nico', 'https://live.nicovideo.jp/watch/lv123')
        ok_resp = MagicMock(status_code=200, text='"name":"RoomTitle","description":"Desc"')

        with patch.object(nico_mod.biliup.common.util.client, 'get', new=AsyncMock(return_value=ok_resp)):
            with patch.object(nico_mod.subprocess, 'Popen') as popen_mock:
                self.assertTrue(await nico.acheck_stream(is_check=True))
                popen_mock.assert_not_called()
                self.assertEqual(nico.room_title, 'RoomTitle')

        bad_resp = MagicMock(status_code=404, text='')
        with patch.object(nico_mod.biliup.common.util.client, 'get', new=AsyncMock(return_value=bad_resp)):
            self.assertFalse(await nico.acheck_stream(is_check=True))

        fake_proc = MagicMock()
        fake_proc.poll.return_value = None
        with patch.object(nico_mod.biliup.common.util.client, 'get', new=AsyncMock(return_value=ok_resp)):
            with patch.object(nico_mod.subprocess, 'Popen', return_value=fake_proc) as popen_mock:
                with patch.object(nico_mod.asyncio, 'sleep', new=AsyncMock()) as sleep_mock:
                    self.assertTrue(await nico.acheck_stream(is_check=False))
                    cmd = popen_mock.call_args.args[0]
                    self.assertIn('--niconico-email', cmd)
                    self.assertIn('a@b.com', cmd)
                    self.assertIn('--niconico-password', cmd)
                    self.assertIn('pw', cmd)
                    self.assertIn('--niconico-user-session', cmd)
                    self.assertIn('sess', cmd)
                    self.assertIn('--niconico-purge-credentials', cmd)
                    self.assertEqual(sleep_mock.await_count, 5)

    async def test_youtube_to_thread_ytarchive_termination_and_no_close(self):
        yt_live = youtube_mod.YoutubeLive('yt', 'https://www.youtube.com/channel/abc/live')

        class _FakeYDL:
            def __init__(self, opts):
                self.archive = set()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def _make_archive_id(self, entry):
                return f"youtube {entry.get('id', '1')}"

            def extract_info(self, url, download=False, process=True):
                return {
                    'id': '1',
                    'upload_date': '20260101',
                    'live_status': 'is_live',
                    'fulltitle': 'YT Live',
                    'title': 'YT Live',
                    'thumbnail': 'https://img',
                    'webpage_url': 'https://www.youtube.com/watch?v=1',
                    'manifest_url': 'https://manifest',
                }

        with patch.object(youtube_mod.yt_dlp, 'YoutubeDL', _FakeYDL):
            with patch.object(youtube_mod.asyncio, 'to_thread', wraps=asyncio.to_thread) as to_thread_mock:
                self.assertTrue(await yt_live.acheck_stream(is_check=True))
                to_thread_mock.assert_awaited_once()

        yt = youtube_mod.Youtube('yt2', 'https://www.youtube.com/@chan/streams')
        with patch.object(youtube_mod.yt_dlp, 'YoutubeDL', _FakeYDL):
            with patch.object(youtube_mod.asyncio, 'to_thread', wraps=asyncio.to_thread) as to_thread_mock:
                self.assertTrue(await yt.acheck_stream(is_check=True))
                to_thread_mock.assert_awaited_once()

        # Verify ytarchive finally ordering (terminate before wait, kill only on TimeoutExpired) & no cover_thread.close()
        yt_live.downloader = 'ytarchive'
        yt_live.use_live_cover = True
        fake_thread = MagicMock()
        fake_thread.is_alive.return_value = True
        del fake_thread.close  # AttributeError if close() were still called

        call_order = []
        fake_proc = MagicMock()
        fake_proc.stdout.readline.return_value = b''
        fake_proc.poll.return_value = None

        def _term():
            call_order.append('terminate')

        def _wait(timeout=None):
            call_order.append(('wait', timeout))
            if timeout == 20:
                raise subprocess.TimeoutExpired(cmd='ytarchive', timeout=20)

        def _kill():
            call_order.append('kill')

        fake_proc.terminate.side_effect = _term
        fake_proc.wait.side_effect = _wait
        fake_proc.kill.side_effect = _kill

        with patch.object(youtube_mod.threading, 'Thread', return_value=fake_thread):
            with patch.object(youtube_mod.subprocess, 'Popen', return_value=fake_proc):
                self.assertTrue(yt_live.download())

        self.assertEqual(call_order, ['terminate', ('wait', 20), 'kill', ('wait', None)])
        fake_thread.join.assert_called_once_with(timeout=20)

    def test_douyu_get_real_rid_caches_truthy_only_with_timeout(self):
        fail_resp = MagicMock(text='not found')
        ok_resp = MagicMock(text='window.$ROOM = {"roomInfo":{"rid":777888}}')
        fake_requests = SimpleNamespace(get=MagicMock(side_effect=[fail_resp, ok_resp]))
        with patch.dict(sys.modules, {'requests': fake_requests}):
            self.assertIsNone(douyu_mod._get_real_rid('https://www.douyu.com/abc'))
            # None must NOT be cached, so second attempt retries HTTP call
            self.assertEqual(douyu_mod._get_real_rid('https://www.douyu.com/abc'), '777888')
            # Truthy RID IS cached, so third attempt does not invoke requests.get
            self.assertEqual(douyu_mod._get_real_rid('https://www.douyu.com/abc'), '777888')

        self.assertEqual(fake_requests.get.call_count, 2)
        self.assertEqual(fake_requests.get.call_args_list[0].kwargs.get('timeout'), 10)


if __name__ == '__main__':
    unittest.main()
