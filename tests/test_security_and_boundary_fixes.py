import asyncio
import copy
import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp.test_utils import TestClient, TestServer
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from biliup.config import config


def _bootstrap_modules():
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
        import biliup.common.tools as tools_mod
        import biliup.plugins.bilibili as bilibili_mod
        import biliup.plugins.douyu as douyu_mod
        import biliup.uploader as uploader_mod
        import biliup.web as web_mod
        from biliup.database.models import BaseModel, Configuration, LiveStreamers, UploadStreamers
        return (
            app_mod,
            tools_mod,
            bilibili_mod,
            douyu_mod,
            uploader_mod,
            web_mod,
            BaseModel,
            Configuration,
            LiveStreamers,
            UploadStreamers,
        )
    finally:
        asyncio.get_running_loop = orig_get_running_loop
        temp_loop.close()


(
    app_mod,
    tools_mod,
    bilibili_mod,
    douyu_mod,
    uploader_mod,
    web_mod,
    BaseModel,
    Configuration,
    LiveStreamers,
    UploadStreamers,
) = _bootstrap_modules()


class TestWebSecurityAndCrud(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._orig_password = web_mod.GLOBAL_PASSWORD
        self._orig_cwd = os.getcwd()
        self._config_snapshot = {
            k: (v if k == 'PluginInfo' else copy.deepcopy(v))
            for k, v in config.data.items()
        }
        self._context_snapshot = {
            k: (v if k == 'PluginInfo' else copy.deepcopy(v))
            for k, v in app_mod.context.items()
        }
        self._tmpdir = tempfile.TemporaryDirectory()
        os.chdir(self._tmpdir.name)

        self._engine = create_engine('sqlite:///:memory:')
        BaseModel.metadata.create_all(self._engine)
        self._TestSession = sessionmaker(bind=self._engine, autocommit=False)

    async def asyncTearDown(self):
        web_mod._shutdown_qr_pool()
        self._engine.dispose()
        os.chdir(self._orig_cwd)
        self._tmpdir.cleanup()
        web_mod.set_global_password(self._orig_password)
        config.data.clear()
        config.data.update(self._config_snapshot)
        app_mod.context.clear()
        app_mod.context.update(self._context_snapshot)

    async def _make_client(self, password=None) -> TestClient:
        app = web_mod.create_app(password=password, static_dir=self._tmpdir.name)
        client = TestClient(TestServer(app))
        await client.start_server()
        self.addAsyncCleanup(client.close)
        return client

    async def test_post_users_validation(self):
        client = await self._make_client(password=None)

        with patch.object(web_mod, 'SessionLocal', self._TestSession):
            # 非法 JSON 与非对象请求体 -> 400
            bad_json = await client.post('/v1/users', data=b'{not-json')
            self.assertEqual(bad_json.status, 400)
            bad_body = await client.post('/v1/users', json=['not-a-dict'])
            self.assertEqual(bad_body.status, 400)

            # 仅允许 bilibili-cookies，禁止注入 config 等重复配置键
            for bad_platform in ['config', 'other', '', None]:
                resp = await client.post(
                    '/v1/users',
                    json={'platform': bad_platform, 'value': 'cookies.json'},
                )
                self.assertEqual(resp.status, 400, f'platform={bad_platform!r} should be rejected')

            # 非法路径校验：空值、非字符串、空字节/控制符、.. 穿越、非 .json、敏感绝对路径、UNC
            invalid_values = [
                None,
                123,
                '',
                '   ',
                'cookies.json\x00.txt',
                'a\x00b.json',
                'a\nb.json',
                '../cookies.json',
                'data/../cookies.json',
                '..\\cookies.json',
                'data\\..\\cookies.json',
                'cookies.txt',
                '/etc/passwd',
                '.json',
                'data/.json',
                '//server/share/cookies.json',
                '\\\\server\\share\\cookies.json',
                '/etc/shadow.json',
                '/proc/self/environ.json',
                '/root/cookies.json',
                '/var/run/secret.json',
                'C:\\Windows\\System32\\a.json',
                'c:/Program Files/app/a.json',
                ' /etc/shadow.json',
                ' //server/share/a.json',
                ' C:/Windows/System32/a.json',
                'C:../secret.json',
                'C:Windows/System32/a.json',
                'cookies.json ',
            ]
            for bad_val in invalid_values:
                resp = await client.post(
                    '/v1/users',
                    json={'platform': 'bilibili-cookies', 'value': bad_val},
                )
                self.assertEqual(resp.status, 400, f'value={bad_val!r} should be rejected')

            with self._TestSession() as db:
                self.assertEqual(db.scalars(select(Configuration)).all(), [])

            # 合法路径放行（含含双点的正常文件名与非敏感绝对路径）
            valid_values = [
                'cookies.json',
                'data/123456.json',
                'data/user..1.json',
                '/data/cookies.json',
                'data\\123456.JSON',
            ]
            for ok_val in valid_values:
                resp = await client.post(
                    '/v1/users',
                    json={'platform': 'bilibili-cookies', 'value': ok_val},
                )
                self.assertEqual(resp.status, 200, f'value={ok_val!r} should succeed')
                payload = await resp.json()
                self.assertEqual(payload[0]['value'], ok_val)
                self.assertEqual(payload[0]['platform'], 'bilibili-cookies')

    async def test_crud_missing_record_returns_400_or_404(self):
        client = await self._make_client(password=None)

        with self._TestSession() as db:
            us = UploadStreamers(template_name='tpl1', tags=['tag1'])
            cfg_row = Configuration(key='config', value='{"delay": 0}')
            user_row = Configuration(key='bilibili-cookies', value='data/u1.json')
            db.add_all([us, cfg_row, user_row])
            db.commit()
            us_id = us.id
            cfg_id = cfg_row.id
            user_id = user_row.id

        with patch.object(web_mod, 'SessionLocal', self._TestSession):
            # POST /v1/streamers 不存在的 upload_id -> 400
            bad_streamer = await client.post(
                '/v1/streamers',
                json={
                    'url': 'https://live.bilibili.com/100',
                    'remark': '主播A',
                    'upload_id': 999999,
                },
            )
            self.assertEqual(bad_streamer.status, 400)
            with self._TestSession() as db:
                self.assertEqual(db.scalars(select(LiveStreamers)).all(), [])

            # GET /v1/upload/streamers/{id} 查不到 -> 404；存在 -> 200
            missing_tpl = await client.get('/v1/upload/streamers/999999')
            self.assertEqual(missing_tpl.status, 404)
            ok_tpl = await client.get(f'/v1/upload/streamers/{us_id}')
            self.assertEqual(ok_tpl.status, 200)
            self.assertEqual((await ok_tpl.json())['template_name'], 'tpl1')

            # DELETE /v1/users/{id} 查不到 -> 404
            missing_user = await client.delete('/v1/users/999999')
            self.assertEqual(missing_user.status, 404)

            # DELETE /v1/users/{id} 指向非 bilibili-cookies (config) -> 404 且不删除
            del_cfg = await client.delete(f'/v1/users/{cfg_id}')
            self.assertEqual(del_cfg.status, 404)
            with self._TestSession() as db:
                self.assertIsNotNone(db.get(Configuration, cfg_id))

            # DELETE /v1/users/{id} 指向存在的 bilibili-cookies -> 200 且删除
            del_user = await client.delete(f'/v1/users/{user_id}')
            self.assertEqual(del_user.status, 200)
            with self._TestSession() as db:
                self.assertIsNone(db.get(Configuration, user_id))

    async def test_get_status_allowlist_excludes_secrets(self):
        client = await self._make_client(password=None)

        app_mod.context.clear()
        app_mod.context.update({
            'url_upload_count': {'https://live.bilibili.com/1': 2},
            'upload_filename': ['video_a'],
            'file_upload_count': {'video_a.flv': 1},
            'sync_downloader_map': {'room1': 'sync'},
            'notify_smtp_pass': 'SUPER_SECRET_SMTP_PASS',
            'user': {'bili_cookie': {'SESSDATA': 'SECRET_SESSDATA'}, 'password': 'SECRET_USER_PASS'},
            'streamers': {'room1': {'override': {'cookie': 'SECRET_OVERRIDE_COOKIE'}}},
            'PluginInfo': object(),
        })

        resp = await client.get('/v1/status')
        self.assertEqual(resp.status, 200)
        raw_text = await resp.text()
        data = json.loads(raw_text)

        self.assertEqual(
            set(data.keys()),
            {
                'version',
                'url_upload_count',
                'upload_filename',
                'file_upload_count',
                'sync_downloader_map',
            },
        )
        self.assertEqual(data['url_upload_count'], {'https://live.bilibili.com/1': 2})
        self.assertEqual(data['upload_filename'], ['video_a'])
        self.assertEqual(data['file_upload_count'], {'video_a.flv': 1})
        self.assertEqual(data['sync_downloader_map'], {'room1': 'sync'})
        for secret in (
            'notify_smtp_pass',
            'SUPER_SECRET_SMTP_PASS',
            'SECRET_SESSDATA',
            'SECRET_USER_PASS',
            'SECRET_OVERRIDE_COOKIE',
        ):
            self.assertNotIn(secret, raw_text)


class TestBoundaryAndPluginFixes(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._orig_cwd = os.getcwd()
        self._tmpdir = tempfile.TemporaryDirectory()
        os.chdir(self._tmpdir.name)
        self._config_snapshot = {
            k: (v if k == 'PluginInfo' else copy.deepcopy(v))
            for k, v in config.data.items()
        }
        self._douyu_cache_snapshot = dict(douyu_mod._REAL_RID_CACHE)
        douyu_mod._REAL_RID_CACHE.clear()

    async def asyncTearDown(self):
        os.chdir(self._orig_cwd)
        self._tmpdir.cleanup()
        config.data.clear()
        config.data.update(self._config_snapshot)
        douyu_mod._REAL_RID_CACHE.clear()
        douyu_mod._REAL_RID_CACHE.update(self._douyu_cache_snapshot)

    def test_merge_dict_ignores_non_dict_json_and_values(self):
        self.assertEqual(
            uploader_mod.merge_dict(
                '123',
                'true',
                'null',
                '"hello"',
                '[1, 2, 3]',
                '[["k", "v"]]',
                '{invalid json',
                None,
                123,
                ['x'],
            ),
            {},
        )
        self.assertEqual(
            uploader_mod.merge_dict(
                '{"a": 1}',
                'null',
                '[1, 2]',
                '{"b": 2}',
                {'c': 3},
            ),
            {'a': 1, 'b': 2, 'c': 3},
        )

    async def test_douyu_acheck_stream_offloads_get_real_rid_to_thread(self):
        url = 'https://www.douyu.com/non_numeric_room'
        loop_thread_id = threading.get_ident()
        observed_thread_ids = []

        def fake_requests_get(*args, **kwargs):
            observed_thread_ids.append(threading.get_ident())
            return SimpleNamespace(text='window.$ROOM = {"roomInfo":{"rid":888999}}')

        fake_requests = SimpleNamespace(get=fake_requests_get)
        fake_betard_resp = MagicMock()
        fake_betard_resp.text = json.dumps({'room': {'show_status': 2}})

        orig_to_thread = asyncio.to_thread
        to_thread_calls = []

        async def spy_to_thread(func, *args, **kwargs):
            to_thread_calls.append((func, args, kwargs))
            return await orig_to_thread(func, *args, **kwargs)

        d = douyu_mod.Douyu('douyu_test', url)
        with patch.dict(sys.modules, {'requests': fake_requests}), \
             patch.object(douyu_mod.asyncio, 'to_thread', side_effect=spy_to_thread), \
             patch.object(douyu_mod.client, 'get', new=AsyncMock(return_value=fake_betard_resp)):
            is_live = await d.acheck_stream(is_check=True)

        self.assertFalse(is_live)
        self.assertEqual(len(to_thread_calls), 1)
        self.assertIs(to_thread_calls[0][0], douyu_mod._get_real_rid)
        self.assertEqual(to_thread_calls[0][1], (url,))
        self.assertEqual(len(observed_thread_ids), 1)
        self.assertNotEqual(observed_thread_ids[0], loop_thread_id)
        self.assertEqual(douyu_mod._REAL_RID_CACHE.get(url), '888999')

    def test_linux_birth_timestamp_hardening_and_single_stat(self):
        fname = '-leading-dash.flv'
        mtime_ts = 1700001234.0
        birth_ts = 1700000000.0
        fake_stat = SimpleNamespace(st_mtime=mtime_ts, st_ctime=mtime_ts)

        # 1. 有 birth time 时，stat 参数安全、带 stderr=DEVNULL 和 timeout=5，且 os.stat 仅调一次
        with patch('biliup.common.tools.os.stat', return_value=fake_stat) as stat_mock, \
             patch('biliup.common.tools.platform.system', return_value='Linux'), \
             patch('biliup.common.tools.subprocess.check_output', return_value=f'{birth_ts}\n'.encode('utf-8')) as co_mock:
            self.assertEqual(tools_mod.get_file_create_timestamp(fname), birth_ts)
            stat_mock.assert_called_once_with(fname)
            co_mock.assert_called_once_with(
                ['stat', '-c', '%W', '--', fname],
                stderr=subprocess.DEVNULL,
                timeout=5,
            )

        # 2. 返回 0 回退到 mtime 时，os.stat 也只调一次
        with patch('biliup.common.tools.os.stat', return_value=fake_stat) as stat_mock, \
             patch('biliup.common.tools.platform.system', return_value='Linux'), \
             patch('biliup.common.tools.subprocess.check_output', return_value=b'0\n'):
            self.assertEqual(tools_mod.get_file_create_timestamp(fname), mtime_ts)
            stat_mock.assert_called_once_with(fname)

        # 3. stat 超时时优雅回退到 mtime 且 os.stat 只调一次
        with patch('biliup.common.tools.os.stat', return_value=fake_stat) as stat_mock, \
             patch('biliup.common.tools.platform.system', return_value='Linux'), \
             patch('biliup.common.tools.subprocess.check_output', side_effect=subprocess.TimeoutExpired('stat', 5)):
            self.assertEqual(tools_mod.get_file_create_timestamp(fname), mtime_ts)
            stat_mock.assert_called_once_with(fname)

    def test_bilibili_parse_master_m3u8_preserves_qn_zero_and_filters_empty_buckets(self):
        bili = bilibili_mod.Bililive('bili_test', 'https://live.bilibili.com/12345')

        # 1. 保留 BILI-QN=0
        m3u8_qn_zero = (
            '#EXTM3U\n'
            '#EXT-X-STREAM-INF:CODECS="avc1.640028",BILI-QN=0\n'
            'https://cdn.example.com/live-bvc/1/live_0.m3u8?cdn=tx&suffix=origin\n'
        )
        parsed_zero = bili.parse_master_m3u8(m3u8_qn_zero)
        self.assertIn(0, parsed_zero)
        self.assertIn('tx', parsed_zero[0])

        # 2. 过滤无有效 CDN URL 的空清晰度桶，且缺少 CODECS 时不会抛 KeyError
        m3u8_mixed = (
            '#EXTM3U\n'
            '#EXT-X-STREAM-INF:CODECS="avc1.640028",BILI-QN=10000\n'
            'https://cdn.example.com/live-bvc/1/live_10000.m3u8?cdn=hw&suffix=origin\n'
            '#EXT-X-STREAM-INF:CODECS="avc1.640028",BILI-QN=250\n'
            'https://cdn.example.com/live-bvc/1/live_250.m3u8?suffix=origin\n'
            '#EXT-X-STREAM-INF:CODECS="avc1.640028",BILI-QN=150\n'
            '#EXT-X-STREAM-INF:BILI-QN=80\n'
            'https://cdn.example.com/live-bvc/1/live_80.m3u8?cdn=tx&suffix=origin\n'
        )
        parsed_mixed = bili.parse_master_m3u8(m3u8_mixed)
        self.assertEqual(list(parsed_mixed.keys()), [10000])
        self.assertIn('hw', parsed_mixed[10000])
        self.assertTrue(all(bool(v) for v in parsed_mixed.values()))


if __name__ == '__main__':
    unittest.main()
