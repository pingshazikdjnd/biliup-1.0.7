import asyncio
import copy
import json
import os
import tempfile
import threading
import types
import unittest
from unittest.mock import patch

import requests
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from biliup.config import config
from biliup.database.models import BaseModel, Configuration


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
        import biliup.app  # noqa: F401
        import biliup.web as web_mod
        return web_mod
    finally:
        asyncio.get_running_loop = orig_get_running_loop
        temp_loop.close()


web_mod = _bootstrap_biliup()


def _write_cookie_file(path: str, sessdata: str, bili_jct: str = 'jct_val', token: str = 'tok_val'):
    payload = {
        'cookie_info': {
            'cookies': [
                {'name': 'SESSDATA', 'value': sessdata},
                {'name': 'bili_jct', 'value': bili_jct},
            ]
        },
        'token_info': {
            'access_token': token,
        },
    }
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f)


class TestPR5SessionIsolation(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._config_snapshot = {
            k: (v if k == 'PluginInfo' else copy.deepcopy(v))
            for k, v in config.data.items()
        }
        self._user_sentinel = {
            'cookies': {'SESSDATA': 'global_sentinel'},
            'access_token': 'global_token',
        }
        config.data['user'] = copy.deepcopy(self._user_sentinel)

        self._tmpdir = tempfile.TemporaryDirectory()
        self._engine = create_engine('sqlite:///:memory:', connect_args={'check_same_thread': False})
        BaseModel.metadata.create_all(self._engine)
        self._TestSession = sessionmaker(bind=self._engine)

    async def asyncTearDown(self):
        self._tmpdir.cleanup()
        self._engine.dispose()
        config.data.clear()
        config.data.update(self._config_snapshot)

    def _register_cookie_paths(self, *paths):
        with self._TestSession() as db:
            for p in paths:
                db.add(Configuration(key='bilibili-cookies', value=p))
            db.commit()

    def test_module_does_not_shadow_bilibili_class_with_singleton(self):
        self.assertIsInstance(web_mod.BiliBili, type)

    def test_load_cookies_from_file_does_not_pollute_global_config(self):
        cookie_path = os.path.join(self._tmpdir.name, 'user_a.json')
        _write_cookie_file(cookie_path, sessdata='sess_a', bili_jct='jct_a', token='tok_a')

        cookies = web_mod._load_cookies_from_file(cookie_path)
        self.assertEqual(cookies, {'SESSDATA': 'sess_a', 'bili_jct': 'jct_a'})
        self.assertEqual(config.data['user'], self._user_sentinel)

        with self.assertRaises(FileNotFoundError):
            web_mod._load_cookies_from_file(os.path.join(self._tmpdir.name, 'missing.json'))

        bad_path = os.path.join(self._tmpdir.name, 'bad.json')
        with open(bad_path, 'w', encoding='utf-8') as f:
            f.write('[]')
        with self.assertRaises(ValueError):
            web_mod._load_cookies_from_file(bad_path)

    async def test_concurrent_myinfo_isolated_sessions_and_non_blocking(self):
        path_a = os.path.join(self._tmpdir.name, 'a.json')
        path_b = os.path.join(self._tmpdir.name, 'b.json')
        _write_cookie_file(path_a, sessdata='sess_a', bili_jct='jct_a')
        _write_cookie_file(path_b, sessdata='sess_b', bili_jct='jct_b')

        instances = []
        inst_lock = threading.Lock()
        barrier = threading.Barrier(2, timeout=3.0)

        class FakeBiliBili:
            def __init__(self, data):
                self.data = data
                self.closed = False
                self.seen_cookies = None
                setattr(
                    self,
                    '_BiliBili__session',
                    types.SimpleNamespace(cookies=requests.cookies.RequestsCookieJar()),
                )
                with inst_lock:
                    instances.append(self)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.close()

            def close(self):
                self.closed = True

            def myinfo(self, cookies):
                self.seen_cookies = dict(cookies)
                # 两次并发请求必须在独立线程同步汇合，证明未阻塞事件循环
                barrier.wait()
                sess = cookies.get('SESSDATA')
                return {'code': 0, 'data': {'name': f'name_{sess}', 'mid': sess}}

        app = web.Application()
        app.add_routes(web_mod.routes)
        client = TestClient(TestServer(app))
        await client.start_server()
        self.addAsyncCleanup(client.close)

        missing_path = os.path.join(self._tmpdir.name, 'nope.json')
        unregistered_path = os.path.join(self._tmpdir.name, 'unregistered.json')
        self._register_cookie_paths(path_a, path_b, missing_path)

        with patch.object(web_mod, 'SessionLocal', self._TestSession), \
             patch.object(web_mod, 'BiliBili', FakeBiliBili):
            resp_a, resp_b = await asyncio.gather(
                client.get('/bili/space/myinfo', params={'user': path_a}),
                client.get('/bili/space/myinfo', params={'user': path_b}),
            )
            self.assertEqual(resp_a.status, 200)
            self.assertEqual(resp_b.status, 200)
            self.assertEqual(
                await resp_a.json(),
                {'code': 0, 'data': {'name': 'name_sess_a', 'mid': 'sess_a'}},
            )
            self.assertEqual(
                await resp_b.json(),
                {'code': 0, 'data': {'name': 'name_sess_b', 'mid': 'sess_b'}},
            )

            self.assertEqual(len(instances), 2)
            self.assertIsNot(instances[0], instances[1])
            self.assertTrue(all(inst.closed for inst in instances))
            self.assertEqual(
                {inst.seen_cookies['SESSDATA'] for inst in instances},
                {'sess_a', 'sess_b'},
            )
            self.assertEqual(config.data['user'], self._user_sentinel)

            # 缺参返回 400，未登记路径返回 403，已登记但磁盘缺失返回 500
            self.assertEqual((await client.get('/bili/space/myinfo')).status, 400)
            self.assertEqual((await client.get('/bili/space/myinfo', params={'user': unregistered_path})).status, 403)
            missing = await client.get(
                '/bili/space/myinfo',
                params={'user': missing_path},
            )
            self.assertEqual(missing.status, 500)

    async def test_concurrent_get_seasons_isolated_and_closes_on_error(self):
        path_a = os.path.join(self._tmpdir.name, 'sa.json')
        path_b = os.path.join(self._tmpdir.name, 'sb.json')
        _write_cookie_file(path_a, sessdata='season_a')
        _write_cookie_file(path_b, sessdata='season_b')

        instances = []
        inst_lock = threading.Lock()
        barrier = threading.Barrier(2, timeout=3.0)
        should_raise = {'flag': False}

        class FakeBiliBili:
            def __init__(self, data):
                self.closed = False
                setattr(
                    self,
                    '_BiliBili__session',
                    types.SimpleNamespace(cookies=requests.cookies.RequestsCookieJar()),
                )
                with inst_lock:
                    instances.append(self)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.close()

            def close(self):
                self.closed = True

            def list_seasons(self):
                if should_raise['flag']:
                    raise RuntimeError('season api error')
                barrier.wait()
                sess = self._BiliBili__session.cookies.get('SESSDATA')
                return {
                    'seasons': [
                        {
                            'season': {'id': 100 if sess == 'season_a' else 200, 'title': f'title_{sess}'},
                            'sections': {'sections': [{'id': 11 if sess == 'season_a' else 22}]},
                        }
                    ]
                }

        app = web.Application()
        app.add_routes(web_mod.routes)
        client = TestClient(TestServer(app))
        await client.start_server()
        self.addAsyncCleanup(client.close)

        self._register_cookie_paths(path_a, path_b)

        with patch.object(web_mod, 'SessionLocal', self._TestSession), \
             patch.object(web_mod, 'BiliBili', FakeBiliBili):
            resp_a, resp_b = await asyncio.gather(
                client.get('/bili/seasons', params={'user': path_a}),
                client.get('/bili/seasons', params={'user': path_b}),
            )
            self.assertEqual(resp_a.status, 200)
            self.assertEqual(resp_b.status, 200)
            self.assertEqual(
                await resp_a.json(),
                [{'season_id': 100, 'title': 'title_season_a', 'section_id': 11}],
            )
            self.assertEqual(
                await resp_b.json(),
                [{'season_id': 200, 'title': 'title_season_b', 'section_id': 22}],
            )
            self.assertEqual(len(instances), 2)
            self.assertTrue(all(inst.closed for inst in instances))
            self.assertEqual(config.data['user'], self._user_sentinel)

            # 异常场景也必须通过 with 释放连接
            should_raise['flag'] = True
            err_resp = await client.get('/bili/seasons', params={'user': path_a})
            self.assertEqual(err_resp.status, 500)
            self.assertTrue(instances[-1].closed)

    async def test_pre_archive_isolated_and_skips_failed_cookies(self):
        path_a = os.path.join(self._tmpdir.name, 'pre_a.json')
        path_b = os.path.join(self._tmpdir.name, 'pre_b.json')
        _write_cookie_file(path_a, sessdata='expired_sess')
        _write_cookie_file(path_b, sessdata='valid_sess')

        with self._TestSession() as db:
            db.add(Configuration(key='bilibili-cookies', value=path_a))
            db.add(Configuration(key='bilibili-cookies', value=path_b))
            db.commit()

        instances = []

        class FakeBiliBili:
            def __init__(self, data):
                self.closed = False
                instances.append(self)

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.close()

            def close(self):
                self.closed = True

            def tid_archive(self, cookies):
                if cookies.get('SESSDATA') == 'expired_sess':
                    return {'code': -101, 'message': '账号未登录'}
                return {'code': 0, 'data': {'typelist': [{'id': 171, 'name': '电子竞技'}]}}

        app = web.Application()
        app.add_routes(web_mod.routes)
        client = TestClient(TestServer(app))
        await client.start_server()
        self.addAsyncCleanup(client.close)

        with patch.object(web_mod, 'SessionLocal', self._TestSession), \
             patch.object(web_mod, 'BiliBili', FakeBiliBili):
            resp = await client.get('/bili/archive/pre')
            self.assertEqual(resp.status, 200)
            body = await resp.json()
            self.assertEqual(body['code'], 0)
            self.assertEqual(body['data']['typelist'][0]['id'], 171)

        self.assertEqual(len(instances), 2)
        self.assertTrue(all(inst.closed for inst in instances))
        self.assertEqual(config.data['user'], self._user_sentinel)

    async def test_bili_client_injects_default_timeout_and_routes_handle_timeout(self):
        # 1. 验证 _bili_client 默认注入 (5, 15) 超时并保留显式传参的 timeout
        seen_timeouts = []
        fake_response = types.SimpleNamespace(json=lambda: {'code': 0})

        def _fake_request(session_self, method, url, **kwargs):
            seen_timeouts.append(kwargs.get('timeout'))
            return fake_response

        with patch.object(requests.Session, 'request', _fake_request):
            with web_mod._bili_client() as bili:
                session = bili._BiliBili__session
                session.get('https://member.bilibili.com/test')
                session.get('https://member.bilibili.com/test', timeout=10)
        self.assertEqual(seen_timeouts, [(5, 15), 10])

        # 2. 验证 /bili/space/myinfo、/bili/seasons、/api/check_tag 在 requests.Timeout 时返回 504，
        # 以及 TOML 模式 /api/login_by_cookie 在工作线程中执行（不阻塞事件循环）
        path_a = os.path.join(self._tmpdir.name, 'timeout.json')
        _write_cookie_file(path_a, sessdata='timeout_sess')

        class TimeoutBiliBili:
            def __init__(self, data):
                self.closed = False
                setattr(
                    self,
                    '_BiliBili__session',
                    types.SimpleNamespace(cookies=requests.cookies.RequestsCookieJar()),
                )

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.close()

            def close(self):
                self.closed = True

            def myinfo(self, cookies):
                raise requests.Timeout('myinfo read timed out')

            def list_seasons(self):
                raise requests.Timeout('seasons read timed out')

            def check_tag(self, tag):
                raise requests.Timeout('check_tag read timed out')

        app = web.Application()
        app.add_routes(web_mod.routes)
        client = TestClient(TestServer(app))
        await client.start_server()
        self.addAsyncCleanup(client.close)

        event_loop_tid = threading.get_ident()
        seen_login_tids = []

        def _fake_sg_login():
            seen_login_tids.append(threading.get_ident())

        self._register_cookie_paths(path_a)
        config.data['toml'] = True
        with patch.object(web_mod, 'SessionLocal', self._TestSession), \
             patch.object(web_mod, 'BiliBili', TimeoutBiliBili), \
             patch.object(web_mod.stream_gears, 'login_by_cookies', side_effect=_fake_sg_login):
            resp_myinfo = await client.get('/bili/space/myinfo', params={'user': path_a})
            self.assertEqual(resp_myinfo.status, 504)

            resp_seasons = await client.get('/bili/seasons', params={'user': path_a})
            self.assertEqual(resp_seasons.status, 504)

            resp_tag = await client.get('/api/check_tag', params={'tag': 'abc'})
            self.assertEqual(resp_tag.status, 504)

            resp_login = await client.get('/api/login_by_cookie')
            self.assertEqual(resp_login.status, 200)
            self.assertEqual(len(seen_login_tids), 1)
            self.assertNotEqual(seen_login_tids[0], event_loop_tid)


if __name__ == '__main__':
    unittest.main()
