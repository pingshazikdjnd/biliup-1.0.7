import asyncio
import base64
import copy
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch
from urllib.parse import quote

from aiohttp import ClientTimeout, WSServerHandshakeError
from aiohttp.test_utils import TestClient, TestServer

from biliup.config import config


def _bootstrap_biliup_web():
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


web_mod = _bootstrap_biliup_web()


def _basic_auth(password: str, username: str = 'biliup') -> str:
    token = base64.b64encode(f"{username}:{password}".encode('utf-8')).decode('ascii')
    return f"Basic {token}"


class TestPR2WebAuth(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._orig_password = web_mod.GLOBAL_PASSWORD
        self._orig_cwd = os.getcwd()
        self._config_snapshot = {
            k: (v if k == 'PluginInfo' else copy.deepcopy(v))
            for k, v in config.data.items()
        }
        self._tmpdir = tempfile.TemporaryDirectory()
        self._log_path = os.path.join(self._tmpdir.name, 'ds_update.log')
        with open(self._log_path, 'w', encoding='utf-8') as f:
            f.write("hello log\nsecond line\n")
        os.chdir(self._tmpdir.name)

    async def asyncTearDown(self):
        os.chdir(self._orig_cwd)
        self._tmpdir.cleanup()
        web_mod.set_global_password(self._orig_password)
        config.data.clear()
        config.data.update(self._config_snapshot)

    async def _make_client(self, password=None) -> TestClient:
        app = web_mod.create_app(password=password, static_dir=self._tmpdir.name)
        client = TestClient(TestServer(app))
        await client.start_server()
        self.addAsyncCleanup(client.close)
        return client

    async def test_protected_routes_require_auth_when_password_enabled(self):
        client = await self._make_client(password='secret123')

        for path in [
            '/static/ds_update.log',
            '/v1/static-token?file=ds_update.log',
            '/bili/space/myinfo?user=cookies.json',
            '/bili/seasons?user=cookies.json',
            '/bili/proxy?url=https://i0.hdslb.com/bfs/face/noface.jpg',
            '/v1/videos',
            '/api/basic',
            '/url-status',
        ]:
            resp = await client.get(path)
            self.assertEqual(resp.status, 401, f"Expected 401 for {path}, got {resp.status}")

        with self.assertRaises(WSServerHandshakeError) as ctx:
            await client.ws_connect('/ws/logs?file=ds_update.log')
        self.assertEqual(ctx.exception.status, 401)

    async def test_options_preflight_allowed_without_auth(self):
        client = await self._make_client(password='secret123')
        cors_headers = {
            'Origin': 'http://localhost:3000',
            'Access-Control-Request-Method': 'GET',
            'Access-Control-Request-Headers': 'Authorization',
        }

        for path in ['/v1/videos', '/bili/proxy', '/static/ds_update.log', '/api/basic']:
            resp = await client.options(path, headers=cors_headers)
            self.assertEqual(resp.status, 200, f"Expected 200 for OPTIONS {path}, got {resp.status}")

    async def test_basic_auth_utf8_and_static_signed_token(self):
        pw = '密码测试123'
        auth_header = _basic_auth(pw)
        client = await self._make_client(password=pw)

        # UTF-8 密码通过请求头鉴权成功
        resp = await client.get('/api/basic', headers={'Authorization': auth_header})
        self.assertEqual(resp.status, 200)

        # 错误密码拦截
        bad_resp = await client.get('/api/basic', headers={'Authorization': _basic_auth('wrong')})
        self.assertEqual(bad_resp.status, 401)

        # API 路径不接受仅通过 URL 参数传鉴权
        query_api_resp = await client.get(f'/api/basic?authorization={quote(auth_header)}')
        self.assertEqual(query_api_resp.status, 401)

        # /v1/static-token 校验路径合法性并签发单文件短时签名令牌（拒绝目录跳转，放行含连续点的合法文件名）
        bad_token_req = await client.get('/v1/static-token?file=../secret.log', headers={'Authorization': auth_header})
        self.assertEqual(bad_token_req.status, 400)
        dotdot_req = await client.get('/v1/static-token?file=..', headers={'Authorization': auth_header})
        self.assertEqual(dotdot_req.status, 400)

        dots_name_resp = await client.get(f'/v1/static-token?file={quote("直播...mp4")}', headers={'Authorization': auth_header})
        self.assertEqual(dots_name_resp.status, 200)
        dots_token = (await dots_name_resp.json())['token']
        self.assertTrue(web_mod.verify_static_token(client.app['static_token_secret'], '直播...mp4', dots_token))

        token_resp = await client.get('/v1/static-token?file=ds_update.log', headers={'Authorization': auth_header})
        self.assertEqual(token_resp.status, 200)
        static_token = (await token_resp.json())['token']
        self.assertTrue(static_token)

        # /static/ 支持请求头与单文件签名 token，且拒绝在 URL 中使用长期 Basic 凭据或跨文件/过期 token
        static_header_resp = await client.get('/static/ds_update.log', headers={'Authorization': auth_header})
        self.assertEqual(static_header_resp.status, 200)
        self.assertIn('hello log', await static_header_resp.text())

        static_token_resp = await client.get(f'/static/ds_update.log?token={quote(static_token)}')
        self.assertEqual(static_token_resp.status, 200)
        self.assertIn('hello log', await static_token_resp.text())

        other_file_resp = await client.get(f'/static/other.log?token={quote(static_token)}')
        self.assertEqual(other_file_resp.status, 401)

        expired_token = web_mod.generate_static_token(client.app['static_token_secret'], 'ds_update.log', ttl=-10)
        expired_resp = await client.get(f'/static/ds_update.log?token={quote(expired_token)}')
        self.assertEqual(expired_resp.status, 401)

        legacy_query_resp = await client.get(f'/static/ds_update.log?authorization={quote(auth_header)}')
        self.assertEqual(legacy_query_resp.status, 401)

        # 不存在的静态文件应保留 HTTP 404 状态码，而不是返回 200 HTML
        missing_resp = await client.get('/static/missing.log', headers={'Authorization': auth_header})
        self.assertEqual(missing_resp.status, 404)

    async def test_multi_app_websocket_password_isolation(self):
        protected_client = await self._make_client(password='pw1')
        open_client = await self._make_client(password=None)

        # 后创建的无密码 app 不得关闭已受保护 app 的 /ws/logs 鉴权
        with self.assertRaises(WSServerHandshakeError) as ctx:
            await protected_client.ws_connect('/ws/logs?file=ds_update.log')
        self.assertEqual(ctx.exception.status, 401)

        async with protected_client.ws_connect(
            f'/ws/logs?file=ds_update.log&authorization={quote(_basic_auth("pw1"))}'
        ) as ws:
            self.assertEqual(await asyncio.wait_for(ws.receive_str(), timeout=3), 'hello log')

        async with open_client.ws_connect('/ws/logs?file=ds_update.log') as ws:
            self.assertEqual(await asyncio.wait_for(ws.receive_str(), timeout=3), 'hello log')

        open_token_resp = await open_client.get('/v1/static-token?file=ds_update.log')
        self.assertEqual(open_token_resp.status, 200)
        self.assertIsNone((await open_token_resp.json())['token'])

    async def test_websocket_logs_auth_with_and_without_password(self):
        pw = 'ws_secret_密码'
        auth_header = _basic_auth(pw)
        client = await self._make_client(password=pw)

        # 无参数与错误参数均在握手阶段拒绝（401）
        with self.assertRaises(WSServerHandshakeError) as ctx_no_auth:
            await client.ws_connect('/ws/logs?file=ds_update.log')
        self.assertEqual(ctx_no_auth.exception.status, 401)

        with self.assertRaises(WSServerHandshakeError) as ctx_bad_auth:
            await client.ws_connect('/ws/logs?file=ds_update.log&authorization=Basic%20d3Jvbmc=')
        self.assertEqual(ctx_bad_auth.exception.status, 401)

        # query 参数鉴权成功
        async with client.ws_connect(f'/ws/logs?file=ds_update.log&authorization={quote(auth_header)}') as ws:
            first_msg = await asyncio.wait_for(ws.receive_str(), timeout=3)
            self.assertEqual(first_msg, 'hello log')

        # Authorization header 鉴权成功
        async with client.ws_connect('/ws/logs?file=ds_update.log', headers={'Authorization': auth_header}) as ws:
            first_msg = await asyncio.wait_for(ws.receive_str(), timeout=3)
            self.assertEqual(first_msg, 'hello log')

        # 无密码模式兼容直接连接
        open_client = await self._make_client(password=None)
        async with open_client.ws_connect('/ws/logs?file=ds_update.log') as ws:
            first_msg = await asyncio.wait_for(ws.receive_str(), timeout=3)
            self.assertEqual(first_msg, 'hello log')

    async def test_myinfo_and_seasons_cookie_allowlist(self):
        auth_header = _basic_auth('secret123')
        headers = {'Authorization': auth_header}
        client = await self._make_client(password='secret123')

        class _FakeResult:
            def __init__(self, values):
                self._values = values

            def all(self):
                return self._values

        class _FakeDB:
            def scalars(self, stmt):
                if 'uploadstreamers' in str(stmt).lower():
                    return _FakeResult(['data/legacy_template.json'])
                return _FakeResult(['data/registered.json'])

        mock_session_cm = MagicMock()
        mock_session_cm.__enter__.return_value = _FakeDB()
        mock_session_cm.__exit__.return_value = False

        with patch.object(web_mod, 'SessionLocal', return_value=mock_session_cm):
            # 缺少 user 参数 -> 400
            self.assertEqual((await client.get('/bili/space/myinfo', headers=headers)).status, 400)
            self.assertEqual((await client.get('/bili/seasons', headers=headers)).status, 400)

            # 未登记的路径 -> 403
            r1 = await client.get('/bili/space/myinfo?user=../etc/passwd', headers=headers)
            self.assertEqual(r1.status, 403)
            r2 = await client.get('/bili/space/myinfo?user=data/unregistered.json', headers=headers)
            self.assertEqual(r2.status, 403)
            r3 = await client.get('/bili/seasons?user=data/unregistered.json', headers=headers)
            self.assertEqual(r3.status, 403)

            # 已登记路径 & 旧模板 user_cookie 路径 -> 放行并正常返回
            mock_bili = MagicMock()
            mock_bili.myinfo.return_value = {'code': 0, 'data': {'name': 'tester'}}
            mock_bili_inst = MagicMock()
            mock_bili_inst._BiliBili__session = MagicMock()
            mock_bili_inst.list_seasons.return_value = {
                'seasons': [{'season': {'id': 10, 'title': '合集1'}, 'sections': {'sections': [{'id': 20}]}}]
            }
            mock_bili.__class__ = MagicMock(return_value=mock_bili_inst)

            def fake_load_cookies(path):
                config.data['user'] = {'cookies': {'SESSDATA': 'abc'}}

            with patch.object(config, 'load_cookies', side_effect=fake_load_cookies), \
                    patch.object(web_mod, 'BiliBili', mock_bili):
                ok_info = await client.get('/bili/space/myinfo?user=data/registered.json', headers=headers)
                self.assertEqual(ok_info.status, 200)
                self.assertEqual((await ok_info.json())['data']['name'], 'tester')

                ok_seasons = await client.get('/bili/seasons?user=data/registered.json', headers=headers)
                self.assertEqual(ok_seasons.status, 200)
                self.assertEqual(await ok_seasons.json(), [{'season_id': 10, 'title': '合集1', 'section_id': 20}])

                legacy_seasons = await client.get('/bili/seasons?user=data/legacy_template.json', headers=headers)
                self.assertEqual(legacy_seasons.status, 200)
                self.assertEqual(await legacy_seasons.json(), [{'season_id': 10, 'title': '合集1', 'section_id': 20}])

    async def test_bili_proxy_security_and_size_limit(self):
        headers = {'Authorization': _basic_auth('secret123')}
        client = await self._make_client(password='secret123')

        # 缺参 -> 400
        self.assertEqual((await client.get('/bili/proxy', headers=headers)).status, 400)

        # 非法 scheme / 非 hdslb.com 域名 / userinfo -> 403
        for bad_url in [
            'file:///etc/passwd',
            'ftp://i0.hdslb.com/bfs/face/a.jpg',
            'https://example.com/a.jpg',
            'https://evil-hdslb.com/a.jpg',
            'https://user:pass@i0.hdslb.com/a.jpg',
        ]:
            resp = await client.get(f'/bili/proxy?url={quote(bad_url, safe="")}', headers=headers)
            self.assertEqual(resp.status, 403, f"Expected 403 for {bad_url}, got {resp.status}")

        class _FakeContent:
            def __init__(self, chunks):
                self._chunks = chunks

            async def iter_chunked(self, size):
                for ch in self._chunks:
                    yield ch

        class _FakeUpstreamResp:
            def __init__(self, status=200, resp_headers=None, chunks=None):
                self.status = status
                self.headers = resp_headers or {}
                self.content = _FakeContent(chunks or [])

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

        recorded_timeouts = []
        recorded_get_calls = []

        def make_session_cls(upstream_resp=None, side_effect=None):
            class _FakeSession:
                def __init__(self, timeout=None, **kwargs):
                    recorded_timeouts.append(timeout)

                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    return False

                def get(self, url, **kwargs):
                    recorded_get_calls.append((url, kwargs))
                    if side_effect is not None:
                        raise side_effect
                    return upstream_resp

            return _FakeSession

        target = 'https://i0.hdslb.com/bfs/face/test.jpg'

        # 1. 正常代理：校验 allow_redirects=False 与 timeout=15
        ok_upstream = _FakeUpstreamResp(
            status=200,
            resp_headers={'Content-Type': 'image/jpeg'},
            chunks=[b'hello ', b'image'],
        )
        with patch.object(web_mod, 'ClientSession', make_session_cls(ok_upstream)):
            ok_resp = await client.get(f'/bili/proxy?url={quote(target, safe="")}', headers=headers)
            self.assertEqual(ok_resp.status, 200)
            self.assertEqual(await ok_resp.read(), b'hello image')
            self.assertEqual(ok_resp.headers.get('Content-Type'), 'image/jpeg')

        self.assertEqual(len(recorded_timeouts), 1)
        self.assertIsInstance(recorded_timeouts[0], ClientTimeout)
        self.assertEqual(recorded_timeouts[0].total, 15)
        self.assertEqual(recorded_get_calls[0][1].get('allow_redirects'), False)

        # 2. Content-Length 超 10MB -> 413
        oversize_header_resp = _FakeUpstreamResp(
            status=200,
            resp_headers={'Content-Length': str(web_mod.MAX_PROXY_BODY_SIZE + 1)},
            chunks=[],
        )
        with patch.object(web_mod, 'ClientSession', make_session_cls(oversize_header_resp)):
            r_hdr = await client.get(f'/bili/proxy?url={quote(target, safe="")}', headers=headers)
            self.assertEqual(r_hdr.status, 413)

        # 3. 流式分块超 10MB（无 Content-Length） -> 413
        big_chunk = b'a' * (6 * 1024 * 1024)
        oversize_stream_resp = _FakeUpstreamResp(
            status=200,
            resp_headers={},
            chunks=[big_chunk, big_chunk],
        )
        with patch.object(web_mod, 'ClientSession', make_session_cls(oversize_stream_resp)):
            r_stream = await client.get(f'/bili/proxy?url={quote(target, safe="")}', headers=headers)
            self.assertEqual(r_stream.status, 413)

        # 4. 超时 -> 504
        with patch.object(web_mod, 'ClientSession', make_session_cls(side_effect=asyncio.TimeoutError())):
            r_timeout = await client.get(f'/bili/proxy?url={quote(target, safe="")}', headers=headers)
            self.assertEqual(r_timeout.status, 504)


if __name__ == '__main__':
    unittest.main()
