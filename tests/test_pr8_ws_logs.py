import asyncio
import base64
import copy
import os
import tempfile
import unittest
from urllib.parse import quote

from aiohttp import WSServerHandshakeError
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


class TestPR8WebsocketLogs(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._orig_password = web_mod.GLOBAL_PASSWORD
        self._orig_cwd = os.getcwd()
        self._config_snapshot = {
            k: (v if k == 'PluginInfo' else copy.deepcopy(v))
            for k, v in config.data.items()
        }
        self._tmpdir = tempfile.TemporaryDirectory()
        self._log_path = os.path.join(self._tmpdir.name, 'ds_update.log')
        with open(self._log_path, 'wb') as f:
            f.write('初始日志\n'.encode('utf-8'))
        os.chdir(self._tmpdir.name)

    async def asyncTearDown(self):
        os.chdir(self._orig_cwd)
        self._tmpdir.cleanup()
        web_mod.set_global_password(self._orig_password)
        config.data.clear()
        config.data.update(self._config_snapshot)

    async def _create_app(self, password=None):
        return web_mod.create_app(password=password, static_dir=self._tmpdir.name)

    async def test_websocket_logs_split_multibyte_utf8_and_partial_lines(self):
        app = await self._create_app(password=None)
        async with TestClient(TestServer(app)) as client:
            async with client.ws_connect('/ws/logs?file=ds_update.log') as ws:
                first = await asyncio.wait_for(ws.receive_str(), timeout=3)
                self.assertEqual(first, '初始日志')

                # 写入一行完整中文 + 下一行中文的半截多字节 UTF-8（无换行）
                split_line_bytes = '你好世界，直播开始\n'.encode('utf-8')
                with open(self._log_path, 'ab') as f:
                    f.write('第一行完整中文\n'.encode('utf-8'))
                    f.write(split_line_bytes[:2])  # 半截多字节序列，不应触发 UnicodeDecodeError
                    f.flush()

                line1 = await asyncio.wait_for(ws.receive_str(), timeout=3)
                self.assertEqual(line1, '第一行完整中文')

                # 半截未换行内容不应被提前消费或发送乱码
                with self.assertRaises(asyncio.TimeoutError):
                    await asyncio.wait_for(ws.receive_str(), timeout=0.8)

                # 补齐剩余字节及换行符，应完整解码为原中文字符串
                with open(self._log_path, 'ab') as f:
                    f.write(split_line_bytes[2:])
                    f.flush()

                line2 = await asyncio.wait_for(ws.receive_str(), timeout=3)
                self.assertEqual(line2, '你好世界，直播开始')
                self.assertNotIn('\ufffd', line2)

                await ws.close()

    async def test_websocket_logs_invalid_utf8_replace_and_truncation_reload(self):
        app = await self._create_app(password=None)
        async with TestClient(TestServer(app)) as client:
            async with client.ws_connect('/ws/logs?file=ds_update.log') as ws:
                first = await asyncio.wait_for(ws.receive_str(), timeout=3)
                self.assertEqual(first, '初始日志')

                # 包含非法 UTF-8 字节但以换行结束时，应 errors='replace' 安全解码而非断连
                with open(self._log_path, 'ab') as f:
                    f.write(b'bad:\xff\xfeok\n')
                    f.flush()

                msg = await asyncio.wait_for(ws.receive_str(), timeout=3)
                self.assertEqual(msg, 'bad:\ufffd\ufffdok')

                # 文件被截断时应重新加载并继续跟踪后续追加内容
                with open(self._log_path, 'wb') as f:
                    f.write('短\n'.encode('utf-8'))
                    f.flush()

                notice = await asyncio.wait_for(ws.receive_str(), timeout=3)
                self.assertIn('截断', notice)
                reloaded = await asyncio.wait_for(ws.receive_str(), timeout=3)
                self.assertEqual(reloaded, '短')

                with open(self._log_path, 'ab') as f:
                    f.write('截断后新日志\n'.encode('utf-8'))
                    f.flush()

                tail = await asyncio.wait_for(ws.receive_str(), timeout=3)
                self.assertEqual(tail, '截断后新日志')
                await ws.close()

    async def test_websocket_logs_utf8_basic_auth_and_disallowed_file(self):
        utf8_pwd = '测试密码#2026'
        app = await self._create_app(password=utf8_pwd)
        async with TestClient(TestServer(app)) as client:
            with self.assertRaises(WSServerHandshakeError) as ctx:
                await client.ws_connect('/ws/logs?file=ds_update.log')
            self.assertEqual(ctx.exception.status, 401)

            auth_q = quote(_basic_auth(utf8_pwd))
            async with client.ws_connect(f'/ws/logs?file=ds_update.log&authorization={auth_q}') as ws:
                first = await asyncio.wait_for(ws.receive_str(), timeout=3)
                self.assertEqual(first, '初始日志')
                await ws.close()

            async with client.ws_connect(f'/ws/logs?file=../secret.log&authorization={auth_q}') as ws:
                err_msg = await asyncio.wait_for(ws.receive_str(), timeout=3)
                self.assertIn('不允许访问', err_msg)


if __name__ == '__main__':
    unittest.main()
