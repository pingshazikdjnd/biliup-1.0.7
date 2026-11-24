import asyncio
import copy
import inspect
import json
import os
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from aiohttp.test_utils import TestClient, TestServer

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
        import biliup.web as web_mod
        import biliup.database.db as db_mod
        import biliup.__main__ as main_mod
        import biliup.common.reload as reload_mod
        from biliup.plugins import bili_webup as bili_webup_mod
        return app_mod, web_mod, db_mod, main_mod, reload_mod, bili_webup_mod
    finally:
        asyncio.get_running_loop = orig_get_running_loop
        temp_loop.close()


app_mod, web_mod, db_mod, main_mod, reload_mod, bili_webup_mod = _bootstrap_biliup()


class TestPR9PythonHttpsAndOps(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._orig_cwd = os.getcwd()
        self._tmpdir = tempfile.TemporaryDirectory()
        os.chdir(self._tmpdir.name)
        self._orig_password = web_mod.GLOBAL_PASSWORD
        self._config_snapshot = {
            k: (v if k == 'PluginInfo' else copy.deepcopy(v))
            for k, v in config.data.items()
        }
        self._pending_snapshot = set(app_mod.pending_download_urls)
        self._plugin_info_snapshot = app_mod.context.get('PluginInfo')
        app_mod.pending_download_urls.clear()
        web_mod._shutdown_qr_pool()

    async def asyncTearDown(self):
        web_mod._shutdown_qr_pool()
        os.chdir(self._orig_cwd)
        self._tmpdir.cleanup()
        web_mod.set_global_password(self._orig_password)
        app_mod.pending_download_urls.clear()
        app_mod.pending_download_urls.update(self._pending_snapshot)
        if self._plugin_info_snapshot is not None:
            app_mod.context['PluginInfo'] = self._plugin_info_snapshot
        else:
            app_mod.context.pop('PluginInfo', None)
        config.data.clear()
        config.data.update(self._config_snapshot)

    async def test_web_qrcode_json_loads_lazy_pool_url_status_and_cookie_login(self):
        # 进程池默认未创建（懒初始化）
        self.assertIsNone(web_mod._qr_pool)

        app = web_mod.create_app(password=None, static_dir=self._tmpdir.name)
        async with TestClient(TestServer(app)) as client:
            # 1. /v1/get_qrcode 使用 json.loads 而不是 eval
            with patch.object(web_mod.stream_gears, 'get_qrcode', return_value='{"code": 0, "data": {"url": "https://qr"}}'):
                resp = await client.get('/v1/get_qrcode')
                self.assertEqual(resp.status, 200)
                self.assertEqual(await resp.json(), {'code': 0, 'data': {'url': 'https://qr'}})

            # 非法 Python 表达式绝不会触发 eval，必须返回 400
            with patch('builtins.eval', side_effect=AssertionError('eval must not be called')):
                with patch.object(web_mod.stream_gears, 'get_qrcode', return_value='__import__("os").getpid()'):
                    resp = await client.get('/v1/get_qrcode')
                    self.assertEqual(resp.status, 400)

            # 2. /url-status 读取 context['PluginInfo'].url_status
            app_mod.context['PluginInfo'] = SimpleNamespace(
                url_status={'https://live.bilibili.com/1': 1, 'https://live.bilibili.com/2': 0}
            )
            resp = await client.get('/url-status')
            self.assertEqual(resp.status, 200)
            self.assertEqual(
                await resp.json(),
                {'https://live.bilibili.com/1': 1, 'https://live.bilibili.com/2': 0},
            )

            # 3. /api/login_by_cookie 缺失 cookie 或抛异常时返回 400；成功返回 None 时返回 200
            config.data.pop('toml', None)
            config.data['user'] = {}
            resp = await client.get('/api/login_by_cookie')
            self.assertEqual(resp.status, 400)

            config.data['user'] = {'cookies': {'SESSDATA': 'abc'}}
            fake_bili = MagicMock()
            fake_bili.__enter__.return_value = fake_bili
            fake_bili.login_by_cookies.side_effect = Exception('invalid cookie')
            with patch.object(web_mod, '_bili_client', return_value=fake_bili):
                resp = await client.get('/api/login_by_cookie')
                self.assertEqual(resp.status, 400)

            fake_bili.login_by_cookies.side_effect = None
            fake_bili.login_by_cookies.return_value = None  # BiliBili.login_by_cookies 成功真实返回 None
            with patch.object(web_mod, '_bili_client', return_value=fake_bili):
                resp = await client.get('/api/login_by_cookie')
                self.assertEqual(resp.status, 200)

        # 验证懒初始化（spawn 上下文）与 shutdown 显式终止残留子进程
        fake_proc = MagicMock()
        fake_proc.is_alive.side_effect = [True, False]
        fake_pool = MagicMock()
        fake_pool._processes = {1: fake_proc}
        with patch.object(web_mod.concurrent.futures, 'ProcessPoolExecutor', return_value=fake_pool) as pool_cls:
            p1 = web_mod._get_qr_pool()
            p2 = web_mod._get_qr_pool()
            self.assertIs(p1, fake_pool)
            self.assertIs(p2, fake_pool)
            pool_cls.assert_called_once()
            self.assertEqual(pool_cls.call_args.kwargs['mp_context'].get_start_method(), 'spawn')
        web_mod._shutdown_qr_pool()
        fake_pool.shutdown.assert_called_once_with(wait=False, cancel_futures=True)
        fake_proc.terminate.assert_called_once()
        fake_proc.join.assert_called()
        self.assertIsNone(web_mod._qr_pool)

    async def test_bili_webup_uses_https_and_raises_timeout_error(self):
        src = inspect.getsource(bili_webup_mod)
        for insecure in (
            'http://passport.bilibili.com',
            'http://api.bilibili.com',
            'http://member.bilibili.com',
        ):
            self.assertNotIn(insecure, src)

        bili = bili_webup_mod.BiliBili(bili_webup_mod.Data())
        session = bili._BiliBili__session

        ok_qr = MagicMock()
        ok_qr.json.return_value = {'code': 0, 'data': {'auth_code': 'ac'}}
        with patch.object(session, 'post', return_value=ok_qr) as post_mock:
            res = bili.get_qrcode()
            self.assertEqual(res['code'], 0)
            self.assertTrue(post_mock.call_args.args[0].startswith('https://passport.bilibili.com/'))

        pending_poll = MagicMock()
        pending_poll.json.return_value = {'code': 86039}
        with patch.object(session, 'post', return_value=pending_poll) as post_mock:
            with patch.object(bili_webup_mod.asyncio, 'sleep', return_value=None):
                with self.assertRaises(TimeoutError):
                    await bili.login_by_qrcode({'data': {'auth_code': 'ac'}})
            self.assertTrue(post_mock.call_args.args[0].startswith('https://passport.bilibili.com/'))

        myinfo_resp = MagicMock()
        myinfo_resp.json.return_value = {'code': 0, 'data': {}}
        with patch.object(session, 'get', return_value=myinfo_resp) as get_mock:
            bili.myinfo({'SESSDATA': 'x'})
            self.assertTrue(get_mock.call_args.args[0].startswith('https://api.bilibili.com/'))

    def test_db_rowcount_property_and_alembic_readonly_fallback(self):
        fake_db = MagicMock()
        fake_result = SimpleNamespace(rowcount=3)
        fake_db.execute.return_value = fake_result

        self.assertEqual(db_mod.delete_stream_info(fake_db, 'streamer_a'), 3)
        self.assertEqual(db_mod.delete_stream_info_by_date(fake_db, 'streamer_a', time.localtime()), 3)

        # 使用含空格的工作目录，验证 Alembic 实际解析出的 version_locations 不会被空格拆分
        spaced_dir = os.path.join(self._tmpdir.name, 'bili up space')
        os.makedirs(spaced_dir, exist_ok=True)
        prev_cwd = os.getcwd()
        os.chdir(spaced_dir)
        try:
            orig_makedirs = os.makedirs
            captured_cfg = {}

            def _fail_pkg_versions(path, *args, **kwargs):
                if path.endswith(os.path.join('migration', 'versions')):
                    raise PermissionError('read-only site-packages')
                return orig_makedirs(path, *args, **kwargs)

            def _fake_stamp(cfg, *args, **kwargs):
                from alembic.script import ScriptDirectory
                captured_cfg['cfg'] = cfg
                captured_cfg['parsed_locations'] = list(ScriptDirectory.from_config(cfg)._version_locations)

            def _fake_revision(cfg, **kwargs):
                captured_cfg['version_path'] = kwargs.get('version_path')
                return None

            with patch.object(db_mod.os, 'makedirs', side_effect=_fail_pkg_versions):
                with patch.object(db_mod.command, 'stamp', side_effect=_fake_stamp):
                    with patch.object(db_mod.command, 'revision', side_effect=_fake_revision):
                        db_mod.migrate_via_alembic()

            expected_fallback = os.path.abspath(os.path.join('.', 'data', 'migration_versions'))
            self.assertTrue(os.path.isdir(expected_fallback))
            self.assertEqual(captured_cfg.get('version_path'), expected_fallback)
            self.assertIn(expected_fallback, captured_cfg.get('parsed_locations', []))
        finally:
            os.chdir(prev_cwd)

    def test_main_deep_merge_log_conf_skips_none(self):
        base = {
            'version': 1,
            'handlers': {
                'console': {'level': 'INFO', 'class': 'logging.StreamHandler', 'stream': 'ext://sys.stdout'},
                'file': {
                    'level': 'DEBUG',
                    'class': 'biliup.common.log.SafeRotatingFileHandler',
                    'when': 'W0',
                    'interval': 1,
                    'backupCount': 1,
                    'filename': 'ds_update.log',
                },
            },
            'loggers': {
                'biliup': {'handlers': ['file'], 'level': 'INFO'},
            },
        }
        override = {
            'version': None,
            'handlers': {
                'console': {'level': 'WARNING', 'class': None},
                'file': {'class': 'logging.FileHandler', 'filename': 'app.log'},
            },
            'loggers': {
                'biliup': {'level': 'DEBUG'},
            },
        }
        main_mod._deep_merge_dict(base, override)
        self.assertEqual(base['version'], 1)
        # console 未更换 class，保留原 class/stream 并更新 level
        self.assertEqual(base['handlers']['console']['level'], 'WARNING')
        self.assertEqual(base['handlers']['console']['class'], 'logging.StreamHandler')
        self.assertEqual(base['handlers']['console']['stream'], 'ext://sys.stdout')
        # file 更换了 class，整体替换不残留 when/interval/backupCount
        self.assertEqual(base['handlers']['file'], {'class': 'logging.FileHandler', 'filename': 'app.log'})
        self.assertEqual(base['loggers']['biliup']['handlers'], ['file'])
        self.assertEqual(base['loggers']['biliup']['level'], 'DEBUG')

        # override 为 None 时不报错
        main_mod._deep_merge_dict(base, None)

    def test_reload_work_free_is_docker_and_safe_restart(self):
        app_mod.context['PluginInfo'] = SimpleNamespace(url_status={'u1': 0})
        app_mod.context['url_upload_count'] = {'u1': 0}
        app_mod.pending_download_urls.clear()
        self.assertTrue(reload_mod.AutoReload._work_free())

        # 正在下载中 -> False
        app_mod.context['PluginInfo'].url_status['u1'] = 1
        self.assertFalse(reload_mod.AutoReload._work_free())
        app_mod.context['PluginInfo'].url_status['u1'] = 0

        # 待派发下载 -> False
        app_mod.pending_download_urls.add('u1')
        self.assertFalse(reload_mod.AutoReload._work_free())
        app_mod.pending_download_urls.clear()

        # 正在上传中 -> False
        app_mod.context['url_upload_count']['u1'] = 1
        self.assertFalse(reload_mod.AutoReload._work_free())
        app_mod.context['url_upload_count']['u1'] = 0

        # 存在媒体文件 -> False
        tmp_flv = os.path.join(self._tmpdir.name, 'rec.flv')
        with open(tmp_flv, 'wb') as f:
            f.write(b'123')
        self.assertFalse(reload_mod.AutoReload._work_free())
        os.remove(tmp_flv)
        self.assertTrue(reload_mod.AutoReload._work_free())

        # is_docker 使用 with open 关闭句柄
        fake_cgroup = os.path.join(self._tmpdir.name, 'cgroup')
        with open(fake_cgroup, 'w', encoding='utf-8') as f:
            f.write('1:name=systemd:/docker/abcdef\n')
        real_open = open
        opened_files = []

        def _tracking_open(path, *args, **kwargs):
            fh = real_open(fake_cgroup, *args, **kwargs)
            opened_files.append(fh)
            return fh

        with patch.object(reload_mod.os.path, 'exists', return_value=False):
            with patch.object(reload_mod.os.path, 'isfile', return_value=True):
                with patch('builtins.open', side_effect=_tracking_open):
                    self.assertTrue(reload_mod.is_docker())
        self.assertTrue(all(f.closed for f in opened_files))

        # _build_restart_cmd 与 _cleanup_own_pidfile
        with patch.object(sys, 'argv', ['biliup', '-P', '19159']):
            with patch('biliup.IS_FROZEN', False):
                self.assertEqual(reload_mod._build_restart_cmd(), [sys.executable, '-m', 'biliup', '-P', '19159'])
            with patch('biliup.IS_FROZEN', True):
                self.assertEqual(reload_mod._build_restart_cmd(), [sys.executable, '-P', '19159'])

        pidfile = os.path.join(self._tmpdir.name, 'watch_process.pid')
        with open(pidfile, 'w', encoding='utf-8') as f:
            f.write('999999\n')
        reload_mod._cleanup_own_pidfile(pidfile)
        self.assertTrue(os.path.exists(pidfile))  # 非本进程 PID 不误删

        with open(pidfile, 'w', encoding='utf-8') as f:
            f.write(f'{os.getpid()}\n')
        reload_mod._cleanup_own_pidfile(pidfile)
        self.assertFalse(os.path.exists(pidfile))  # 本进程 PID 安全清理

        # POSIX execv 不含 'start' 时保留 PID 文件；含 'start' 时清理 PID 文件；且都会先关闭扫码池
        with open(pidfile, 'w', encoding='utf-8') as f:
            f.write(f'{os.getpid()}\n')
        with patch.object(reload_mod.sys, 'platform', 'linux'), \
                patch.object(reload_mod.sys, 'argv', ['biliup', '-P', '19159']), \
                patch.object(reload_mod.os, 'execv', create=True) as execv_mock, \
                patch.object(web_mod, '_shutdown_qr_pool') as shutdown_mock:
            reload_mod._restart_process()
            shutdown_mock.assert_called_once()
            execv_mock.assert_called_once()
            self.assertTrue(os.path.exists(pidfile))

        with patch.object(reload_mod.sys, 'platform', 'linux'), \
                patch.object(reload_mod.sys, 'argv', ['biliup', 'start']), \
                patch.object(reload_mod.os, 'execv', create=True) as execv_mock, \
                patch.object(web_mod, '_shutdown_qr_pool') as shutdown_mock:
            reload_mod._restart_process()
            shutdown_mock.assert_called_once()
            execv_mock.assert_called_once()
            self.assertFalse(os.path.exists(pidfile))

        # 使用 'biliup --password restart restart' 启动时：仅子命令 'restart' 转为 'start' 并清理 PID，密码值不受影响
        with open(pidfile, 'w', encoding='utf-8') as f:
            f.write(f'{os.getpid()}\n')
        with patch.object(reload_mod.sys, 'platform', 'linux'), \
                patch.object(reload_mod.sys, 'argv', ['biliup', '--password', 'restart', 'restart']), \
                patch('biliup.IS_FROZEN', False), \
                patch.object(reload_mod.os, 'execv', create=True) as execv_mock, \
                patch.object(web_mod, '_shutdown_qr_pool') as shutdown_mock:
            reload_mod._restart_process()
            shutdown_mock.assert_called_once()
            execv_mock.assert_called_once_with(
                sys.executable,
                [sys.executable, '-m', 'biliup', '--password', 'restart', 'start'],
            )
            self.assertFalse(os.path.exists(pidfile))


if __name__ == '__main__':
    unittest.main()
