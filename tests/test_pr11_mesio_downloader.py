import asyncio
import copy
import errno
import json
import os
import queue
import shutil
import signal
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import psutil

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from biliup.config import config
from biliup.database.models import BaseModel


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
        import biliup.database.db as db_mod
        import biliup.engine.download as download_mod
        import biliup.handler as handler_mod
        import biliup.__main__ as main_mod
        from biliup.engine.event import Event
        from biliup.engine.upload import UploadBase
        return app_mod, db_mod, download_mod, handler_mod, main_mod, Event, UploadBase
    finally:
        asyncio.get_running_loop = orig_get_running_loop
        temp_loop.close()


app_mod, db_mod, download_mod, handler_mod, main_mod, Event, UploadBase = _bootstrap_biliup()


class DummyMesioDownloader(download_mod.DownloadBase):
    def __init__(self, fname='test_up', url='https://live.bilibili.com/12345', suffix='flv', opt_args=None):
        super().__init__(fname, url, suffix=suffix, opt_args=opt_args)
        self.raw_stream_url = 'https://cdn.example.com/live/stream.flv?token=abc'
        self.room_title = '测试直播间'
        self.check_calls = 0

    async def acheck_stream(self, is_check=False):
        self.check_calls += 1
        return True


class FakeStdout:
    def __init__(self):
        self._queue = queue.Queue()
        self.closed = False

    def push(self, data: bytes):
        self._queue.put(data)

    def push_eof(self):
        self._queue.put(b'')

    def readline(self):
        if self.closed:
            return b''
        item = self._queue.get()
        return b'' if item is None else item

    def close(self):
        self.closed = True
        self._queue.put(b'')


class FakePopenProcess:
    def __init__(self, cmd, *, returncode=0, on_poll=None, on_signal=None, on_kill=None):
        self.cmd = list(cmd)
        self.stdout = FakeStdout()
        self.returncode = None
        self._final_returncode = returncode
        self._on_poll = on_poll
        self._on_signal = on_signal
        self._on_kill = on_kill
        self.signals = []
        self.terminated = False
        self.killed = False
        self.poll_count = 0

    def poll(self):
        self.poll_count += 1
        if self._on_poll is not None:
            res = self._on_poll(self, self.poll_count)
            if res is not None:
                self.returncode = res
        return self.returncode

    def send_signal(self, sig):
        self.signals.append(sig)
        if self._on_signal is not None:
            self._on_signal(self, sig)

    def terminate(self):
        self.terminated = True
        if self.returncode is None:
            self.returncode = self._final_returncode
        self.stdout.push_eof()

    def kill(self):
        self.killed = True
        if self._on_kill is not None:
            self._on_kill(self)
        elif self.returncode is None:
            self.returncode = -9
        self.stdout.push_eof()

    def wait(self, timeout=None):
        if self.returncode is None:
            self.returncode = self._final_returncode
        return self.returncode


def _extract_session_dir(cmd):
    idx = cmd.index('-o')
    return cmd[idx + 1]


class TestPR11MesioDownloader(unittest.TestCase):
    def setUp(self):
        self._orig_cwd = os.getcwd()
        self._tmpdir = tempfile.TemporaryDirectory()
        os.chdir(self._tmpdir.name)
        self._config_snapshot = {
            k: (v if k == 'PluginInfo' else copy.deepcopy(v))
            for k, v in config.data.items()
        }
        self._plugin_info_snapshot = app_mod.context.get('PluginInfo')
        self._url_upload_snapshot = copy.deepcopy(app_mod.context.get('url_upload_count', {}))
        self._file_upload_snapshot = copy.deepcopy(app_mod.context.get('file_upload_count', {}))
        config.data.clear()
        config.data.update({
            'streamers': {},
            'url_upload_count': {},
            'upload_filename': [],
            'file_upload_count': {},
            'sync_downloader_map': {},
        })
        app_mod.context['url_upload_count'] = {}
        app_mod.context['file_upload_count'] = {}

        db_file = os.path.join(self._tmpdir.name, 'test_pr11.sqlite3')
        self._engine = create_engine(f'sqlite:///{db_file}', connect_args={'check_same_thread': False})
        BaseModel.metadata.create_all(self._engine)
        self._TestSession = sessionmaker(bind=self._engine, autocommit=False)

    def tearDown(self):
        self._engine.dispose()
        os.chdir(self._orig_cwd)
        self._tmpdir.cleanup()
        if self._plugin_info_snapshot is not None:
            app_mod.context['PluginInfo'] = self._plugin_info_snapshot
        else:
            app_mod.context.pop('PluginInfo', None)
        app_mod.context['url_upload_count'] = self._url_upload_snapshot
        app_mod.context['file_upload_count'] = self._file_upload_snapshot
        config.data.clear()
        config.data.update(self._config_snapshot)

    def _join_segment_threads(self, dl):
        for thread in dl.segment_processor_thread:
            thread.join(timeout=5)

    def test_helpers_duration_binary_and_unique_target(self):
        self.assertEqual(download_mod._parse_duration_to_seconds('01:00:00'), 3600)
        self.assertEqual(download_mod._parse_duration_to_seconds('00:01:30'), 90)
        self.assertIsNone(download_mod._parse_duration_to_seconds('00:00:00'))
        self.assertIsNone(download_mod._parse_duration_to_seconds(''))
        self.assertIsNone(download_mod._parse_duration_to_seconds(None))
        self.assertIsNone(download_mod._parse_duration_to_seconds('invalid'))
        self.assertIsNone(download_mod._parse_duration_to_seconds('01:70:00'))
        self.assertIsNone(download_mod._parse_duration_to_seconds('-00:05:00'))
        self.assertIsNone(download_mod._parse_duration_to_seconds('00:00:²'))
        self.assertIsNone(download_mod._parse_duration_to_seconds('00:00:١'))

        with patch('biliup.engine.download.shutil.which', return_value='/usr/bin/mesio') as which_mock:
            self.assertEqual(download_mod._resolve_mesio_binary(None), '/usr/bin/mesio')
            self.assertEqual(download_mod._resolve_mesio_binary('   '), '/usr/bin/mesio')
            which_mock.assert_called_with('mesio')

        with patch('biliup.engine.download.shutil.which', return_value='/opt/custom-mesio') as which_mock:
            self.assertEqual(download_mod._resolve_mesio_binary(' custom-mesio '), '/opt/custom-mesio')
            which_mock.assert_called_once_with('custom-mesio')

        bin_dir = os.path.join(self._tmpdir.name, 'bin')
        os.makedirs(bin_dir, exist_ok=True)
        bin_file = os.path.join(bin_dir, 'mesio.exe')
        with open(bin_file, 'wb') as f:
            f.write(b'exe')
        if os.name != 'nt':
            os.chmod(bin_file, 0o755)
        self.assertEqual(download_mod._resolve_mesio_binary(f'  {bin_file}  '), bin_file)
        self.assertIsNone(download_mod._resolve_mesio_binary(os.path.join(bin_dir, 'missing.exe')))

        # POSIX 不可执行文件应被拒绝返回 None；具备可执行权限则返回路径
        with patch('biliup.engine.download.os.name', 'posix'), \
                patch('biliup.engine.download.os.access', return_value=False):
            self.assertIsNone(download_mod._resolve_mesio_binary(bin_file))
        with patch('biliup.engine.download.os.name', 'posix'), \
                patch('biliup.engine.download.os.access', return_value=True):
            self.assertEqual(download_mod._resolve_mesio_binary(bin_file), bin_file)

        self.assertEqual(download_mod._allocate_unique_media_target('clip', '.flv', '.'), 'clip.flv')
        with open('clip.flv', 'wb') as f:
            f.write(b'1')
        self.assertEqual(download_mod._allocate_unique_media_target('clip', '.flv', '.'), 'clip_p001.flv')
        with open('clip_p001.flv', 'wb') as f:
            f.write(b'2')
        self.assertEqual(download_mod._allocate_unique_media_target('clip', '.flv', '.'), 'clip_p002.flv')

    def test_cli_arguments_headers_duration_size_and_no_opt_args(self):
        dl = DummyMesioDownloader(opt_args=['-ss', '00:00:10', '-c:v', 'libx264'])
        dl.downloader = 'mesio'
        dl.database_row_id = 42
        dl.segment_time = '01:00:00'
        dl.file_size = 1048576
        dl.stream_headers = {
            'User-Agent': 'BiliAgent/1.0\r\nX-Injected: bad',
            'Referer': 'https://live.bilibili.com/12345',
            'Accept-Encoding': 'gzip, deflate',
            'accept-encoding': 'br',
            'BlankHeader': '   ',
            'NullHeader': None,
        }

        captured = {}

        def fake_popen(cmd, **kwargs):
            captured['cmd'] = list(cmd)
            captured['kwargs'] = kwargs
            sdir = _extract_session_dir(cmd)
            with open(os.path.join(sdir, '.meta.json'), 'r', encoding='utf-8') as f:
                captured['meta'] = json.load(f)
            proc = FakePopenProcess(cmd, returncode=1, on_poll=lambda p, n: 1)
            proc.stdout.push_eof()
            return proc

        with patch('biliup.engine.download.subprocess.Popen', side_effect=fake_popen):
            ret = dl.mesio_download('/usr/local/bin/mesio')

        self.assertFalse(ret)
        self.assertEqual(captured['meta']['database_row_id'], 42)
        self.assertEqual(captured['meta']['name'], dl.fname)
        cmd = captured['cmd']
        self.assertEqual(cmd[0], '/usr/local/bin/mesio')
        self.assertIn('--fix', cmd)
        self.assertIn('--disable-log-file', cmd)
        self.assertIn('-o', cmd)
        session_dir = _extract_session_dir(cmd)
        self.assertTrue(session_dir.startswith('.mesio_tmp'))
        idx_n = cmd.index('-n')
        self.assertEqual(cmd[idx_n + 1], 'seg_%i')
        idx_ct = cmd.index('--connect-timeout')
        self.assertEqual(cmd[idx_ct + 1], '20')
        idx_rt = cmd.index('--read-timeout')
        self.assertEqual(cmd[idx_rt + 1], '30')

        h_values = [cmd[i + 1] for i, token in enumerate(cmd) if token == '-H']
        self.assertEqual(
            h_values,
            ['User-Agent: BiliAgent/1.0X-Injected: bad', 'Referer: https://live.bilibili.com/12345'],
        )
        self.assertTrue(all('accept-encoding' not in hv.lower() for hv in h_values))
        self.assertEqual(cmd[cmd.index('-d') + 1], '3600s')
        self.assertEqual(cmd[cmd.index('-m') + 1], '1048576B')
        self.assertEqual(cmd[-1], dl.raw_stream_url)
        for forbidden in ('-ss', '00:00:10', '-c:v', 'libx264', '--no-proxy'):
            self.assertNotIn(forbidden, cmd)

    def test_missing_binary_and_exit_code_fallback_behavior(self):
        # 1. mesio_bin missing -> falls back to ffmpeg_download() when ffmpeg exists
        dl = DummyMesioDownloader()
        dl.downloader = 'mesio'
        with patch('biliup.engine.download._resolve_mesio_binary', return_value=None), \
                patch('biliup.engine.download.shutil.which', side_effect=lambda name: '/usr/bin/ffmpeg' if name == 'ffmpeg' else None), \
                patch.object(dl, 'ffmpeg_download', return_value=True) as mock_ffmpeg:
            self.assertTrue(dl.download())
            mock_ffmpeg.assert_called_once_with()

        # 2. mesio_bin missing AND ffmpeg missing -> falls through to stream_gears_download
        dl2 = DummyMesioDownloader()
        dl2.downloader = 'mesio'
        with patch('biliup.engine.download._resolve_mesio_binary', return_value=None), \
                patch('biliup.engine.download.shutil.which', return_value=None), \
                patch('biliup.engine.download.stream_gears_download') as mock_sg:
            self.assertTrue(dl2.download())
            mock_sg.assert_called_once()

        # 3. exit code 2 with 0 segments -> auto falls back to ffmpeg_download()
        dl3 = DummyMesioDownloader()
        dl3.downloader = 'mesio'

        def popen_exit2(cmd, **kwargs):
            proc = FakePopenProcess(cmd, returncode=2, on_poll=lambda p, n: 2)
            proc.stdout.push(b'error: unexpected argument\n')
            proc.stdout.push_eof()
            return proc

        with patch('biliup.engine.download.subprocess.Popen', side_effect=popen_exit2), \
                patch('biliup.engine.download.shutil.which', side_effect=lambda name: '/usr/bin/ffmpeg' if name == 'ffmpeg' else None), \
                patch.object(dl3, 'ffmpeg_download', return_value=True) as mock_ffmpeg3:
            self.assertTrue(dl3.mesio_download('/usr/bin/mesio'))
            mock_ffmpeg3.assert_called_once_with()

        # 4. exit code 1 (e.g. 404) with 0 segments -> does NOT fall back to ffmpeg, returns False
        dl4 = DummyMesioDownloader()
        dl4.downloader = 'mesio'

        def popen_exit1(cmd, **kwargs):
            proc = FakePopenProcess(cmd, returncode=1, on_poll=lambda p, n: 1)
            proc.stdout.push(b'ERROR 404 Not Found\n')
            proc.stdout.push_eof()
            return proc

        with patch('biliup.engine.download.subprocess.Popen', side_effect=popen_exit1), \
                patch('biliup.engine.download.shutil.which', return_value='/usr/bin/ffmpeg'), \
                patch.object(dl4, 'ffmpeg_download') as mock_ffmpeg4:
            self.assertFalse(dl4.mesio_download('/usr/bin/mesio'))
            mock_ffmpeg4.assert_not_called()

        # 5. Popen 抛 OSError 时自动回退到 ffmpeg_download()，且在此前已完成 .mesio_tmp 清理
        dl5 = DummyMesioDownloader()
        dl5.downloader = 'mesio'

        def _check_cleaned_and_ok():
            self.assertFalse(os.path.exists('.mesio_tmp'))
            return True

        with patch('biliup.engine.download.subprocess.Popen', side_effect=OSError('spawn failed')), \
                patch('biliup.engine.download.shutil.which', side_effect=lambda name: '/usr/bin/ffmpeg' if name == 'ffmpeg' else None), \
                patch.object(dl5, 'ffmpeg_download', side_effect=_check_cleaned_and_ok) as mock_ffmpeg5:
            self.assertTrue(dl5.mesio_download('/usr/bin/mesio'))
            mock_ffmpeg5.assert_called_once_with()
        self.assertFalse(os.path.exists('.mesio_tmp'))

        # 6. Popen 抛 OSError 且无 ffmpeg 时返回 False 并清理 .mesio_tmp
        dl6 = DummyMesioDownloader()
        dl6.downloader = 'mesio'
        with patch('biliup.engine.download.subprocess.Popen', side_effect=OSError('spawn failed')), \
                patch('biliup.engine.download.shutil.which', return_value=None):
            self.assertFalse(dl6.mesio_download('/usr/bin/mesio'))
        self.assertFalse(os.path.exists('.mesio_tmp'))

    def test_isolated_dir_dual_flush_db_registration_and_static_prefix_collision(self):
        dl = DummyMesioDownloader(fname='主播100%测试')
        dl.downloader = 'mesio'
        dl.filename_prefix = 'static_rec'

        with self._TestSession() as db:
            dl.database_row_id = db_mod.add_stream_info(db, dl.fname, dl.url, time.localtime())

        state = {'session_dir': None, 'stage1_ok': False, 'stage2_ok': False}
        poll1_done = threading.Event()
        seg0_db_done = threading.Event()
        poll2_done = threading.Event()
        seg1_db_done = threading.Event()
        stage3_ready = threading.Event()
        drive_errors = []
        driver_threads = []

        orig_update_file_list = download_mod.update_file_list

        def tracked_update_file_list(db, row_id, file_name):
            res = orig_update_file_list(db, row_id, file_name)
            if file_name == 'static_rec.flv':
                seg0_db_done.set()
            elif file_name == 'static_rec_p001.flv':
                seg1_db_done.set()
            return res

        def fake_popen(cmd, **kwargs):
            sdir = _extract_session_dir(cmd)
            state['session_dir'] = sdir
            # 阶段 1：出现 seg_000.flv (1024B) 与正在写入的 seg_001.flv (0B)
            with open(os.path.join(sdir, 'seg_000.flv'), 'wb') as f:
                f.write(b'A' * 1024)
            with open(os.path.join(sdir, 'seg_001.flv'), 'wb') as f:
                pass

            def on_poll(proc, count):
                if count == 1:
                    poll1_done.set()
                    return None
                if count == 2:
                    # 验证阶段 1 后：seg_000.flv 已移出为 static_rec.flv，而 0B 的 seg_001.flv 仍在隔离目录
                    self.assertTrue(seg0_db_done.wait(timeout=5))
                    self.assertTrue(os.path.isfile('static_rec.flv'))
                    self.assertFalse(os.path.exists(os.path.join(sdir, 'seg_000.flv')))
                    self.assertTrue(os.path.isfile(os.path.join(sdir, 'seg_001.flv')))
                    state['stage1_ok'] = True
                    poll2_done.set()
                    return None
                if count == 3:
                    # 验证阶段 2 后：进程存活期间由 Closed segment 日志触发 seg_001.flv 被立即移出为 static_rec_p001.flv
                    self.assertTrue(stage3_ready.wait(timeout=5))
                    self.assertTrue(seg1_db_done.is_set())
                    self.assertTrue(os.path.isfile('static_rec_p001.flv'))
                    self.assertFalse(os.path.exists(os.path.join(sdir, 'seg_001.flv')))
                    state['stage2_ok'] = True
                    return 0
                return 0

            proc = FakePopenProcess(cmd, returncode=0, on_poll=on_poll)
            # 唤醒第 1 轮循环
            proc.stdout.push(b'mesio started\n')

            def _drive():
                try:
                    # 等待第 1 轮 poll 与 seg_000 移出及 DB 登记完成后，再注入 seg_001 字节与 Closed segment 日志
                    self.assertTrue(poll1_done.wait(timeout=5))
                    self.assertTrue(seg0_db_done.wait(timeout=5))
                    with open(os.path.join(sdir, 'seg_001.flv'), 'wb') as f:
                        f.write(b'B' * 2048)
                    closed_line = f'2026-11-25 INFO Closed segment path={os.path.join(sdir, "seg_001.flv")} tags=42\n'
                    proc.stdout.push(closed_line.encode('utf-8'))
                    # 等待第 2 轮 poll 与进程存活期间 seg_001 移出及 DB 登记完成，再准备收尾分段并退出
                    self.assertTrue(poll2_done.wait(timeout=5))
                    self.assertTrue(seg1_db_done.wait(timeout=5))
                    self.assertIsNone(proc.returncode)
                    with open(os.path.join(sdir, 'seg_002.flv'), 'wb') as f:
                        f.write(b'F' * 512)
                    with open(os.path.join(sdir, 'seg_003.mp4'), 'wb') as f:
                        f.write(b'M' * 256)
                    with open(os.path.join(sdir, 'seg_004.flv'), 'wb') as f:
                        pass
                    stage3_ready.set()
                    proc.stdout.push_eof()
                except Exception as exc:
                    drive_errors.append(exc)
                    stage3_ready.set()
                    proc.stdout.push_eof()

            t = threading.Thread(target=_drive, daemon=True)
            driver_threads.append(t)
            t.start()
            return proc

        with patch('biliup.engine.download.SessionLocal', self._TestSession), \
                patch('biliup.engine.download.subprocess.Popen', side_effect=fake_popen), \
                patch('biliup.engine.download.update_file_list', side_effect=tracked_update_file_list):
            ret = dl.mesio_download('/usr/bin/mesio')

        for t in driver_threads:
            t.join(timeout=5)
        self._join_segment_threads(dl)
        self.assertEqual(drive_errors, [])
        self.assertTrue(ret)
        self.assertTrue(state['stage1_ok'])
        self.assertTrue(state['stage2_ok'])

        self.assertTrue(os.path.isfile('static_rec.flv'))
        self.assertTrue(os.path.isfile('static_rec_p001.flv'))
        self.assertTrue(os.path.isfile('static_rec_p002.flv'))
        self.assertTrue(os.path.isfile('static_rec.mp4'))
        self.assertFalse(os.path.exists('.mesio_tmp'))

        with self._TestSession() as db:
            db_files = db_mod.get_file_list(db, dl.database_row_id)
        self.assertEqual(
            db_files,
            ['static_rec.flv', 'static_rec_p001.flv', 'static_rec_p002.flv', 'static_rec.mp4'],
        )

    def test_percent_escape_strftime_fallback_permission_retry_and_timerange_stop(self):
        dl = DummyMesioDownloader(fname='主播100%测试')
        dl.downloader = 'mesio'
        dl.room_title = '标题%Y'
        dl.filename_prefix = ''
        dl.time_range = '["00:00:00", "01:00:00"]'

        with self._TestSession() as db:
            dl.database_row_id = db_mod.add_stream_info(db, dl.fname, dl.url, time.localtime())

        real_rename = os.rename
        rename_attempts = {'count': 0}

        def flaky_rename(src, dst):
            rename_attempts['count'] += 1
            if rename_attempts['count'] == 1:
                raise PermissionError('transient lock')
            return real_rename(src, dst)

        def fake_popen(cmd, **kwargs):
            sdir = _extract_session_dir(cmd)
            with open(os.path.join(sdir, 'seg_000.flv'), 'wb') as f:
                f.write(b'DATA')

            def on_signal(proc, sig):
                proc.returncode = 0
                proc.stdout.push_eof()

            proc = FakePopenProcess(cmd, returncode=0, on_signal=on_signal)
            proc.stdout.push(f'Closed segment path={os.path.join(sdir, "seg_000.flv")} index=0\n'.encode('utf-8'))
            return proc

        real_strftime = time.strftime
        strftime_calls = {'count': 0}

        def flaky_strftime(fmt, t):
            strftime_calls['count'] += 1
            if strftime_calls['count'] >= 2:
                raise ValueError('bad strftime during flush')
            return real_strftime(fmt, t)

        with patch('biliup.engine.download.SessionLocal', self._TestSession), \
                patch('biliup.engine.download.os.name', 'nt'), \
                patch('biliup.engine.download.subprocess.Popen', side_effect=fake_popen), \
                patch('biliup.engine.download.os.rename', side_effect=flaky_rename), \
                patch('biliup.engine.download.time.sleep'), \
                patch('biliup.engine.download.time.strftime', side_effect=flaky_strftime), \
                patch('biliup.engine.download.check_timerange', return_value=False):
            ret = dl.mesio_download('/usr/bin/mesio')

        self._join_segment_threads(dl)
        # 即使已经移出 1 个分段，因 time_range 到期停止，仍必须返回 False 终止外层循环
        self.assertFalse(ret)
        self.assertGreaterEqual(rename_attempts['count'], 2)
        moved_files = [f for f in os.listdir('.') if f.endswith('.flv')]
        self.assertEqual(len(moved_files), 1)
        self.assertTrue(moved_files[0].startswith('主播100%测试'))

    def test_soft_watermark_graceful_stop_both_os_and_bypass_delay(self):
        Usage = SimpleNamespace

        for os_name, expected_sig in [
            ('nt', getattr(signal, 'CTRL_BREAK_EVENT', 1)),
            ('posix', signal.SIGTERM),
        ]:
            with self.subTest(os_name=os_name):
                subdir = os.path.join(self._tmpdir.name, f'work_{os_name}')
                os.makedirs(subdir, exist_ok=True)
                prev_cwd = os.getcwd()
                os.chdir(subdir)
                try:
                    config['disk_upload_threshold_gb'] = 10.0
                    dl = DummyMesioDownloader(fname=f'streamer_{os_name}')
                    dl.downloader = 'mesio'
                    dl.filename_prefix = 'soft_seg'

                    observed_signals = []

                    def fake_popen(cmd, **kwargs):
                        sdir = _extract_session_dir(cmd)
                        with open(os.path.join(sdir, 'seg_000.flv'), 'wb') as f:
                            f.write(b'S0' * 512)
                        with open(os.path.join(sdir, 'seg_001.flv'), 'wb') as f:
                            pass

                        def on_signal(proc, sig):
                            observed_signals.append(sig)
                            with open(os.path.join(sdir, 'seg_001.flv'), 'wb') as f:
                                f.write(b'S1' * 256)
                            proc.returncode = 0
                            proc.stdout.push_eof()

                        proc = FakePopenProcess(cmd, returncode=0, on_signal=on_signal)
                        proc.stdout.push(b'started\n')
                        return proc

                    bg_loop = asyncio.new_event_loop()
                    bg_thread = threading.Thread(target=bg_loop.run_forever, daemon=True)
                    bg_thread.start()
                    try:
                        with patch.object(download_mod, 'loop', bg_loop), \
                                patch('biliup.engine.download.SessionLocal', self._TestSession), \
                                patch('biliup.engine.download.os.name', os_name), \
                                patch('biliup.engine.download._resolve_mesio_binary', return_value='/usr/bin/mesio'), \
                                patch('biliup.engine.download.subprocess.Popen', side_effect=fake_popen), \
                                patch('biliup.engine.download.shutil.disk_usage', return_value=Usage(total=100 * 1024**3, used=95 * 1024**3, free=5 * 1024**3)):
                            stream_info = dl.start()
                    finally:
                        bg_loop.call_soon_threadsafe(bg_loop.stop)
                        bg_thread.join(timeout=2)
                        bg_loop.close()

                    self.assertEqual(dl.check_calls, 1)
                    self.assertEqual(observed_signals, [expected_sig])
                    self.assertTrue(stream_info['disk_soft_triggered'])
                    self.assertTrue(os.path.isfile('soft_seg.flv'))
                    self.assertTrue(os.path.isfile('soft_seg_p001.flv'))
                    self.assertFalse(os.path.exists('.mesio_tmp'))

                    # 验证 handler.process_upload 收到 disk_soft_triggered=True 时穿透 delay
                    mock_file = UploadBase.FileInfo(video='soft_seg.flv', danmaku=None)
                    config['delay'] = 300
                    config['max_upload_limit'] = 5
                    app_mod.context['url_upload_count'][dl.url] = 0
                    app_mod.context['PluginInfo'] = MagicMock()
                    app_mod.context['PluginInfo'].url_status = {dl.url: 0}

                    with patch('biliup.handler.UploadBase.file_list', return_value=[mock_file]), \
                            patch('biliup.handler.UploadBase.get_max_upload_count', return_value=0), \
                            patch('biliup.handler.upload', return_value=[mock_file]) as mock_upload, \
                            patch('biliup.handler.uploaded') as mock_uploaded, \
                            patch('biliup.handler.time.sleep') as mock_sleep:
                        handler_mod.process_upload(Event(handler_mod.UPLOAD, (stream_info,)))

                    mock_sleep.assert_not_called()
                    mock_upload.assert_called_once()
                    mock_uploaded.assert_called_once_with(f'streamer_{os_name}', None, [mock_file])
                finally:
                    os.chdir(prev_cwd)

    def test_soft_watermark_timeout_kills_hung_process(self):
        config['disk_upload_threshold_gb'] = 10.0
        dl = DummyMesioDownloader()
        dl.downloader = 'mesio'
        dl.filename_prefix = 'timeout_seg'

        with self._TestSession() as db:
            dl.database_row_id = db_mod.add_stream_info(db, dl.fname, dl.url, time.localtime())

        proc_holder = {}
        now_clock = {'value': 100.0}
        pre_deadline_killed = []

        def fake_popen(cmd, **kwargs):
            sdir = _extract_session_dir(cmd)
            with open(os.path.join(sdir, 'seg_000.flv'), 'wb') as f:
                f.write(b'H' * 100)
            with open(os.path.join(sdir, 'seg_001.flv'), 'wb') as f:
                f.write(b'T' * 50)

            def on_poll(proc, count):
                if count == 2:
                    pre_deadline_killed.append(proc.killed)
                    now_clock['value'] = 105.0
                elif count == 3:
                    pre_deadline_killed.append(proc.killed)
                    now_clock['value'] = 120.0
                return None

            proc = FakePopenProcess(cmd, returncode=-9, on_poll=on_poll)
            proc.stdout.push(b'started\n')
            proc_holder['proc'] = proc
            return proc

        with patch('biliup.engine.download.SessionLocal', self._TestSession), \
                patch('biliup.engine.download.subprocess.Popen', side_effect=fake_popen), \
                patch('biliup.engine.download.time.monotonic', side_effect=lambda: now_clock['value']), \
                patch('biliup.engine.download.shutil.disk_usage', return_value=SimpleNamespace(free=2 * 1024**3)):
            ret = dl.mesio_download('/usr/bin/mesio')

        self._join_segment_threads(dl)
        self.assertTrue(ret)
        self.assertEqual(pre_deadline_killed, [False, False])
        self.assertTrue(proc_holder['proc'].killed)
        self.assertTrue(os.path.isfile('timeout_seg.flv'))
        self.assertTrue(os.path.isfile('timeout_seg_p001.flv'))

    def test_send_signal_oserror_falls_back_to_terminate(self):
        config['disk_upload_threshold_gb'] = 10.0
        dl = DummyMesioDownloader()
        dl.downloader = 'mesio'
        dl.filename_prefix = 'sig_fallback'

        with self._TestSession() as db:
            dl.database_row_id = db_mod.add_stream_info(db, dl.fname, dl.url, time.localtime())

        proc_holder = {}

        def fake_popen(cmd, **kwargs):
            sdir = _extract_session_dir(cmd)
            with open(os.path.join(sdir, 'seg_000.flv'), 'wb') as f:
                f.write(b'A' * 100)
            with open(os.path.join(sdir, 'seg_001.flv'), 'wb') as f:
                pass
            proc = FakePopenProcess(cmd, returncode=0)

            def _raise_signal(sig):
                raise OSError('invalid console handle')

            proc.send_signal = _raise_signal
            proc.stdout.push(b'started\n')
            proc_holder['proc'] = proc
            return proc

        with patch('biliup.engine.download.SessionLocal', self._TestSession), \
                patch('biliup.engine.download.subprocess.Popen', side_effect=fake_popen), \
                patch('biliup.engine.download.shutil.disk_usage', return_value=SimpleNamespace(free=1 * 1024**3)):
            ret = dl.mesio_download('/usr/bin/mesio')

        self._join_segment_threads(dl)
        self.assertTrue(ret)
        self.assertTrue(proc_holder['proc'].terminated)
        self.assertFalse(proc_holder['proc'].killed)
        self.assertTrue(os.path.isfile('sig_fallback.flv'))

    def test_recover_stale_mesio_tmp_and_main_cold_start_hook(self):
        with self._TestSession() as db:
            row_id = db_mod.add_stream_info(db, 'up99', 'https://live.bilibili.com/99', time.localtime())

        # 预置冲突目标文件，确保恢复不覆盖现有文件；且文件名故意不含 {streamer} ('up99')
        with open('recovered_stream.flv', 'wb') as f:
            f.write(b'existing')

        stale_dir = os.path.join('.mesio_tmp', '99_12345')
        os.makedirs(stale_dir, exist_ok=True)
        with open(os.path.join(stale_dir, '.meta.json'), 'w', encoding='utf-8') as f:
            json.dump({'name': 'up99', 'base_name': 'recovered_stream', 'database_row_id': row_id}, f)
        with open(os.path.join(stale_dir, 'seg_000.flv'), 'wb') as f:
            f.write(b'payload_flv')
        with open(os.path.join(stale_dir, 'seg_001.flv'), 'wb') as f:
            pass

        stale_mp4_dir = os.path.join('.mesio_tmp', '99_12346')
        os.makedirs(stale_mp4_dir, exist_ok=True)
        with open(os.path.join(stale_mp4_dir, '.meta.json'), 'w', encoding='utf-8') as f:
            json.dump({'name': 'up99', 'base_name': 'recovered_clip', 'database_row_id': row_id}, f)
        with open(os.path.join(stale_mp4_dir, 'seg_000.mp4'), 'wb') as f:
            f.write(b'payload_mp4')

        # 缺失 .meta.json 时应回退使用目录名作为前缀；无效 database_row_id 不应崩溃
        no_meta_dir = os.path.join('.mesio_tmp', '88_999')
        os.makedirs(no_meta_dir, exist_ok=True)
        with open(os.path.join(no_meta_dir, 'seg_000.ts'), 'wb') as f:
            f.write(b'payload_ts')

        missing_row_dir = os.path.join('.mesio_tmp', '77_888')
        os.makedirs(missing_row_dir, exist_ok=True)
        with open(os.path.join(missing_row_dir, '.meta.json'), 'w', encoding='utf-8') as f:
            json.dump({'name': 'ghost', 'base_name': 'ghost_rec', 'database_row_id': 999999}, f)
        with open(os.path.join(missing_row_dir, 'seg_000.mkv'), 'wb') as f:
            f.write(b'payload_mkv')

        with patch('biliup.engine.download.SessionLocal', self._TestSession), \
                patch('biliup.engine.upload.SessionLocal', self._TestSession):
            recovered = download_mod.recover_stale_mesio_tmp()
            self.assertEqual(
                set(recovered),
                {'recovered_stream_p001.flv', 'recovered_clip.mp4', '88_999.ts', 'ghost_rec.mkv'},
            )
            with self._TestSession() as db:
                self.assertEqual(
                    db_mod.get_file_list(db, row_id),
                    ['recovered_stream_p001.flv', 'recovered_clip.mp4'],
                )
            # 验证 UploadBase.file_list('up99') 能精准匹配不含 {streamer} 的恢复文件而不误匹配无关冲突文件
            app_mod.context['PluginInfo'] = SimpleNamespace(url_status={}, inverted_index={})
            config['filtering_threshold'] = 0
            scanned = UploadBase.file_list('up99')
            self.assertEqual(
                {item.video for item in scanned},
                {'recovered_stream_p001.flv', 'recovered_clip.mp4'},
            )

        with open('recovered_stream.flv', 'rb') as f:
            self.assertEqual(f.read(), b'existing')
        with open('recovered_stream_p001.flv', 'rb') as f:
            self.assertEqual(f.read(), b'payload_flv')
        with open('recovered_clip.mp4', 'rb') as f:
            self.assertEqual(f.read(), b'payload_mp4')
        with open('88_999.ts', 'rb') as f:
            self.assertEqual(f.read(), b'payload_ts')
        with open('ghost_rec.mkv', 'rb') as f:
            self.assertEqual(f.read(), b'payload_mkv')
        self.assertFalse(os.path.exists('.mesio_tmp'))

        # 验证 __main__.main 启动时调用 recover_stale_mesio_tmp 且异常不阻断启动
        async def _run_main():
            fake_args = SimpleNamespace(no_http=True)
            fake_detector = MagicMock()

            async def _noop_astart():
                return None

            fake_detector.astart.side_effect = _noop_astart
            with patch.object(app_mod.event_manager, 'start') as mock_em_start, \
                    patch('biliup.engine.download.recover_stale_mesio_tmp', side_effect=RuntimeError('boom')) as mock_rec, \
                    patch.object(main_mod, 'AutoReload', return_value=fake_detector):
                await main_mod.main(fake_args)
                mock_rec.assert_called_once_with()
                mock_em_start.assert_called_once()

        asyncio.run(_run_main())

    def test_soft_watermark_triggers_on_nonempty_active_segment_and_timerange_preserves_flag(self):
        # 1. moved == 0 时：0 字节空文件不误停机，当前分段一旦写入非空字节即触发软水位优雅停机
        config['disk_upload_threshold_gb'] = 10.0
        dl = DummyMesioDownloader(fname='long_seg_up')
        dl.downloader = 'mesio'
        dl.filename_prefix = 'long_seg'

        with self._TestSession() as db:
            dl.database_row_id = db_mod.add_stream_info(db, dl.fname, dl.url, time.localtime())

        state = {'signals_at_zero_bytes': None}
        proc_holder = {}

        def fake_popen(cmd, **kwargs):
            sdir = _extract_session_dir(cmd)
            seg_path = os.path.join(sdir, 'seg_000.flv')
            with open(seg_path, 'wb') as f:
                pass

            def on_poll(proc, count):
                if count == 2:
                    state['signals_at_zero_bytes'] = list(proc.signals)
                    with open(seg_path, 'wb') as f:
                        f.write(b'LIVE' * 128)
                elif count >= 5:
                    proc.stdout.push_eof()
                    return 1
                return None

            def on_signal(proc, sig):
                proc.returncode = 0
                proc.stdout.push_eof()

            proc = FakePopenProcess(cmd, returncode=0, on_poll=on_poll, on_signal=on_signal)
            proc_holder['proc'] = proc
            return proc

        with patch('biliup.engine.download.SessionLocal', self._TestSession), \
                patch('biliup.engine.download.subprocess.Popen', side_effect=fake_popen), \
                patch('biliup.engine.download.shutil.disk_usage', return_value=SimpleNamespace(free=2 * 1024**3)):
            ret = dl.mesio_download('/usr/bin/mesio')

        self._join_segment_threads(dl)
        self.assertTrue(ret)
        self.assertEqual(state['signals_at_zero_bytes'], [])
        self.assertEqual(proc_holder['proc'].returncode, 0)
        self.assertTrue(dl._mesio_soft_triggered)
        self.assertTrue(os.path.isfile('long_seg.flv'))

        # 2. stopped_by_timerange 期间或结束时遇低磁盘，start() 仍返回 disk_soft_triggered=True
        subdir = os.path.join(self._tmpdir.name, 'timerange_soft')
        os.makedirs(subdir, exist_ok=True)
        prev_cwd = os.getcwd()
        os.chdir(subdir)
        try:
            config['disk_upload_threshold_gb'] = 10.0
            dl_tr = DummyMesioDownloader(fname='timerange_up')
            dl_tr.downloader = 'mesio'
            dl_tr.filename_prefix = 'tr_seg'
            dl_tr.time_range = '["00:00:00", "01:00:00"]'

            disk_state = {'free_gb': 20}

            def fake_disk_usage(_path):
                return SimpleNamespace(total=100 * 1024**3, used=80 * 1024**3, free=disk_state['free_gb'] * 1024**3)

            def fake_popen_tr(cmd, **kwargs):
                sdir = _extract_session_dir(cmd)
                with open(os.path.join(sdir, 'seg_000.flv'), 'wb') as f:
                    f.write(b'TR' * 128)

                def on_signal(proc, sig):
                    # 停机排空期间磁盘跌破软水位
                    disk_state['free_gb'] = 1
                    proc.returncode = 0
                    proc.stdout.push_eof()

                return FakePopenProcess(cmd, returncode=0, on_signal=on_signal)

            bg_loop = asyncio.new_event_loop()
            bg_thread = threading.Thread(target=bg_loop.run_forever, daemon=True)
            bg_thread.start()
            try:
                with patch.object(download_mod, 'loop', bg_loop), \
                        patch('biliup.engine.download.SessionLocal', self._TestSession), \
                        patch('biliup.engine.download._resolve_mesio_binary', return_value='/usr/bin/mesio'), \
                        patch('biliup.engine.download.subprocess.Popen', side_effect=fake_popen_tr), \
                        patch('biliup.engine.download.check_timerange', side_effect=[True, False]), \
                        patch('biliup.engine.download.shutil.disk_usage', side_effect=fake_disk_usage):
                    stream_info = dl_tr.start()
            finally:
                bg_loop.call_soon_threadsafe(bg_loop.stop)
                bg_thread.join(timeout=2)
                bg_loop.close()

            self.assertTrue(dl_tr._mesio_soft_triggered)
            self.assertTrue(stream_info['disk_soft_triggered'])
            self.assertTrue(os.path.isfile('tr_seg.flv'))
        finally:
            os.chdir(prev_cwd)

    def test_publish_mesio_file_collision_safe_posix_and_windows_and_concurrent(self):
        with open('clip.flv', 'wb') as f:
            f.write(b'original')

        # 1. POSIX os.link 遇 FileExistsError 自动分配 _p001，绝不覆盖原文件
        src_posix = os.path.join(self._tmpdir.name, 'src_posix.flv')
        with open(src_posix, 'wb') as f:
            f.write(b'posix_new')

        link_calls = []
        unlink_calls = []

        def fake_link(src, dst):
            link_calls.append((src, dst))
            if dst == 'clip.flv':
                raise FileExistsError('exists')
            with open(dst, 'wb') as f:
                with open(src, 'rb') as sf:
                    f.write(sf.read())

        def fake_unlink(path):
            unlink_calls.append(path)
            os.remove(path)

        with patch('biliup.engine.download.os.name', 'posix'), \
                patch('biliup.engine.download.os.link', side_effect=fake_link), \
                patch('biliup.engine.download.os.unlink', side_effect=fake_unlink):
            dst_posix = download_mod._publish_mesio_file(src_posix, 'clip', '.flv', '.')

        self.assertEqual(dst_posix, 'clip_p001.flv')
        self.assertEqual([d for _, d in link_calls], ['clip.flv', 'clip_p001.flv'])
        self.assertEqual(unlink_calls, [src_posix])
        self.assertFalse(os.path.exists(src_posix))
        with open('clip.flv', 'rb') as f:
            self.assertEqual(f.read(), b'original')
        with open('clip_p001.flv', 'rb') as f:
            self.assertEqual(f.read(), b'posix_new')

        # 2. POSIX os.link 返回不支持硬链接的 OSError 时，锁内确认目标不存在后再回退 _rename_with_retry
        src_fb = os.path.join(self._tmpdir.name, 'src_fb.flv')
        with open(src_fb, 'wb') as f:
            f.write(b'posix_fallback')

        def unsupported_link(src, dst):
            raise OSError(errno.EOPNOTSUPP, 'operation not supported')

        with patch('biliup.engine.download.os.name', 'posix'), \
                patch('biliup.engine.download.os.link', side_effect=unsupported_link):
            dst_fb = download_mod._publish_mesio_file(src_fb, 'clip', '.flv', '.')

        self.assertEqual(dst_fb, 'clip_p002.flv')
        with open('clip.flv', 'rb') as f:
            self.assertEqual(f.read(), b'original')
        with open('clip_p002.flv', 'rb') as f:
            self.assertEqual(f.read(), b'posix_fallback')

        # 3. Windows os.rename 遇 FileExistsError 竞态时自动重试下一后缀
        src_nt = os.path.join(self._tmpdir.name, 'src_nt.flv')
        with open(src_nt, 'wb') as f:
            f.write(b'nt_new')
        real_rename = os.rename
        nt_targets = []

        def race_rename(src, dst):
            nt_targets.append(dst)
            if dst == 'clip_p003.flv':
                with open(dst, 'wb') as f:
                    f.write(b'raced_other')
                raise FileExistsError('raced')
            return real_rename(src, dst)

        with patch('biliup.engine.download.os.name', 'nt'), \
                patch('biliup.engine.download.os.rename', side_effect=race_rename):
            dst_nt = download_mod._publish_mesio_file(src_nt, 'clip', '.flv', '.')

        self.assertEqual(nt_targets, ['clip_p003.flv', 'clip_p004.flv'])
        self.assertEqual(dst_nt, 'clip_p004.flv')
        with open('clip_p003.flv', 'rb') as f:
            self.assertEqual(f.read(), b'raced_other')
        with open('clip_p004.flv', 'rb') as f:
            self.assertEqual(f.read(), b'nt_new')

        # 4. 多线程同前缀并发发布绝不互相覆盖
        worker_count = 6
        barrier = threading.Barrier(worker_count)
        results = []
        errors = []
        res_lock = threading.Lock()

        def _worker(idx):
            try:
                src_w = os.path.join(self._tmpdir.name, f'worker_{idx}.flv')
                with open(src_w, 'wb') as f:
                    f.write(f'payload_{idx}'.encode('utf-8'))
                barrier.wait(timeout=5)
                out = download_mod._publish_mesio_file(src_w, 'race_clip', '.flv', '.')
                with res_lock:
                    results.append((idx, out))
            except Exception as exc:
                with res_lock:
                    errors.append(exc)

        threads = [threading.Thread(target=_worker, args=(i,)) for i in range(worker_count)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        self.assertEqual(errors, [])
        self.assertEqual(len(results), worker_count)
        self.assertEqual(len({dst for _, dst in results}), worker_count)
        for idx, dst in results:
            with open(dst, 'rb') as f:
                self.assertEqual(f.read(), f'payload_{idx}'.encode('utf-8'))

        # 5. POSIX 下 os.link 成功但 os.unlink(src) 抛 OSError 时，回滚删除 dst 且保留完整 src 并向上抛出异常
        src_unlink_fail = os.path.join(self._tmpdir.name, 'src_unlink_fail.flv')
        with open(src_unlink_fail, 'wb') as f:
            f.write(b'keep_source')

        unlink_attempts = []

        def real_like_link(src, dst):
            with open(dst, 'wb') as f:
                with open(src, 'rb') as sf:
                    f.write(sf.read())

        def failing_unlink(path):
            unlink_attempts.append(path)
            if path == src_unlink_fail:
                raise OSError(errno.EACCES, 'cannot unlink source')
            os.remove(path)

        with patch('biliup.engine.download.os.name', 'posix'), \
                patch('biliup.engine.download.os.link', side_effect=real_like_link), \
                patch('biliup.engine.download.os.unlink', side_effect=failing_unlink):
            with self.assertRaises(OSError):
                download_mod._publish_mesio_file(src_unlink_fail, 'rollback_clip', '.flv', '.')

        self.assertEqual(unlink_attempts, [src_unlink_fail, 'rollback_clip.flv'])
        self.assertFalse(os.path.exists('rollback_clip.flv'))
        self.assertTrue(os.path.isfile(src_unlink_fail))
        with open(src_unlink_fail, 'rb') as f:
            self.assertEqual(f.read(), b'keep_source')

    def test_recover_skips_live_mesio_pid_and_retries_pending_db_registration(self):
        # 1. mesio_download 成功启动后写入 pid 到 .meta.json
        dl = DummyMesioDownloader(fname='pid_up')
        dl.downloader = 'mesio'
        dl.database_row_id = 55
        recorded_meta = {}

        def fake_popen_pid(cmd, **kwargs):
            sdir = _extract_session_dir(cmd)
            proc = FakePopenProcess(cmd, returncode=1)
            proc.pid = 77889

            def capture_on_poll(p, count):
                with open(os.path.join(sdir, '.meta.json'), 'r', encoding='utf-8') as f:
                    recorded_meta.update(json.load(f))
                p.stdout.push_eof()
                return 1

            proc._on_poll = capture_on_poll
            return proc

        with patch('biliup.engine.download.subprocess.Popen', side_effect=fake_popen_pid):
            dl.mesio_download('/usr/bin/mesio')
        self.assertEqual(recorded_meta.get('pid'), 77889)

        # 2. _is_live_mesio_pid 校验存活 mesio / 僵尸 / 非 mesio 进程 / psutil.Error
        live_proc = MagicMock()
        live_proc.is_running.return_value = True
        live_proc.status.return_value = psutil.STATUS_RUNNING
        live_proc.name.return_value = 'mesio.exe'
        with patch('biliup.engine.download.psutil.Process', return_value=live_proc):
            self.assertTrue(download_mod._is_live_mesio_pid(54321))

        zombie_proc = MagicMock()
        zombie_proc.is_running.return_value = True
        zombie_proc.status.return_value = psutil.STATUS_ZOMBIE
        with patch('biliup.engine.download.psutil.Process', return_value=zombie_proc):
            self.assertFalse(download_mod._is_live_mesio_pid(54321))

        other_proc = MagicMock()
        other_proc.is_running.return_value = True
        other_proc.status.return_value = psutil.STATUS_RUNNING
        other_proc.name.return_value = 'python.exe'
        other_proc.cmdline.return_value = ['python', '-m', 'http.server']
        with patch('biliup.engine.download.psutil.Process', return_value=other_proc):
            self.assertFalse(download_mod._is_live_mesio_pid(54321))

        with patch('biliup.engine.download.psutil.Process', side_effect=psutil.NoSuchProcess(54321)):
            self.assertFalse(download_mod._is_live_mesio_pid(54321))

        # 3. recover_stale_mesio_tmp 跳过存活 mesio PID 会话，且 DB 登记失败后可二次恢复补登记
        with self._TestSession() as db:
            row_id = db_mod.add_stream_info(db, 'retry_up', 'https://live.bilibili.com/777', time.localtime())

        live_dir = os.path.join('.mesio_tmp', '201_live')
        os.makedirs(live_dir, exist_ok=True)
        with open(os.path.join(live_dir, '.meta.json'), 'w', encoding='utf-8') as f:
            json.dump({'name': 'live_up', 'base_name': 'live_rec', 'database_row_id': row_id, 'pid': 54321}, f)
        with open(os.path.join(live_dir, 'seg_000.flv'), 'wb') as f:
            f.write(b'LIVE_ACTIVE')

        retry_dir = os.path.join('.mesio_tmp', '202_retry')
        os.makedirs(retry_dir, exist_ok=True)
        with open(os.path.join(retry_dir, '.meta.json'), 'w', encoding='utf-8') as f:
            json.dump({'name': 'retry_up', 'base_name': 'custom_rec', 'database_row_id': row_id}, f)
        with open(os.path.join(retry_dir, 'seg_000.flv'), 'wb') as f:
            f.write(b'RETRY_DATA')

        # 第 1 次恢复：live_dir 因活动 PID 被跳过；retry_dir 移出文件但 DB 登记抛异常
        with patch('biliup.engine.download.SessionLocal', self._TestSession), \
                patch('biliup.engine.download._is_live_mesio_pid', side_effect=lambda p: p == 54321), \
                patch('biliup.engine.download.update_file_list', side_effect=RuntimeError('database is locked')):
            with self.assertLogs('biliup', level='WARNING') as warn_logs:
                rec1 = download_mod.recover_stale_mesio_tmp()

        self.assertEqual(rec1, ['custom_rec.flv'])
        self.assertTrue(any('54321' in msg for msg in warn_logs.output))
        self.assertTrue(os.path.isfile(os.path.join(live_dir, 'seg_000.flv')))
        self.assertTrue(os.path.isfile(os.path.join(live_dir, '.meta.json')))
        self.assertTrue(os.path.isfile('custom_rec.flv'))
        self.assertTrue(os.path.isfile(os.path.join(retry_dir, '.meta.json')))
        with open(os.path.join(retry_dir, '.meta.json'), 'r', encoding='utf-8') as f:
            retry_meta = json.load(f)
        self.assertEqual(retry_meta.get('pending_db_files'), ['custom_rec.flv'])

        # 清理活动会话以便测试二次恢复清理整个 .mesio_tmp
        shutil.rmtree(live_dir)

        # 第 2 次恢复：DB 恢复正常，补登记 pending_db_files 并彻底清理 retry_dir 与 .mesio_tmp
        with patch('biliup.engine.download.SessionLocal', self._TestSession):
            rec2 = download_mod.recover_stale_mesio_tmp()

        self.assertEqual(rec2, [])
        self.assertFalse(os.path.exists('.mesio_tmp'))
        with self._TestSession() as db:
            self.assertEqual(db_mod.get_file_list(db, row_id), ['custom_rec.flv'])

    def test_upload_file_list_discovers_older_streamer_info_recovery_files(self):
        with self._TestSession() as db:
            old_row_id = db_mod.add_stream_info(db, 'hist_up', 'https://live.bilibili.com/555', time.localtime())
            db_mod.update_file_list(db, old_row_id, 'custom_old_session.flv')
            # 主播重启后开始新录制，写入更新的 StreamerInfo 记录
            _new_row_id = db_mod.add_stream_info(db, 'hist_up', 'https://live.bilibili.com/555', time.localtime())

        with open('custom_old_session.flv', 'wb') as f:
            f.write(b'OLD_RECOVERED')
        with open('unrelated_other.flv', 'wb') as f:
            f.write(b'OTHER')

        app_mod.context['PluginInfo'] = SimpleNamespace(url_status={}, inverted_index={})
        config['filtering_threshold'] = 0
        with patch('biliup.engine.upload.SessionLocal', self._TestSession):
            scanned = UploadBase.file_list('hist_up')

        self.assertEqual([item.video for item in scanned], ['custom_old_session.flv'])


if __name__ == '__main__':
    unittest.main()
