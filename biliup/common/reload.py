import asyncio
import logging
import subprocess
import sys
import os
from .timer import Timer

logger = logging.getLogger('biliup')

global global_reloader


def has_extension(fname_list, *extension):
    for fname in fname_list:
        result = list(map(fname.endswith, extension))
        if True in result:
            return True
    return False


class AutoReload(Timer):
    def __init__(self, *watched, interval=10):
        super().__init__(interval)
        self.watched = watched
        self.mtimes = {}
        self.triggered = False

    @staticmethod
    def _iter_module_files():
        """Iterator to module's source filename of sys.modules (built-in
        excluded).
        """
        for module in list(sys.modules.values()):
            filename = getattr(module, '__file__', None)
            if filename:
                if filename[-4:] in ('.pyo', '.pyc'):
                    filename = filename[:-1]
                yield filename

    def _is_any_file_changed(self):
        """Return 1 if there is any source file of sys.modules changed,
        otherwise 0. mtimes is dict to store the last modify time for
        comparing."""
        for filename in self._iter_module_files():
            try:
                mtime = os.stat(filename).st_mtime
            except IOError:
                continue
            old_time = self.mtimes.get(filename, None)
            if old_time is None:
                self.mtimes[filename] = mtime
            elif mtime > old_time:
                logger.info('模块已更新')
                return True
        return False

    @staticmethod
    def _work_free():
        fname_list = os.listdir('.')
        if has_extension(fname_list, '.mp4', '.flv', '.3gp', '.webm', '.mkv', '.ts', '.part'):
            return False
        try:
            from biliup.app import context, pending_download_urls
            from biliup.common.tools import NamedLock
            with NamedLock('download_dispatch'):
                plugin_info = context.get('PluginInfo')
                if plugin_info is not None and any(status == 1 for status in plugin_info.url_status.values()):
                    return False
                if pending_download_urls:
                    return False
                if any(count > 0 for count in context.get('url_upload_count', {}).values()):
                    return False
            with NamedLock('upload_filename'):
                if context.get('upload_filename'):
                    return False
        except Exception:
            logger.exception('检查运行态任务空闲状态失败')
            return False
        logger.info('进程空闲')
        return True

    async def atimer(self):
        """Check file state ervry interval. If any change is detected, exit this
        process with a special code, so that deamon will to restart a new process.
        """
        if not self._is_any_file_changed() and not self.triggered:
            return
        while True:
            await asyncio.sleep(self.interval)
            if self._work_free():
                for watched in self.watched:
                    if callable(watched):
                        if asyncio.iscoroutinefunction(watched):
                            await watched()
                        else:
                            watched()
                    else:
                        watched.stop()
                self.stop()
                logger.info('重启')
                if not is_docker():
                    _restart_process()
                return


_VALUE_FLAGS = {'-H', '-P', '--static-dir', '--password', '--config'}


def _normalize_restart_args(argv_tail):
    """将子命令位置的 'restart' 转为 'start'，避免 execv 后 Daemon.restart() 杀死自身；跳过带值选项参数。"""
    normalized = []
    has_start_subcmd = False
    skip_next = False
    for arg in argv_tail:
        if skip_next:
            normalized.append(arg)
            skip_next = False
        elif arg in _VALUE_FLAGS:
            normalized.append(arg)
            skip_next = True
        elif arg == 'restart':
            normalized.append('start')
            has_start_subcmd = True
        else:
            if arg == 'start':
                has_start_subcmd = True
            normalized.append(arg)
    return normalized, has_start_subcmd


def _build_restart_cmd():
    from biliup import IS_FROZEN
    args, _ = _normalize_restart_args(sys.argv[1:])
    if IS_FROZEN:
        return [sys.executable, *args]
    return [sys.executable, '-m', 'biliup', *args]


def _cleanup_own_pidfile(pidfile='watch_process.pid'):
    if not os.path.isfile(pidfile):
        return
    try:
        with open(pidfile, 'r', encoding='utf-8') as f:
            pid_text = f.read().strip()
        if pid_text == str(os.getpid()):
            os.remove(pidfile)
    except OSError:
        logger.warning(f'清理 PID 文件 {pidfile} 失败', exc_info=True)


def _restart_process():
    web_mod = sys.modules.get('biliup.web')
    if web_mod is not None and hasattr(web_mod, '_shutdown_qr_pool'):
        try:
            web_mod._shutdown_qr_pool()
        except Exception:
            logger.warning('关闭扫码进程池失败', exc_info=True)

    _, has_start_subcmd = _normalize_restart_args(sys.argv[1:])
    cmd = _build_restart_cmd()
    if sys.platform != 'win32':
        # 仅在重启命令包含 'start' 子命令（含由 'restart' 规范化而来，Daemon.start 会重建 PID 文件）时清理旧 PID；
        # 否则 os.execv 保持原 PID，需保留现有 PID 文件。
        if has_start_subcmd:
            _cleanup_own_pidfile()
        os.execv(cmd[0], cmd)
    else:
        _cleanup_own_pidfile()
        subprocess.Popen(cmd)


def is_docker():
    if os.path.exists('/.dockerenv'):
        return True
    path = '/proc/self/cgroup'
    if not os.path.isfile(path):
        return False
    try:
        with open(path, encoding='utf-8', errors='ignore') as f:
            return any('docker' in line for line in f)
    except OSError:
        return False
