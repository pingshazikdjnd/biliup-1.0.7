import logging
import os
import platform
import subprocess
import sys
import threading
from typing import Optional

logger = logging.getLogger('biliup')


class NamedLock:
    """
    简单实现的命名锁
    """
    from _thread import LockType
    _meta_lock = threading.Lock()
    _lock_dict = {}

    def __new__(cls, name) -> LockType:
        with cls._meta_lock:
            lock = cls._lock_dict.get(name)
            if lock is None:
                lock = threading.Lock()
                cls._lock_dict[name] = lock
            return lock


def silence_event_loop_closed(func):
    from functools import wraps

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except RuntimeError as e:
            if str(e) != 'Event loop is closed':
                raise

    return wrapper


def get_file_birth_timestamp(file: str) -> Optional[float]:
    """
    跨平台获取文件真实创建时间（birth time）
    如平台或文件系统不支持则返回 None
    """
    stat_result = os.stat(file)

    if hasattr(stat_result, "st_birthtime"):
        return stat_result.st_birthtime

    if platform.system() == 'Windows' and sys.version_info < (3, 12):
        return stat_result.st_ctime

    if platform.system() == 'Linux':
        try:
            birth_time = float(subprocess.check_output(["stat", "-c", "%W", file]).decode('utf8'))
            if birth_time > 0:
                return birth_time
        except Exception:
            pass

    return None


def get_file_create_timestamp(file: str) -> float:
    """
    跨平台获取文件创建时间
    如无法获取则返回修改时间
    """
    birth_ts = get_file_birth_timestamp(file)
    if birth_ts is not None:
        return birth_ts
    return os.stat(file).st_mtime


def processor(processors, data):
    for process in processors:
        if process.get('run'):
            try:
                process_output = subprocess.check_output(
                    process['run'], shell=True,
                    input=data,
                    stderr=subprocess.STDOUT, text=True)
                logger.info(process_output.rstrip())
            except subprocess.CalledProcessError as e:
                logger.exception(e.output)
                continue
