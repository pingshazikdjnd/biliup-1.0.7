import asyncio
import collections
import errno
import inspect
import itertools
import json
import logging
import os
import queue
import re
import signal
import subprocess
import threading
import time
import shutil
import copy
from abc import ABC, abstractmethod
from typing import AsyncGenerator, List, Callable, Optional
from urllib.parse import urlparse
from datetime import datetime, timezone

import psutil
import requests
from requests.utils import DEFAULT_ACCEPT_ENCODING, parse_header_links
from httpx import HTTPStatusError

from biliup.common.tools import NamedLock
from biliup.common.util import client, loop, check_timerange
from biliup.database.db import add_stream_info, SessionLocal, get_stream_info, update_cover_path, update_room_title, update_file_list
from biliup.plugins import random_user_agent
import stream_gears
from PIL import Image

from biliup.config import config
from biliup.Danmaku import IDanmakuClient
from biliup.plugins.bili_webup_sync import BiliWebAsync
from biliup.uploader import fmt_title_and_desc

from .sync_downloader import SyncDownloader
from biliup.app import context
logger = logging.getLogger('biliup')

_MESIO_MEDIA_EXTENSIONS = ('.flv', '.ts', '.mp4', '.mkv', '.webm')
_MESIO_CLOSED_SEGMENT_RE = re.compile(r'Closed segment path=(.+?)(?:\s+tags=|\s+index=|$)')
_MESIO_SEG_INDEX_RE = re.compile(r'seg_(\d+)')


def _parse_duration_to_seconds(duration_str: Optional[str]) -> Optional[int]:
    if not duration_str or not isinstance(duration_str, str):
        return None
    parts = duration_str.strip().split(':')
    if len(parts) != 3 or any(not (part.isascii() and part.isdigit()) for part in parts):
        return None
    hours, minutes, seconds = int(parts[0]), int(parts[1]), int(parts[2])
    if minutes >= 60 or seconds >= 60:
        return None
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds if total_seconds > 0 else None


def _resolve_mesio_binary(mesio_path: Optional[str]) -> Optional[str]:
    if isinstance(mesio_path, str):
        candidate = mesio_path.strip()
        if candidate:
            has_sep = os.sep in candidate or (os.altsep is not None and os.altsep in candidate)
            if has_sep:
                if not os.path.isfile(candidate):
                    return None
                if os.name != 'nt' and not os.access(candidate, os.X_OK):
                    logger.error(f"mesio 文件不可执行，请检查权限（如执行 chmod +x）: {candidate}")
                    return None
                return candidate
            return shutil.which(candidate)
    return shutil.which('mesio')


_HARDLINK_UNSUPPORTED_ERRNOS = {
    errno.EPERM,
    errno.EOPNOTSUPP,
    errno.EXDEV,
    errno.EMLINK,
}
if hasattr(errno, 'ENOTSUP'):
    _HARDLINK_UNSUPPORTED_ERRNOS.add(errno.ENOTSUP)


def _format_media_target(base_name: str, ext: str, target_dir: str, idx: int) -> str:
    use_cwd = not target_dir or target_dir == '.'
    candidate = f'{base_name}{ext}' if idx == 0 else f'{base_name}_p{idx:03d}{ext}'
    return candidate if use_cwd else os.path.join(target_dir, candidate)


def _allocate_unique_media_target(base_name: str, ext: str, target_dir: str = '.') -> str:
    if ext and not ext.startswith('.'):
        ext = f'.{ext}'
    for idx in itertools.count(0):
        target_path = _format_media_target(base_name, ext, target_dir, idx)
        if not (os.path.exists(target_path) or os.path.islink(target_path)):
            return target_path
    return _format_media_target(base_name, ext, target_dir, 0)


def _publish_mesio_file(src_path: str, base_name: str, ext: str, target_dir: str = '.') -> str:
    if ext and not ext.startswith('.'):
        ext = f'.{ext}'
    with NamedLock('mesio_publish_target'):
        for idx in itertools.count(0):
            dst = _format_media_target(base_name, ext, target_dir, idx)
            if os.name == 'nt':
                if os.path.exists(dst) or os.path.islink(dst):
                    continue
                try:
                    _rename_with_retry(src_path, dst)
                    return dst
                except FileExistsError:
                    continue
            else:
                try:
                    os.link(src_path, dst)
                except FileExistsError:
                    continue
                except OSError as e:
                    if e.errno not in _HARDLINK_UNSUPPORTED_ERRNOS:
                        raise
                    if os.path.exists(dst) or os.path.islink(dst):
                        continue
                    try:
                        _rename_with_retry(src_path, dst)
                        return dst
                    except FileExistsError:
                        continue
                os.unlink(src_path)
                return dst
    return dst


def _write_mesio_meta(meta_path: str, meta: dict) -> None:
    meta_dir = os.path.dirname(meta_path) or '.'
    tmp_meta_path = os.path.join(meta_dir, f'.meta.json.tmp.{os.getpid()}.{threading.get_ident()}')
    try:
        with open(tmp_meta_path, 'w', encoding='utf-8') as f:
            json.dump(meta, f, ensure_ascii=False)
        os.replace(tmp_meta_path, meta_path)
    except Exception:
        try:
            if os.path.exists(tmp_meta_path):
                os.remove(tmp_meta_path)
        except OSError:
            pass
        raise


def _is_live_mesio_pid(pid: int) -> bool:
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return False
    try:
        proc = psutil.Process(pid)
        if not proc.is_running() or proc.status() == psutil.STATUS_ZOMBIE:
            return False
        proc_name = (proc.name() or '').lower()
        if 'mesio' in proc_name:
            return True
        cmdline = proc.cmdline() or []
        return any('mesio' in str(arg).lower() for arg in cmdline)
    except psutil.Error:
        return False


def _normalize_mesio_path(path_str: str) -> str:
    return os.path.normcase(os.path.abspath(os.path.normpath(path_str)))


def _rename_with_retry(src: str, dst: str, retries: int = 3, delay: float = 0.1) -> None:
    for attempt in range(retries + 1):
        try:
            os.rename(src, dst)
            return
        except PermissionError:
            if attempt >= retries:
                raise
            time.sleep(delay)


def _list_sorted_mesio_media(session_dir: str):
    entries = []
    if not os.path.isdir(session_dir):
        return entries
    for name in os.listdir(session_dir):
        src_path = os.path.join(session_dir, name)
        if not os.path.isfile(src_path):
            continue
        _, ext = os.path.splitext(name)
        if ext.lower() not in _MESIO_MEDIA_EXTENSIONS:
            continue
        try:
            st = os.stat(src_path)
        except FileNotFoundError:
            continue
        match = _MESIO_SEG_INDEX_RE.search(name)
        seg_index = int(match.group(1)) if match else float('inf')
        entries.append(((seg_index, st.st_mtime, name), src_path, name, ext, st.st_size))
    entries.sort(key=lambda item: item[0])
    return [(src_path, name, ext, size) for _, src_path, name, ext, size in entries]


def _try_register_mesio_file(database_row_id: Optional[int], file_basename: str, log_target: str) -> bool:
    if database_row_id is None:
        return True
    try:
        from biliup.database.models import StreamerInfo
        with SessionLocal() as db:
            if db.get(StreamerInfo, database_row_id) is not None:
                update_file_list(db, database_row_id, file_basename)
        return True
    except Exception as e:
        logger.warning(f"登记恢复的 mesio 录像到数据库失败 ({log_target}): {e}")
        return False


def recover_stale_mesio_tmp(tmp_root: str = '.mesio_tmp', target_dir: str = '.') -> List[str]:
    recovered: List[str] = []
    if not os.path.isdir(tmp_root):
        return recovered
    if target_dir and target_dir != '.':
        os.makedirs(target_dir, exist_ok=True)
    use_cwd = not target_dir or target_dir == '.'

    for entry in sorted(os.listdir(tmp_root)):
        session_dir = os.path.join(tmp_root, entry)
        if not os.path.isdir(session_dir):
            continue
        meta_path = os.path.join(session_dir, '.meta.json')
        base_name = entry
        database_row_id = None
        meta: Optional[dict] = None
        live_pid = None
        orig_pending_db_files: List[str] = []
        if os.path.isfile(meta_path):
            try:
                with open(meta_path, 'r', encoding='utf-8') as f:
                    loaded_meta = json.load(f)
                if isinstance(loaded_meta, dict):
                    meta = loaded_meta
                    meta_base = meta.get('base_name')
                    if isinstance(meta_base, str) and meta_base.strip():
                        base_name = os.path.basename(meta_base.strip()) or entry
                    raw_row_id = meta.get('database_row_id')
                    if not isinstance(raw_row_id, bool):
                        try:
                            parsed_row_id = int(raw_row_id)
                            if parsed_row_id > 0:
                                database_row_id = parsed_row_id
                        except (TypeError, ValueError):
                            pass
                    raw_pid = meta.get('pid')
                    if isinstance(raw_pid, int) and not isinstance(raw_pid, bool) and raw_pid > 0:
                        live_pid = raw_pid
                    raw_pending = meta.get('pending_db_files')
                    if isinstance(raw_pending, list):
                        for item in raw_pending:
                            if isinstance(item, str) and item.strip():
                                fname = os.path.basename(item.strip())
                                if fname and fname not in orig_pending_db_files:
                                    orig_pending_db_files.append(fname)
            except Exception as e:
                logger.warning(f"读取残留 mesio 会话元数据失败 ({meta_path}): {e}")

        if live_pid is not None and _is_live_mesio_pid(live_pid):
            logger.warning(f"检测到活动 mesio 进程 (pid={live_pid}) 仍在写入，跳过恢复会话目录: {session_dir}")
            continue

        reg_failed = False
        pending_db_files: List[str] = []
        for fname in orig_pending_db_files:
            pending_target = fname if use_cwd else os.path.join(target_dir, fname)
            if not os.path.isfile(pending_target):
                continue
            if not _try_register_mesio_file(database_row_id, fname, pending_target):
                reg_failed = True
                if fname not in pending_db_files:
                    pending_db_files.append(fname)

        move_failed = False
        for src_path, _name, ext, size in _list_sorted_mesio_media(session_dir):
            if size == 0:
                try:
                    os.remove(src_path)
                except OSError as e:
                    logger.warning(f"删除残留空媒体文件失败 ({src_path}): {e}")
                continue
            try:
                dst = _publish_mesio_file(src_path, base_name, ext, target_dir)
                recovered.append(dst)
                logger.info(f"恢复残留 mesio 录像: {src_path} -> {dst}")
            except OSError as e:
                move_failed = True
                logger.error(f"恢复残留 mesio 录像失败 ({src_path}): {e}")
                continue
            dst_name = os.path.basename(dst)
            if not _try_register_mesio_file(database_row_id, dst_name, dst):
                reg_failed = True
                if dst_name not in pending_db_files:
                    pending_db_files.append(dst_name)

        for name in os.listdir(session_dir):
            if name == '.meta.json':
                continue
            other_path = os.path.join(session_dir, name)
            if os.path.isfile(other_path):
                try:
                    if os.path.getsize(other_path) == 0:
                        os.remove(other_path)
                except OSError:
                    pass

        if reg_failed or pending_db_files:
            updated_meta = dict(meta) if isinstance(meta, dict) else {'base_name': base_name}
            if database_row_id is not None and 'database_row_id' not in updated_meta:
                updated_meta['database_row_id'] = database_row_id
            updated_meta['pending_db_files'] = pending_db_files
            try:
                _write_mesio_meta(meta_path, updated_meta)
            except OSError as e:
                logger.warning(f"更新残留 mesio 元数据失败 ({meta_path}): {e}")
        elif move_failed and orig_pending_db_files and isinstance(meta, dict) and os.path.isfile(meta_path):
            updated_meta = dict(meta)
            updated_meta.pop('pending_db_files', None)
            try:
                _write_mesio_meta(meta_path, updated_meta)
            except OSError as e:
                logger.warning(f"更新残留 mesio 元数据失败 ({meta_path}): {e}")
        elif not move_failed and not reg_failed and not pending_db_files and os.path.isfile(meta_path):
            try:
                os.remove(meta_path)
            except OSError as e:
                logger.warning(f"删除残留 mesio 元数据失败 ({meta_path}): {e}")

        try:
            os.rmdir(session_dir)
        except OSError:
            pass

    try:
        os.rmdir(tmp_root)
    except OSError:
        pass

    return recovered


def _get_low_disk_soft_info(plugin_msg: str) -> Optional[tuple]:
    try:
        threshold_gb = float(config.get('disk_upload_threshold_gb', 0) or 0)
    except (ValueError, TypeError):
        threshold_gb = 0.0
    if threshold_gb <= 0:
        return None
    try:
        free_bytes = shutil.disk_usage('.').free
    except OSError as e:
        logger.warning(f"{plugin_msg}: 检测磁盘剩余空间失败，跳过本次软水位检查: {e}")
        return None
    threshold_bytes = threshold_gb * 1024 * 1024 * 1024
    if free_bytes < threshold_bytes:
        return free_bytes / (1024 ** 3), threshold_gb
    return None


class DownloadBase(ABC):
    def __init__(self, fname, url, suffix=None, opt_args=None):
        self.room_title = None
        if opt_args is None:
            opt_args = []
        self.fname = fname
        self.url = url
        # 录制后保存文件格式而非源流格式 对应原配置文件format 仅ffmpeg及streamlink生效
        if not suffix:
            logger.error(f'检测到suffix不存在，请补充后缀')
        else:
            self.suffix = suffix.lower()
        self.live_cover_path = None
        self.database_row_id = 0
        self.downloader = config.get('downloader', 'stream-gears')
        self.mesio_path = config.get('mesio_path')
        self._mesio_soft_triggered = False
        # ffmpeg.exe -i  http://vfile1.grtn.cn/2018/1542/0254/3368/154202543368.ssm/154202543368.m3u8
        # -c copy -bsf:a aac_adtstoasc -movflags +faststart output.mp4
        self.raw_stream_url = None

        # 主播单独传参会覆盖全局设置。例如新增了一个全局的filename_prefix参数，在下面添加self.filename_prefix = config.get('filename_prefix'),
        # 即可通过self.filename_prefix在下载或者上传时候传递主播单独的设置参数用于调用（如果该主播有设置单独参数，将会优先使用单独参数；如无，则会优先你用全局参数。）
        self.filename_prefix = config.get('filename_prefix')
        self.use_live_cover = config.get('use_live_cover', False)
        self.opt_args = opt_args
        self.live_cover_url = None
        self.fake_headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'accept-encoding': DEFAULT_ACCEPT_ENCODING,
            'accept-language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
            'user-agent': random_user_agent(),
        }
        self.stream_headers = copy.deepcopy(self.fake_headers)
        self.segment_time = config.get('segment_time', '01:00:00')
        self.time_range = config.get('time_range')
        self.excluded_keywords = config.get('excluded_keywords')
        self.file_size = config.get('file_size')

        # 是否是下载模式 跳过下播检测
        self.is_download = False

        # 分段后处理
        self.segment_processor = config.get('segment_processor')
        self.segment_processor_thread = []
        # 分段后处理并行
        self.segment_processor_parallel = config.get('segment_processor_parallel', False)

        # 弹幕客户端
        self.danmaku: Optional[IDanmakuClient] = None

        self.platform = self.__class__.__name__
        self.plugin_msg = f"[{self.platform}]{self.fname} - {self.url}"

    @abstractmethod
    async def acheck_stream(self, is_check=False):
        # is_check 是否是检测模式 检测模式可以忽略只有下载时需要的耗时操作
        raise NotImplementedError()

    def should_record(self):
        # 检查磁盘硬水位
        try:
            disk_stop_threshold_gb = float(config.get('disk_stop_threshold_gb', 0) or 0)
        except (ValueError, TypeError):
            disk_stop_threshold_gb = 0.0

        if disk_stop_threshold_gb > 0:
            try:
                free_bytes = shutil.disk_usage('.').free
            except OSError as e:
                logger.warning(f"{self.plugin_msg}: 检测磁盘剩余空间失败，跳过本次硬水位检查: {e}")
            else:
                threshold_bytes = disk_stop_threshold_gb * 1024 * 1024 * 1024
                if free_bytes < threshold_bytes:
                    free_gb = free_bytes / (1024 ** 3)
                    logger.error(
                        f"{self.plugin_msg}: 磁盘剩余空间 ({free_gb:.2f} GB) 低于硬水位阈值 ({disk_stop_threshold_gb} GB)，"
                        f"暂停新开录制以保护系统文件"
                    )
                    from biliup.common.notify import send_notification
                    send_notification(
                        title=f"biliup 磁盘硬水位告警: {self.fname}",
                        content=f"主播: {self.fname}\nURL: {self.url}\n磁盘剩余空间 ({free_gb:.2f} GB) 低于硬水位阈值 ({disk_stop_threshold_gb} GB)，已暂停新开录制。",
                        streamer_name=self.fname,
                        error_type="disk_stop_threshold_reached"
                    )
                    return False

        # 检查房间名
        keywords = config['streamers'].get(self.fname, {}).get('excluded_keywords')
        if self.room_title and keywords:
            if any(k.strip() in self.room_title for k in keywords):
                return False

        # 检查时间范围
        if not check_timerange(self.fname):
            return False

        return True

    def download(self):
        self.update_headers(self.stream_headers)
        # print(f"{self.plugin_msg}: Plugin settings - {self.__dict__}")
        logger.debug(f"{self.plugin_msg}: Plugin settings - {self.__dict__}")
        # logger.info(f"{self.plugin_msg}: Request headers - {self.fake_headers}")
        logger.info(f"{self.plugin_msg}: Request url - {self.raw_stream_url}")
        # 调试使用边录边上传功能
        # self.downloader = 'sync-downloader'
        if self.is_download:
            if not shutil.which("ffmpeg"):
                logger.error("未安装 FFMpeg 或不存在于 PATH 内")
                logger.info("Current user's PATH is:" + os.getenv("PATH"))
                return False
            else:
                return self.ffmpeg_segment_download()

        parsed_url_path = urlparse(self.raw_stream_url).path
        if self.downloader == 'mesio':
            mesio_bin = _resolve_mesio_binary(getattr(self, 'mesio_path', None))
            if mesio_bin:
                return self.mesio_download(mesio_bin)
            logger.error("未安装 mesio 或不存在于 PATH / mesio_path 内，本次下载尝试回退到 ffmpeg")

        if self.downloader != 'stream-gears':
            if not shutil.which("ffmpeg"):
                logger.error("未安装 FFMpeg 或不存在于 PATH 内，本次下载使用 stream-gears")
                logger.info("Current user's PATH is:" + os.getenv("PATH"))
            else:
                # 同步下载上传器
                if self.downloader == 'sync-downloader':
                    logger.info(f"{self.plugin_msg}: 使用同步下载器")
                    stream_info = config.get('streamers', {}).get(self.fname, {})
                    stream_info.update({'name': self.fname})
                    min_size = 10 * 1024 * 1024
                    if not self.file_size:
                        self.file_size = 2 * 1024 * 1024 * 1024
                    self.file_size = ((self.file_size + min_size - 1) // min_size) * min_size  # 向上取整
                    sync_download(self.raw_stream_url, self.stream_headers,
                                max_file_size=int(self.file_size / 1024 / 1024),
                                output_prefix=self.gen_download_filename(True),
                                stream_info=stream_info,
                                file_name_callback=lambda file_name: self.__download_segment_callback(file_name), database_row_id=self.database_row_id)
                    return True
                # streamlink无法处理flv,所以回退到ffmpeg
                if self.downloader == 'streamlink' and '.flv' not in parsed_url_path:
                    return self.ffmpeg_download(use_streamlink=True)
                return self.ffmpeg_download()

        if '.flv' in parsed_url_path:
            # 假定flv流
            self.suffix = 'flv'
        else:
            # 其他流stream_gears会按hls保存为ts
            self.suffix = 'ts'
        stream_gears_download(self.raw_stream_url, self.stream_headers, self.gen_download_filename(),
                              self.segment_time,
                              self.file_size,
                              lambda file_name: self.__download_segment_callback(file_name))
        return True

    def ffmpeg_segment_download(self):
        # TODO 无日志
        # , '-report'
        # ffmpeg 输入参数
        input_args = [
            '-loglevel', 'quiet', '-y'
        ]
        # ffmpeg 输出参数
        output_args = [
            '-bsf:a', 'aac_adtstoasc'
        ]
        input_args += ['-headers', ''.join('%s: %s\r\n' % x for x in self.fake_headers.items()),
                       '-rw_timeout', '20000000']
        if '.m3u8' in urlparse(self.raw_stream_url).path:
            input_args += ['-max_reload', '1000']

        input_args += ['-i', self.raw_stream_url]

        output_args += ['-f', 'segment']
        # output_args += ['-segment_format', self.suffix]
        output_args += ['-segment_list', 'pipe:1']
        output_args += ['-segment_list_type', 'flat']
        output_args += ['-reset_timestamps', '1']
        # output_args += ['-strftime', '1']
        if self.segment_time:
            output_args += ['-segment_time', self.segment_time]
        else:
            # 避免适配两套
            output_args += ['-segment_time', '9999:00:00']

        output_args += ['-c', 'copy']
        output_args += self.opt_args
        file_name = self.gen_download_filename(is_fmt=True)
        args = ['ffmpeg', *input_args, *output_args, f'{file_name}_%d.{self.suffix}']
        with subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                              stderr=subprocess.DEVNULL) as proc:
            for line in iter(proc.stdout.readline, b''):  # b'\n'-separated lines
                try:
                    ffmpeg_file_name = line.rstrip().decode(errors='ignore')
                    time.sleep(1)
                    # 文件重命名
                    self.download_file_rename(ffmpeg_file_name, f'{file_name}.{self.suffix}')
                    self.__download_segment_callback(f'{file_name}.{self.suffix}')
                    file_name = self.gen_download_filename(is_fmt=True)
                except:
                    logger.error(f'分段事件失败：{self.__class__.__name__} - {self.fname}', exc_info=True)

        return proc.returncode == 0

    def ffmpeg_download(self, use_streamlink=False):
        # streamlink进程
        streamlink_proc = None
        # updatedFileList = False
        try:
            # 文件名不含后戳
            fmt_file_name = self.gen_download_filename(is_fmt=True)
            # ffmpeg 输入参数
            input_args = [
                # '-http_proxy', 'http://127.0.0.1:10808',
                # "-fflags", "+genpts",
            ]
            # ffmpeg 输出参数
            output_args = [
                '-c',
                'copy',
            ]
            # https://github.com/biliup/biliup/issues/991
            if use_streamlink and not self.raw_stream_url.startswith('http://localhost:'):
                streamlink_cmd = [
                    'streamlink',
                    '--stream-segment-threads', '3',
                    '--hls-playlist-reload-attempts', '1',
                    # '--http-proxy', 'http://127.0.0.1:7890',
                    # '--hls-live-restart',
                ]
                for key, value in self.stream_headers.items():
                    streamlink_cmd.extend(['--http-header', f'{key}={value}'])
                if self.__class__.__name__ == 'Bililive':
                    # Fix: segment 不携带参数时 404
                    from urllib.parse import parse_qs
                    def parse_query_params(url: str) -> List[str]:
                        query_params = []
                        params = url.split('?')[-1]
                        whitelist = ['uparams', 'upsig', 'sigparams', 'sign', 'flvsk', 'sk', 'mid', 'site']
                        data = parse_qs(params)
                        whitelist.extend(data.get('sigparams', [''])[0].split(','))
                        whitelist.extend(data.get('uparams', [''])[0].split(','))
                        if not params or params.startswith('http'):
                            return query_params
                        for param in params.split('&'):
                            if param.split('=')[0] not in whitelist:
                                continue
                            query_params.extend(["--http-query-param", param])
                        return query_params
                    streamlink_cmd.extend(parse_query_params(self.raw_stream_url))
                streamlink_cmd.extend([
                    self.raw_stream_url,
                    'best',
                    '-O'
                ])
                streamlink_proc = subprocess.Popen(streamlink_cmd, stdout=subprocess.PIPE)
                input_uri = 'pipe:0'
            else:
                input_args += ['-headers', ''.join('%s: %s\r\n' % x for x in self.stream_headers.items()),
                               '-rw_timeout', '20000000']
                if '.m3u8' in urlparse(self.raw_stream_url).path:
                    input_args += ['-max_reload', '1000']
                input_uri = self.raw_stream_url

            input_args += ['-i', input_uri]

            duration = get_duration(self.segment_time, self.time_range)
            if duration:
                output_args += ['-to', duration]
            if self.file_size:
                output_args += ['-fs', str(self.file_size)]

            output_args += self.opt_args

            if self.suffix == 'mp4':
                output_args += ['-bsf:a', 'aac_adtstoasc', '-f', 'mp4']
            elif self.suffix == 'ts':
                output_args += ['-f', 'mpegts']
            elif self.suffix == 'mkv':
                output_args += ['-f', 'matroska']
            else:
                output_args += ['-f', self.suffix]

            args = ['ffmpeg', '-y', *input_args, *output_args,
                    f'{fmt_file_name}.{self.suffix}.part']
            with subprocess.Popen(args, stdin=subprocess.DEVNULL if not streamlink_proc else streamlink_proc.stdout,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as proc:
                for line in iter(proc.stdout.readline, b''):  # b'\n'-separated lines
                    decode_line = line.rstrip().decode(errors='ignore')
                    print(decode_line)
                    logger.debug(decode_line)

            if proc.returncode == 0:
                # 文件重命名
                self.download_file_rename(f'{fmt_file_name}.{self.suffix}.part', f'{fmt_file_name}.{self.suffix}')
                # 触发分段事件
                self.__download_segment_callback(f'{fmt_file_name}.{self.suffix}')
                return True
            else:
                return False
        finally:
            try:
                if streamlink_proc:
                    streamlink_proc.terminate()
                    streamlink_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                streamlink_proc.kill()
            except:
                logger.exception(f'terminate {self.fname} failed')

    def mesio_download(self, mesio_bin: str) -> bool:
        self._mesio_soft_triggered = False
        session_tmp_dir = os.path.join(
            '.mesio_tmp',
            f'{self.database_row_id}_{int(time.time() * 1000)}_{os.getpid()}',
        )
        os.makedirs(session_tmp_dir, exist_ok=True)

        def _render_base_name(fallback: str) -> str:
            try:
                tmpl = self.gen_download_filename(is_fmt=False)
                rendered = time.strftime(
                    tmpl.encode("unicode-escape").decode(),
                    time.localtime(),
                ).encode().decode("unicode-escape")
                return rendered or fallback
            except Exception:
                return fallback

        safe_fallback = re.sub(r'[\\/:*?"<>|%\s]+', '_', str(self.fname or 'mesio')).strip('_') or 'mesio'
        session_base_name = _render_base_name(f'{safe_fallback}_{int(time.time())}')
        meta_path = os.path.join(session_tmp_dir, '.meta.json')
        session_meta = {
            'name': self.fname,
            'base_name': session_base_name,
            'database_row_id': self.database_row_id,
        }
        try:
            _write_mesio_meta(meta_path, session_meta)
        except OSError as e:
            logger.warning(f"{self.plugin_msg}: 写入 mesio 会话元数据失败: {e}")

        cmd = [
            mesio_bin,
            '--fix',
            '--disable-log-file',
            '-o', session_tmp_dir,
            '-n', 'seg_%i',
            '--connect-timeout', '20',
            '--read-timeout', '30',
        ]
        if isinstance(self.stream_headers, dict):
            for key, value in self.stream_headers.items():
                if key is None or value is None:
                    continue
                clean_key = str(key).replace('\r', '').replace('\n', '').strip()
                if clean_key.lower() == 'accept-encoding':
                    continue
                clean_val = str(value).replace('\r', '').replace('\n', '').strip()
                if clean_key and clean_val:
                    cmd.extend(['-H', f'{clean_key}: {clean_val}'])

        duration_str = get_duration(self.segment_time, self.time_range)
        total_sec = _parse_duration_to_seconds(duration_str)
        if total_sec is not None and total_sec > 0:
            cmd.extend(['-d', f'{total_sec}s'])

        if self.file_size is not None and self.file_size != '':
            try:
                file_size_int = int(self.file_size)
            except (ValueError, TypeError):
                file_size_int = 0
            if file_size_int > 0:
                cmd.extend(['-m', f'{file_size_int}B'])

        cmd.append(self.raw_stream_url)

        log_lock = threading.Lock()
        recent_lines = collections.deque(maxlen=30)
        explicitly_closed_paths = set()
        closed_event = threading.Event()
        proc = None
        log_thread = None
        segments_moved = 0
        stop_requested = False
        stopped_by_timerange = False
        stop_deadline = 0.0

        def _read_stdout():
            stdout = proc.stdout if proc is not None else None
            if stdout is None:
                closed_event.set()
                return
            try:
                while True:
                    raw_line = stdout.readline()
                    if not raw_line:
                        break
                    if isinstance(raw_line, bytes):
                        line = raw_line.decode('utf-8', errors='replace').rstrip('\r\n')
                    else:
                        line = str(raw_line).rstrip('\r\n')
                    logger.debug(f"{self.plugin_msg}[mesio]: {line}")
                    with log_lock:
                        recent_lines.append(line)
                    match = _MESIO_CLOSED_SEGMENT_RE.search(line)
                    if match:
                        raw_path = match.group(1).strip()
                        if len(raw_path) >= 2 and raw_path[0] == raw_path[-1] and raw_path[0] in ('"', "'"):
                            raw_path = raw_path[1:-1].strip()
                        if raw_path:
                            norm_candidates = {_normalize_mesio_path(raw_path)}
                            if not os.path.isabs(raw_path) and not os.path.dirname(raw_path):
                                norm_candidates.add(_normalize_mesio_path(os.path.join(session_tmp_dir, raw_path)))
                            with log_lock:
                                explicitly_closed_paths.update(norm_candidates)
                            closed_event.set()
            except (ValueError, OSError):
                pass
            finally:
                closed_event.set()

        def _flush_closed_segments(final: bool) -> int:
            media_files = _list_sorted_mesio_media(session_tmp_dir)
            if not media_files:
                return 0
            with log_lock:
                closed_snapshot = set(explicitly_closed_paths)
            moved_count = 0
            last_idx = len(media_files) - 1
            for idx, (src_path, _name, ext, size) in enumerate(media_files):
                if final:
                    if size == 0:
                        try:
                            os.remove(src_path)
                        except OSError as e:
                            logger.warning(f"{self.plugin_msg}: 删除 mesio 0 字节文件失败 ({src_path}): {e}")
                        continue
                else:
                    if size <= 0:
                        continue
                    norm_src = _normalize_mesio_path(src_path)
                    if norm_src not in closed_snapshot and idx >= last_idx:
                        continue

                base_name = _render_base_name(session_base_name)
                try:
                    dst = _publish_mesio_file(src_path, base_name, ext, '.')
                except OSError as e:
                    logger.warning(f"{self.plugin_msg}: 移出 mesio 分段失败 ({src_path}): {e}")
                    continue
                self.__download_segment_callback(dst)
                moved_count += 1
            return moved_count

        def _has_nonempty_session_media() -> bool:
            return any(size > 0 for _, _, _, size in _list_sorted_mesio_media(session_tmp_dir))

        def _send_graceful_stop():
            if proc is None:
                return
            try:
                if os.name == 'nt':
                    proc.send_signal(getattr(signal, 'CTRL_BREAK_EVENT', 1))
                else:
                    proc.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass
            except OSError as e:
                logger.debug(f"{self.plugin_msg}: 发送 mesio 优雅停机信号失败，尝试 terminate 兜底: {e}")
                try:
                    proc.terminate()
                except OSError:
                    pass

        popen_kwargs = {
            'stdin': subprocess.DEVNULL,
            'stdout': subprocess.PIPE,
            'stderr': subprocess.STDOUT,
        }
        if os.name == 'nt':
            popen_kwargs['creationflags'] = getattr(subprocess, 'CREATE_NEW_PROCESS_GROUP', 0x00000200)

        try:
            try:
                proc = subprocess.Popen(cmd, **popen_kwargs)
            except OSError as e:
                logger.error(f"{self.plugin_msg}: 启动 mesio 进程失败: {e}")
                proc = None

            if proc is not None:
                proc_pid = getattr(proc, 'pid', None)
                if isinstance(proc_pid, int) and not isinstance(proc_pid, bool) and proc_pid > 0:
                    session_meta['pid'] = proc_pid
                    try:
                        _write_mesio_meta(meta_path, session_meta)
                    except OSError as e:
                        logger.warning(f"{self.plugin_msg}: 更新 mesio 进程 PID 元数据失败: {e}")

                if proc.stdout is not None:
                    log_thread = threading.Thread(
                        target=_read_stdout,
                        daemon=True,
                        name=f"mesio_log_{self.database_row_id}_{os.getpid()}",
                    )
                    log_thread.start()

                while True:
                    closed_event.wait(timeout=2.0)
                    closed_event.clear()

                    exited = proc.poll() is not None
                    moved = _flush_closed_segments(final=exited)
                    segments_moved += moved

                    if not exited and not stop_requested:
                        if moved > 0 or _has_nonempty_session_media():
                            low_disk = _get_low_disk_soft_info(self.plugin_msg)
                            if low_disk is not None:
                                free_gb, disk_upload_threshold_gb = low_disk
                                logger.warning(
                                    f"{self.plugin_msg}: 磁盘剩余空间 ({free_gb:.2f} GB) 低于软水位阈值 ({disk_upload_threshold_gb} GB)，"
                                    f"通知 mesio 优雅停止当前录制以立即触发上传与清理"
                                )
                                self._mesio_soft_triggered = True
                                stop_requested = True
                                stop_deadline = time.monotonic() + 15.0
                                _send_graceful_stop()

                        if not stop_requested and self.time_range and not check_timerange(self.fname):
                            logger.info(f"{self.plugin_msg}: 超出录制时间范围，通知 mesio 优雅停止")
                            stopped_by_timerange = True
                            stop_requested = True
                            stop_deadline = time.monotonic() + 15.0
                            _send_graceful_stop()

                    if stop_requested and not exited and time.monotonic() >= stop_deadline:
                        logger.warning(f"{self.plugin_msg}: mesio 优雅停机超时 (15s)，强制结束进程")
                        try:
                            proc.kill()
                        except OSError:
                            pass
                        try:
                            proc.wait(timeout=5)
                        except Exception:
                            pass
                        break

                    if exited:
                        break
        finally:
            if proc is not None:
                try:
                    if proc.poll() is None:
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=5)
                    else:
                        proc.wait(timeout=1)
                except Exception:
                    pass
                if log_thread is not None and log_thread.is_alive():
                    log_thread.join(timeout=3)
                if proc.stdout is not None:
                    try:
                        proc.stdout.close()
                    except Exception:
                        pass

            segments_moved += _flush_closed_segments(final=True)

            try:
                remaining_files = [name for name in os.listdir(session_tmp_dir) if name != '.meta.json']
            except OSError:
                remaining_files = []
            if not remaining_files and os.path.isfile(meta_path):
                try:
                    os.remove(meta_path)
                except OSError:
                    pass
            try:
                os.rmdir(session_tmp_dir)
            except OSError:
                pass
            try:
                os.rmdir('.mesio_tmp')
            except OSError:
                pass

        if proc is None:
            if shutil.which("ffmpeg"):
                return self.ffmpeg_download()
            return False

        with log_lock:
            tail_log = '\n'.join(recent_lines)

        if proc.returncode == 2 and segments_moved == 0:
            logger.error(f"{self.plugin_msg}: mesio 参数不兼容或启动失败 (exit code 2)，最近日志:\n{tail_log}")
            if shutil.which("ffmpeg"):
                return self.ffmpeg_download()
            return False

        if stopped_by_timerange:
            if not self._mesio_soft_triggered and _get_low_disk_soft_info(self.plugin_msg) is not None:
                self._mesio_soft_triggered = True
            return False

        if segments_moved == 0:
            logger.warning(
                f"{self.plugin_msg}: mesio 退出 (returncode={proc.returncode}) 且未产出有效分段，最近日志:\n{tail_log}"
            )
            return False

        return True

    def __download_segment_callback(self, file_name: str):
        """
        分段后触发返回含后戳的文件名
        """
        exclude_ext_file_name = os.path.splitext(file_name)[0]
        danmaku_file_name = os.path.splitext(file_name)[0] + '.xml'
        if self.danmaku:
            self.danmaku.save(danmaku_file_name)

        def x():
            # 将文件名和直播标题存储到数据库
            with SessionLocal() as db:
                update_file_list(db, self.database_row_id, file_name)
            if self.segment_processor:
                try:
                    if not self.segment_processor_parallel and prev_thread:
                        prev_thread.join()
                    from biliup.common.tools import processor
                    data = os.path.abspath(file_name)
                    if os.path.exists(danmaku_file_name):
                        data += f'\n{os.path.abspath(danmaku_file_name)}'
                    processor(self.segment_processor, data)
                except:
                    logger.warning(f'{self.plugin_msg}: 执行后处理失败', exc_info=True)

        thread = threading.Thread(target=x, daemon=True, name=f"segment_processor_{exclude_ext_file_name}")
        prev_thread = self.segment_processor_thread[-1] if self.segment_processor_thread else None
        self.segment_processor_thread.append(thread)
        thread.start()

    def download_success_callback(self):
        pass

    def run(self):
        try:
            if not asyncio.run_coroutine_threadsafe(self.acheck_stream(), loop).result() or not self.should_record():
                return False
            with SessionLocal() as db:
                update_room_title(db, self.database_row_id, self.room_title)
            self.danmaku_init()
            if self.danmaku:
                self.danmaku.start()
            retval = self.download()
            return retval
        finally:
            if self.danmaku:
                self.danmaku.stop()
                self.danmaku = None

    def start(self):
        logger.info(f"{self.plugin_msg}: 开始下载")
        # 开始时间
        start_time = time.localtime()
        # 结束时间
        end_time = None

        with SessionLocal() as db:
            self.database_row_id = add_stream_info(db, self.fname, self.url, start_time)  # 返回数据库中此行记录的 id
        ret = True
        disk_soft_triggered = False
        while ret:
            # 下载结果
            try:
                ret = self.run()
            except Exception:
                logger.warning(f"{self.plugin_msg}: 下载失败", exc_info=True)
                # TODO: 重试等待时间
                time.sleep(3)
            finally:

                self.close()

            # 下载模式跳过下播延迟检测
            if self.is_download:
                break

            # 最后一次下载完成时间
            end_time = time.localtime()

            # 每段录制完成后、进入下一轮循环前，检测磁盘软水位
            if getattr(self, '_mesio_soft_triggered', False):
                disk_soft_triggered = True
                break

            if ret:
                low_disk = _get_low_disk_soft_info(self.plugin_msg)
                if low_disk is not None:
                    free_gb, disk_upload_threshold_gb = low_disk
                    logger.warning(
                        f"{self.plugin_msg}: 磁盘剩余空间 ({free_gb:.2f} GB) 低于软水位阈值 ({disk_upload_threshold_gb} GB)，"
                        f"提前结束本轮录制会话以立即触发上传与清理"
                    )
                    disk_soft_triggered = True
                    break

        self.download_cover(
            time.strftime(self.gen_download_filename().encode("unicode-escape").decode(), end_time if end_time else time.localtime()
                          ).encode().decode("unicode-escape"))
        # 更新数据库中封面存储路径
        with SessionLocal() as db:
            update_cover_path(db, self.database_row_id, self.live_cover_path)

        for thread in self.segment_processor_thread:
            if thread.is_alive():
                logger.info(f'{self.plugin_msg}: 等待分段后处理完成 - {thread.name}')
                thread.join()
        if (self.is_download and ret) or not self.is_download:
            self.download_success_callback()
        # self.segment_processor_thread
        logger.info(f'{self.plugin_msg}: 退出下载')

        if str(self.database_row_id) in context["sync_downloader_map"]:
            context["sync_downloader_map"].pop(str(self.database_row_id))
            logger.info(f"{self.plugin_msg} {self.database_row_id}: 从同步下载器列表中移除")

        stream_info = {
            'name': self.fname,
            'url': self.url,
            'title': self.room_title,
            'date': start_time,
            'end_time': end_time if end_time else time.localtime(),
            'live_cover_path': self.live_cover_path,
            'is_download': self.is_download,
            'platform': self.platform,
            'disk_soft_triggered': disk_soft_triggered,
        }
        return stream_info

    def download_cover(self, fmtname):
        # 获取封面
        if self.use_live_cover and self.live_cover_url is not None:
            try:
                save_dir = f'cover/{self.__class__.__name__}/{self.fname}/'
                if not os.path.exists(save_dir):
                    os.makedirs(save_dir)

                url_path = urlparse(self.live_cover_url).path
                suffix = None
                if '.jpg' in url_path:
                    suffix = 'jpg'
                elif '.png' in url_path:
                    suffix = 'png'
                elif '.webp' in url_path:
                    suffix = 'webp'

                if suffix:
                    live_cover_path = f'{save_dir}{fmtname}.{suffix}'
                    if os.path.exists(live_cover_path):
                        self.live_cover_path = live_cover_path
                    else:
                        response = requests.get(self.live_cover_url, headers=self.fake_headers, timeout=30)
                        with open(live_cover_path, 'wb') as f:
                            f.write(response.content)

                    if suffix == 'webp':
                        with Image.open(live_cover_path) as img:
                            img = img.convert('RGB')
                            img.save(f'{save_dir}{fmtname}.jpg', format='JPEG')
                        os.remove(live_cover_path)
                        live_cover_path = f'{save_dir}{fmtname}.jpg'

                    self.live_cover_path = live_cover_path
                    logger.info(
                        f'{self.plugin_msg}: 封面下载成功，路径：{os.path.abspath(self.live_cover_path)}')
                else:
                    logger.warning(
                        f'{self.plugin_msg}: 封面为不支持的格式：{self.live_cover_url}')
            except:
                logger.exception(f'{self.plugin_msg}: 封面下载失败')

    async def acheck_url_healthy(self, url):
        async def __client_get(url, stream: bool = False):
            request_headers = self.stream_headers.copy()
            self.update_headers(request_headers)
            if stream:
                async with client.stream("GET", url, timeout=60, follow_redirects=False, headers=request_headers) as response:
                    pass
            else:
                response = await client.get(url, headers=request_headers)
            if response.status_code not in (301, 302):
                response.raise_for_status()
            return response

        try:
            if '.m3u8' in url:
                r = await __client_get(url)
                import m3u8
                m3u8_obj = m3u8.loads(r.text)
                if m3u8_obj.is_variant:
                    url = m3u8_obj.playlists[0].uri
                    logger.info(f'{self.plugin_msg}: stream url: {url}')
                    r = await __client_get(url)
            else:  # 处理 Flv
                r = await __client_get(url, stream=True)
                if r.headers.get('Location'):
                    url = r.headers['Location']
                    # logger.info(f'{self.plugin_msg}: stream url: {url}')
                    # url = self.replace_url(url)
                    logger.info(f'{self.plugin_msg}: stream url: {url}')
                    r = await __client_get(url, stream=True)
            if r.status_code == 200:
                return url
        except HTTPStatusError as e:
            logger.debug(f'{self.plugin_msg}: url {url}: status_code-{e.response.status_code}')
        except:
            logger.debug(f'{self.plugin_msg}: url {url}: ', exc_info=True)
        return None

    def gen_download_filename(self, is_fmt=False):
        safe_fname = str(self.fname or '').replace('%', '%%')
        safe_title = str(self.room_title or '').replace('%', '%%')
        if self.filename_prefix:  # 判断是否存在自定义录播命名设置
            filename = (self.filename_prefix.format(streamer=safe_fname, title=safe_title).encode(
                'unicode-escape').decode()).encode().decode("unicode-escape")
        else:
            filename = f'{safe_fname}%Y-%m-%dT%H_%M_%S'
        filename = get_valid_filename(filename)
        if is_fmt:
            file_time = time.time()
            while True:
                fmt_file_name = time.strftime(filename.encode("unicode-escape").decode(),
                                              time.localtime(file_time)).encode().decode("unicode-escape")
                if os.path.exists(f"{fmt_file_name}.{self.suffix}"):
                    file_time += 1
                else:
                    return fmt_file_name
        else:
            return filename

    def update_headers(self, headers):
        pass

    @staticmethod
    def download_file_rename(old_file_name, file_name):
        try:
            os.rename(old_file_name, file_name)
            logger.info(f'更名 {old_file_name} 为 {file_name}')
        except:
            logger.error(f'更名 {old_file_name} 为 {file_name} 失败', exc_info=True)

    def danmaku_init(self):
        pass

    def close(self):
        pass


def stream_gears_download(url, headers, file_name, segment_time=None, file_size=None,
                          file_name_callback: Callable[[str], None] = None):

    segment = stream_gears.PySegment()
    if segment_time:
        seg_time = segment_time.split(':')
        # print(int(seg_time[0]) * 60 * 60 + int(seg_time[1]) * 60 + int(seg_time[2]))
        segment.time = int(seg_time[0]) * 60 * 60 + int(seg_time[1]) * 60 + int(seg_time[2])
    if file_size:
        segment.size = file_size
    if file_size is None and segment_time is None:
        segment.size = 8 * 1024 * 1024 * 1024
    # FIXME: 下载时如出现403，这里不会回到上层方法获取新链接
    if file_name_callback:
        stream_gears.download_with_callback(
            url,
            headers,
            file_name,
            segment,
            file_name_callback
        )
    else:
        stream_gears.download(
            url,
            headers,
            file_name,
            segment,
        )


def sync_download(stream_url, headers, segment_duration=60, max_file_size=100, output_prefix="segment", stream_info=None, file_name_callback: Callable[[str], None] = None, database_row_id=0):
    logger.info(f"启动同步下载器 max_file_size {max_file_size}MB")
    video_queue = queue.SimpleQueue()

    def upload(video_queue, stream_info, stop_event: threading.Event):
        with SessionLocal() as db:
            data = get_stream_info(db, f"{stream_info['name']}")
        data = {**data, "name": stream_info['name']}
        if "title" not in data:
            data["title"] = stream_info.get("title", "")
        # 使用 fmt_title_and_desc 生成格式化后的标题和简介
        # fmt_title_and_desc 返回 (data, context)，其中 context 中包含已格式化的 description
        data, context_fmt = fmt_title_and_desc(data)

        # 更新基本信息（含 format_title）
        stream_info.update(data)

        # 若存在格式化后的简介，将其写入 stream_info，保证后续上传时使用正确的简介
        if context_fmt.get('description'):
            stream_info['description'] = context_fmt['description']
        logger.info(f"stream_info: {stream_info}")
        # 获取 BiliWebAsync.__init__ 的参数名
        init_params = inspect.signature(BiliWebAsync.__init__).parameters
        # 过滤 info 中的无关键
        filtered_info = {key: value for key, value in stream_info.items() if key in init_params}

        filtered_info['submit_api'] = config.get('submit_api')
        filtered_info['lines'] = config.get('lines', 'AUTO')
        # 映射 'uploader' 到 'principal'
        filtered_info['principal'] = ""
        filtered_info["data"] = stream_info
        uploader = BiliWebAsync(**filtered_info, video_queue=video_queue)
        uploader.upload(total_size=max_file_size * 1024 * 1024,
                        stop_event=stop_event, output_prefix=output_prefix,
                        file_name_callback=file_name_callback, database_row_id=database_row_id)
        # print("上传器结束")
        logger.info(f"{stream_info['name']} 上传器结束")
        # video_queue = queue.SimpleQueue()

    downloader = SyncDownloader(stream_url, headers, segment_duration, max_file_size, output_prefix, video_queue)

    # 启动上传器
    upload_thread = threading.Thread(target=upload, args=(video_queue, stream_info, downloader.stop_event), daemon=True)
    upload_thread.start()

    downloader.run()
    logger.info(f"{stream_info['name']} 下载器结束")


def get_valid_filename(name):
    """
    Return the given string converted to a string that can be used for a clean
    filename. Remove leading and trailing spaces; convert other spaces to
    underscores; and remove anything that is not an alphanumeric, dash,
    underscore, or dot.
    # >>> get_valid_filename("john's portrait in 2004.jpg")
    >>> get_valid_filename("{self.fname}%Y-%m-%dT%H_%M_%S")
    '{self.fname}%Y-%m-%dT%H_%M_%S'
    """
    # s = str(name).strip().replace(" ", "_") #因为有些人会在主播名中间加入空格，为了避免和录播完毕自动改名冲突，所以注释掉
    s = re.sub(r"(?u)[^-\w.%{}\[\]【】「」（）・°、。+\s]", "", str(name))
    if s in {"", ".", ".."}:
        raise RuntimeError("Could not derive file name from '%s'" % name)
    return s


def get_duration(segment_time_str, time_range_str):
    """
    计算当前时间到给定结束时间的时差
    如果计算的时差大于segment_time，则返回segment_time。
    """
    try:
        time_range = json.loads(time_range_str)
        if not isinstance(time_range, (list, tuple)) or len(time_range) != 2:
            return segment_time_str
        end_time = datetime.fromisoformat(time_range[1].replace('Z', '+00:00')).time()
    except Exception as e:
        return segment_time_str

    now = datetime.now(timezone.utc).time()
    now_sec = now.hour * 3600 + now.minute * 60 + now.second
    end_sec = end_time.hour * 3600 + end_time.minute * 60 + end_time.second

    # 计算到结束时间的秒数
    diff = end_sec - now_sec if end_sec >= now_sec else (24 * 3600 - now_sec + end_sec)

    try:
        h, m, s = map(int, segment_time_str.split(':'))
        segment_sec = h * 3600 + m * 60 + s
    except Exception:
        return segment_time_str

    if diff > segment_sec:
        return segment_time_str

    hours = diff // 3600
    minutes = (diff % 3600) // 60
    seconds = diff % 60

    return f"{hours:02}:{minutes:02}:{seconds:02}"


class BatchCheck(ABC):
    @staticmethod
    @abstractmethod
    async def abatch_check(check_urls: List[str]) -> AsyncGenerator[str, None]:
        """
        批量检测直播或下载状态
        返回的是url_list
        """
