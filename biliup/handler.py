import json
import logging
import os
import re
import shutil
import requests
import subprocess
import threading
import time
from functools import reduce
from pathlib import Path
from typing import Dict, List, Set, Union

from biliup.config import config
from .app import event_manager, context, pending_download_urls
from .common.notify import send_notification
from .common.tools import NamedLock, processor
from .common.util import check_timerange
from .database.db import get_stream_info_by_filename, SessionLocal
from .downloader import biliup_download
from .engine.event import Event
from .engine.upload import UploadBase
from .uploader import upload, fmt_title_and_desc

PRE_DOWNLOAD = 'pre_download'
DOWNLOAD = 'download'
DOWNLOADED = 'downloaded'
UPLOAD = 'upload'
UPLOADED = 'uploaded'
logger = logging.getLogger('biliup')

_notified_exhausted_lock = threading.Lock()
_notified_exhausted_files: Dict[str, Set[str]] = {}


def clear_notified_exhausted_files():
    """清理已告警的耗尽/熔断文件记录（主要供测试使用）"""
    with _notified_exhausted_lock:
        _notified_exhausted_files.clear()


def _mark_notified_exhausted(
    name: str,
    file_list: List[UploadBase.FileInfo],
    max_upload_limit: int,
    *,
    all_files: bool = False,
) -> Set[str]:
    if all_files:
        exhausted_videos = {item.video for item in file_list}
    else:
        with NamedLock("file_upload_count"):
            file_upload_count = context.setdefault('file_upload_count', {})
            exhausted_videos = {
                item.video for item in file_list
                if file_upload_count.get(item.video, 0) >= max_upload_limit
            }
        if not exhausted_videos:
            exhausted_videos = {item.video for item in file_list}
    if not exhausted_videos:
        return set()
    with _notified_exhausted_lock:
        existing = _notified_exhausted_files.setdefault(name, set())
        newly_marked = exhausted_videos - existing
        existing.update(newly_marked)
        return newly_marked


def _unmark_notified_exhausted(name: str, videos: Set[str]) -> None:
    if not videos:
        return
    with _notified_exhausted_lock:
        existing = _notified_exhausted_files.get(name)
        if not existing:
            return
        existing.difference_update(videos)
        if not existing:
            _notified_exhausted_files.pop(name, None)


# @event_manager.register(CHECK, block='Asynchronous3')


@event_manager.register(PRE_DOWNLOAD, block='Asynchronous1')
def pre_processor(name, url):
    plugin_info = context["PluginInfo"]
    url_status = plugin_info.url_status
    with NamedLock('download_dispatch'):
        inverted_index = getattr(plugin_info, 'inverted_index', None)
        if inverted_index is not None and inverted_index.get(url) != name:
            pending_download_urls.discard(url)
            logger.debug(f"{name} - {url} 已移除或变更，跳过下载")
            return
        if url_status.get(url) == 1:
            pending_download_urls.discard(url)
            logger.debug(f"{name} - {url} 正在下载中，跳过下载")
            return
        url_status[url] = 1
        pending_download_urls.discard(url)

    logger.debug(f"{name} - {url} 开播了准备下载")
    try:
        preprocessor = config['streamers'].get(name, {}).get('preprocessor')
        if preprocessor:
            processor(preprocessor, json.dumps({
                "name": name,
                "url": url,
                "start_time": int(time.time())
            }, ensure_ascii=False))
    except Exception:
        with NamedLock('download_dispatch'):
            url_status[url] = 0 if check_timerange(name) else 2
            pending_download_urls.discard(url)
        raise
    yield Event(DOWNLOAD, (name, url))


@event_manager.register(DOWNLOAD, block='Asynchronous1')
def process(name, url):
    url_status = context['PluginInfo'].url_status
    # 下载开始
    try:
        with NamedLock('download_dispatch'):
            url_status[url] = 1
            pending_download_urls.discard(url)
        stream_info = biliup_download(name, url, config['streamers'][name].copy())
        yield Event(DOWNLOADED, (stream_info,))
    except Exception as e:
        logger.exception(f"下载错误: {name} - {e}")
    finally:
        # 下载结束
        with NamedLock('download_dispatch'):
            url_status[url] = 0 if check_timerange(name) else 2
            pending_download_urls.discard(url)


@event_manager.register(DOWNLOADED, block='Asynchronous1')
def processed(stream_info):
    name = stream_info['name']
    url = stream_info['url']
    # 下载后处理 上传前处理
    downloaded_processor = config['streamers'].get(name, {}).get('downloaded_processor')
    if downloaded_processor:
        default_date = time.localtime()
        file_list = UploadBase.file_list(name)
        processor(downloaded_processor, json.dumps({
            "name": name,
            "url": url,
            "room_title": stream_info.get('title', name),
            "start_time": int(time.mktime(stream_info.get('date', default_date))),
            "end_time": int(time.mktime(stream_info.get('end_time', default_date))),
            "file_list": [file.video for file in file_list]
        }, ensure_ascii=False))
        # 后处理完成后重新扫描文件列表
    yield Event(UPLOAD, (stream_info,))


_UNRECOVERABLE_CODE_RE = re.compile(
    r'(?:^|[\s{,"\'(])(?:(?:error\s*)?code["\']?\s*[:=]\s*)?(?:-101|21001|21015|21016|21044|21045)(?=$|[\s},)"\':;，。])'
)
_UNRECOVERABLE_OS_ERROR_RE = re.compile(r'\bos error 2(?![0-9])')


def is_unrecoverable_error(e: Union[Exception, str, None]) -> bool:
    if e is None:
        return False
    if isinstance(e, FileNotFoundError):
        return True
    err_str = str(e).lower()
    unrecoverable_patterns = [
        "未登录",
        "cookie失效",
        "cookie 失效",
        "cookie过期",
        "cookie 过期",
        "cookie已失效",
        "cookie expired",
        "cookie invalid",
        "文件不存在",
        "filenotfound",
        "no such file",
        "cannot find the file",
    ]
    if any(p in err_str for p in unrecoverable_patterns):
        return True
    if _UNRECOVERABLE_OS_ERROR_RE.search(err_str):
        return True
    return bool(_UNRECOVERABLE_CODE_RE.search(err_str))


@event_manager.register(UPLOAD, block='Asynchronous2')
def process_upload(stream_info):
    url = stream_info['url']
    name = stream_info['name']
    url_upload_count = context['url_upload_count']
    # 永远不可能有两个同url的下载线程
    # 可能对同一个url同时发送两次上传事件
    with NamedLock(f"upload_count_{url}"):
        if context['url_upload_count'][url] > 0:
            return logger.debug(f'{url} 正在上传中，跳过')
        # from .handler import event_manager, UPLOAD
        # += 不是原子操作
        context['url_upload_count'][url] += 1
    # 上传开始
    try:
        raw_file_list = UploadBase.file_list(name)
        if len(raw_file_list) <= 0:
            with _notified_exhausted_lock:
                _notified_exhausted_files.pop(name, None)
            logger.debug("无需上传")
            return

        current_videos = {item.video for item in raw_file_list}
        with _notified_exhausted_lock:
            existing_notified = _notified_exhausted_files.get(name)
            if existing_notified is not None:
                pruned = existing_notified & current_videos
                if pruned:
                    _notified_exhausted_files[name] = pruned
                else:
                    _notified_exhausted_files.pop(name, None)

        raw_limit = config.get("max_upload_limit", 999)
        max_upload_limit = 999 if raw_limit is None else int(raw_limit)
        with NamedLock("file_upload_count"):
            file_upload_count = context.setdefault('file_upload_count', {})
            file_list = [
                item for item in raw_file_list
                if file_upload_count.get(item.video, 0) < max_upload_limit
            ]
        file_count = len(file_list)
        if file_count <= 0:
            with _notified_exhausted_lock:
                newly_exhausted = current_videos - _notified_exhausted_files.get(name, set())
                if newly_exhausted:
                    _notified_exhausted_files.setdefault(name, set()).update(newly_exhausted)
            if newly_exhausted:
                logger.warning(f"上传次数达到阈值: {name}")
                exhausted_batch = set(newly_exhausted)
                send_notification(
                    title=f"biliup 上传达到重试上限告警: {name}",
                    content=f"主播: {name}\nURL: {url}\n待传文件均已达到重试阈值 ({max_upload_limit})，已停止自动重试。",
                    streamer_name=name,
                    error_type="max_upload_limit_reached",
                    on_failure=lambda vids=exhausted_batch: _unmark_notified_exhausted(name, vids),
                )
            else:
                logger.debug(f"待传文件均已达到重试阈值且已告警过，跳过重复告警: {name}")
            return

        # 上传延迟检测
        url_status = context['PluginInfo'].url_status
        if not stream_info.get('disk_soft_triggered') and url_status.get(url) != 2:
            delay = int(config.get('delay') or 0)
            if delay:
                logger.info(f'{name} -> {url} {delay}s 后检测是否上传')
                time.sleep(delay)
                if url_status.get(url) == 1:
                    # 上传延迟检测，启用的话会在一段时间后检测是否存在下载任务，若存在则跳过本次上传
                    return logger.info(f'{name} -> {url} 存在下载任务, 跳过本次上传')

        if ("title" not in stream_info) or (not stream_info["title"]):  # 如果 data 中不存在标题, 说明下载信息已丢失, 则尝试从数据库获取
            with SessionLocal() as db:
                i = 0
                # 按创建时间排序可能导致首个结果无法获取到数据
                while i < file_count:
                    data = get_stream_info_by_filename(db, file_list[i].video)
                    if data:
                        break
                    i += 1
            data, _ = fmt_title_and_desc({**data, "name": name})  # 如果 restart, data 中会缺失 name 项
            stream_info.update(data)

        upload_count = UploadBase.get_max_upload_count(file_list)
        filelist = None
        if upload_count < max_upload_limit:
            logger.info(f"开始第{upload_count+1}次上传： {name}")

            # 上传webhook部分
            expt = None
            try:
                res_files = upload(stream_info, target_files=[item.video for item in file_list])
                if res_files:
                    filelist = res_files
                else:
                    logger.info(f"{name} 未实际占用待传文件，回退本批重试计数")
                    with NamedLock("file_upload_count"):
                        file_upload_count = context.setdefault('file_upload_count', {})
                        for item in file_list:
                            if item.video in file_upload_count:
                                file_upload_count[item.video] = max(0, file_upload_count[item.video] - 1)
            except Exception as e:
                expt = e
                logger.error(f"{name} 上传异常: {e}")
                if is_unrecoverable_error(e):
                    logger.error(f"{name} 检测到不可自愈上传错误 ({e})，标记该批文件不再重试")
                    with NamedLock("file_upload_count"):
                        file_upload_count = context.setdefault('file_upload_count', {})
                        for item in file_list:
                            file_upload_count[item.video] = max_upload_limit
                    marked_videos = _mark_notified_exhausted(name, file_list, max_upload_limit, all_files=True)
                    send_notification(
                        title=f"biliup 上传不可自愈错误告警: {name}",
                        content=f"主播: {name}\nURL: {url}\n错误信息: {e}\n该批文件已被标记跳过重试，请检查账号状态或Cookie。",
                        streamer_name=name,
                        error_type="unrecoverable_upload_error",
                        on_failure=lambda vids=marked_videos: _unmark_notified_exhausted(name, vids),
                    )
                elif upload_count + 1 >= max_upload_limit:
                    logger.warning(f"上传次数达到阈值: {name}")
                    marked_videos = _mark_notified_exhausted(name, file_list, max_upload_limit)
                    send_notification(
                        title=f"biliup 上传达到重试上限告警: {name}",
                        content=f"主播: {name}\nURL: {url}\n部分或全部待传文件重试次数已达到阈值 ({upload_count + 1}/{max_upload_limit})，达到上限的文件将停止自动重试（其余未达上限文件仍会在后续重试）。最后一次错误: {e}",
                        streamer_name=name,
                        error_type="max_upload_limit_reached",
                        on_failure=lambda vids=marked_videos: _unmark_notified_exhausted(name, vids),
                    )
            finally:
                post_processor = config['streamers'].get(name, {}).get("postprocessor", None)
                if post_processor is not None and (expt is not None or filelist):
                    for processor in post_processor:
                        if isinstance(processor, dict) and processor.get('webhook') is not None:
                            try:
                                logger.info(f"{name}发送webhook： {processor.get('webhook')}")
                                res = requests.post(
                                    url = processor.get('webhook'),
                                    json = {
                                        "stream_info": {**stream_info, **config['streamers'].get(stream_info['name'], {})},
                                        "caught_error": str(expt) if expt else ""
                                    },
                                    timeout = 15
                                ).text
                                logger.info(f"{name}发送webhook： {res}")
                                if res != "success":
                                    logger.warning(f"{name} webhook 返回非 success: {res}")
                            except Exception as wh_err:
                                logger.error(f"{name} 发送webhook失败: {wh_err}")
                 
        else:
            logger.warning(f"上传次数达到阈值: {name}")
            marked_videos = _mark_notified_exhausted(name, file_list, max_upload_limit)
            send_notification(
                title=f"biliup 上传达到重试上限告警: {name}",
                content=f"主播: {name}\nURL: {url}\n上传重试次数已达到阈值 ({upload_count}/{max_upload_limit})，已停止自动重试。",
                streamer_name=name,
                error_type="max_upload_limit_reached",
                on_failure=lambda vids=marked_videos: _unmark_notified_exhausted(name, vids),
            )

        if filelist:
            uploaded(name, stream_info.get('live_cover_path'), filelist)
    except Exception:
        logger.exception(f"上传错误: {name}")
    finally:
        # 上传结束
        # 有可能有两个同url的上传线程 保证计数正确
        with NamedLock(f'upload_count_{url}'):
            url_upload_count[url] -= 1


def uploaded(name, live_cover_path, data: List):
    # data = file_list
    post_processor = config['streamers'].get(name, {}).get("postprocessor", None)
    if post_processor is None:
        # 删除封面
        if live_cover_path is not None:
            UploadBase.remove_file(live_cover_path)
        return UploadBase.remove_filelist(data)

    file_list = []
    for i in data:
        file_list.append(i.video)
        if i.danmaku is not None:
            file_list.append(i.danmaku)

    for post_processor in post_processor:
        if  ( # 兼容新WEB字段
                isinstance(post_processor, str)
                and
                post_processor == 'rm'
            ) or (
                isinstance(post_processor, dict)
                and
                post_processor.get('rm')
            ):
            # 删除封面
            if live_cover_path is not None:
                UploadBase.remove_file(live_cover_path)
            UploadBase.remove_filelist(data)
            continue
        if post_processor.get('mv'):
            for file in file_list:
                path = Path(file)
                dest = Path(post_processor['mv'])
                if not dest.is_dir():
                    dest.mkdir(parents=True, exist_ok=True)
                try:
                    shutil.move(path, dest / path.name)
                except Exception as e:
                    logger.exception(e)
                    continue
                logger.info(f"move to {(dest / path.name).absolute()}")
        if post_processor.get('run'):
            try:
                process_output = subprocess.check_output(
                    post_processor['run'], shell=True,
                    input=reduce(lambda x, y: x + str(Path(y).absolute()) + '\n', file_list, ''),
                    stderr=subprocess.STDOUT, text=True)
                logger.info(process_output.rstrip())
            except subprocess.CalledProcessError as e:
                logger.exception(e.output)
                continue
