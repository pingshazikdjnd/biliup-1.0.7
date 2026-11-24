import inspect
import logging
import os
import time
import json

from biliup.config import config
from .common.tools import NamedLock
from .engine.decorators import Plugin
from .engine.upload import UploadBase

logger = logging.getLogger('biliup')

def merge_dict(*dicts):
    """
    合并多个字典，兼容空字典、None 或 JSON 字符串
    :param dicts: 任意数量的字典
    :return: 合并后的字典
    """
    result = {}
    for d in dicts:
        if isinstance(d, str):  # 如果是 JSON 字符串，尝试解析
            try:
                d = json.loads(d)
            except json.JSONDecodeError:
                continue
        if d:  # 检查字典是否为 None 或空
            result.update(d)
    return result

def upload(data, target_files=None):
    """
    上传入口
    :param platform:
    :param index:
    :param data: 现在需包含内容{url,date} 完整包含内容{url,date,format_title}
    :return:
    """
    try:
        index = data['name']
        context = {**config, **config['streamers'][index]}
        platform = context.get("uploader") if context.get("uploader") else "bili_web"
        cls = Plugin.upload_plugins.get(platform)
        if cls is None:
            logger.error(f"No such uploader: {platform}")
            raise RuntimeError(f"No such uploader: {platform}")
        data, context = fmt_title_and_desc(data)
        data['dolby'] = config.get('dolby', 0)
        data['hires'] = config.get('hires', 0)
        data['no_reprint'] = config.get('no_reprint', 0)
        data['extra_fields'] = json.dumps(merge_dict(context.get('extra_fields', {}), {"is_only_self": context.get('is_only_self', 0)}))

        data['charging_pay'] = config.get('charging_pay', 0)
        sig = inspect.signature(cls)
        kwargs = {}
        for k in sig.parameters:
            v = context.get(k)
            if v:
                kwargs[k] = v
        uploader_instance = cls(index, data, **kwargs)
        if target_files is not None:
            return uploader_instance.start(allowed_files=target_files)
        return uploader_instance.start()
    except Exception:
        logger.exception("Uncaught exception:")
        raise


def biliup_uploader(filelist, data):
    if not filelist:
        logger.warning("biliup_uploader 收到空文件列表，跳过上传")
        return None
    registered_names = []
    app_context = None
    try:
        from .app import context as app_context
        index = data['name']
        data, fmt_ctx = fmt_title_and_desc_m(data)
        context = {**config, **{k: v for k, v in fmt_ctx.items() if v is not None and v != ''}}
        platform = context.get("uploader") if context.get("uploader") else "biliup-rs"
        cls = Plugin.upload_plugins.get(platform)
        if cls is None:
            logger.error(f"No such uploader: {platform}")
            return None
        data['dolby'] = context.get('dolby') or 0
        data['hires'] = context.get('hires') or 0
        data['no_reprint'] = context.get('no_reprint') or 0
        data['charging_pay'] = context.get('charging_pay') or 0
        data['extra_fields'] = json.dumps(
            merge_dict(context.get('extra_fields', ''), {"is_only_self": context.get('is_only_self') or 0})
        )

        active_files = []
        with NamedLock('upload_filename'):
            upload_filename_list = app_context.setdefault('upload_filename', [])
            seen = set(upload_filename_list)
            for f in filelist:
                raw_path = f.video if hasattr(f, 'video') else str(f)
                file_base = os.path.splitext(raw_path)[0]
                if file_base in seen:
                    logger.warning(f"文件 {raw_path} 正在上传中，跳过重复上传")
                    continue
                seen.add(file_base)
                file_item = f if hasattr(f, 'video') else UploadBase.FileInfo(video=str(f), danmaku=None)
                active_files.append(file_item)
                registered_names.append(file_base)
            upload_filename_list.extend(registered_names)

        if not active_files:
            logger.warning("所有文件均已在上传中，跳过本次手动上传")
            return None

        sig = inspect.signature(cls)
        kwargs = {}
        for k in sig.parameters:
            v = context.get(k)
            if v is not None and v != '':
                kwargs[k] = v
        logger.info("start biliup")
        return cls(index, data, **kwargs).upload(active_files)
    except Exception:
        logger.exception("Uncaught exception:")
    else:
        logger.info("stop biliup")
    finally:
        if registered_names and app_context is not None:
            with NamedLock('upload_filename'):
                current = app_context.setdefault('upload_filename', [])
                for base_name in registered_names:
                    try:
                        current.remove(base_name)
                    except ValueError:
                        pass


def fmt_title_and_desc_m(data):
    index = data['name']
    context = {**data}
    streamer = data.get('streamer', index)
    date = data.get("date", time.localtime())
    title = data.get('title', index)
    url = data.get('url')
    data["format_title"] = custom_fmtstr(context.get('title') or f'%Y.%m.%d{index}', date, title, streamer, url)
    if context.get('description'):
        context['description'] = custom_fmtstr(context.get('description'), date, title, streamer, url)
    return data, context


# 将格式化标题和简介拆分出来方便复用
def fmt_title_and_desc(data):
    """
    格式化标题和简介
    :param data: {name,url,{
        title,
        row_id,
        name,
        live_cover_path,
        url,
        date: time.struct_time()
    }}
    :return: {name,url,date,format_title}
    """
    index = data['name']
    context = {**config, **config['streamers'][index]}
    user_format_string = context.get('title') or f'%Y.%m.%d{index}'
    live_stream_title = data.get('title', index)
    streamer_name = data.get('streamer', index)
    upload_date = data.get("date", time.localtime())
    stream_url = data.get('url')

    if "{title}" in user_format_string:
        data["format_title"] = custom_fmtstr(user_format_string, upload_date, live_stream_title, streamer_name, stream_url)
    else:
        data["format_title"] = custom_fmtstr(user_format_string, upload_date, streamer_name, streamer_name, stream_url)

    if context.get('description'):
        context['description'] = custom_fmtstr(context.get('description'), upload_date, live_stream_title, streamer_name, stream_url)
    return data, context


def custom_fmtstr(string, date, title, streamer, url):
    return time.strftime(string.encode('unicode-escape').decode(), date).encode().decode("unicode-escape").format(
        title=title, streamer=streamer, url=url)
