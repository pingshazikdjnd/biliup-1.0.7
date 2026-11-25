import asyncio
import atexit
import contextlib
import hashlib
import hmac
import secrets
import socket
import json
import multiprocessing
import os
import pathlib
import concurrent.futures
import tempfile
import threading
import time

import copy
import aiohttp_cors
import requests
import stream_gears
from aiohttp import web, ClientTimeout
from aiohttp.client import ClientSession
from sqlalchemy import select, update
from sqlalchemy.exc import NoResultFound, MultipleResultsFound
from urllib.parse import urlparse

import biliup.common.reload
from biliup.config import config
from biliup.plugins.bili_webup import BiliBili, Data
from .aiohttp_basicauth_middleware import basic_auth_middleware, check_access
from biliup.database.db import SessionLocal
from biliup.database.models import UploadStreamers, LiveStreamers, Configuration, StreamerInfo
from ..app import logger

try:
    from importlib.resources import files
except ImportError:
    # Try backported to PY<37 `importlib_resources`.
    from importlib_resources import files

routes = web.RouteTableDef()

_BILI_REQUEST_TIMEOUT = (5, 15)


@contextlib.contextmanager
def _bili_client(timeout=_BILI_REQUEST_TIMEOUT):
    """为单次请求创建独立 BiliBili 实例，并为 Session 出站请求设置默认连接/读超时"""
    with BiliBili(Data()) as bili:
        session = getattr(bili, '_BiliBili__session', None)
        orig_request = getattr(session, 'request', None)
        if callable(orig_request):
            def _timed_request(*args, **kwargs):
                if kwargs.get('timeout') is None:
                    kwargs['timeout'] = timeout
                return orig_request(*args, **kwargs)
            session.request = _timed_request
        yield bili


def _load_cookies_from_file(path: str) -> dict:
    """从 cookie 文件局部读取 cookies 字典，不污染全局 config.data['user']"""
    if not path or not os.path.isfile(path):
        raise FileNotFoundError(path)
    with open(path, encoding='utf-8') as stream:
        data = json.load(stream)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid cookie file format: {path}")
    cookie_list = data.get("cookie_info", {}).get("cookies")
    if not isinstance(cookie_list, list):
        raise ValueError(f"Invalid cookie file format: {path}")
    cookies = {}
    for item in cookie_list:
        if isinstance(item, dict):
            name = item.get("name")
            value = item.get("value")
            if name is not None and value is not None:
                cookies[name] = value
    return cookies

# 全局变量存储密码
GLOBAL_PASSWORD = None
MAX_PROXY_BODY_SIZE = 10 * 1024 * 1024  # 10 MB
STATIC_TOKEN_TTL_SECONDS = 7200  # 2 小时


def generate_static_token(secret: bytes, filename: str, ttl: int = STATIC_TOKEN_TTL_SECONDS) -> str:
    """生成绑定单文件与过期时间的短时 HMAC-SHA256 签名令牌"""
    exp = int(time.time()) + int(ttl)
    payload = f"{filename}:{exp}".encode('utf-8')
    sig = hmac.new(secret, payload, hashlib.sha256).hexdigest()
    return f"{exp}.{sig}"


def verify_static_token(secret: bytes, filename: str, token: str) -> bool:
    """校验单文件短时签名令牌"""
    if not secret or not filename or not token or '.' not in token:
        return False
    exp_str, sig = token.split('.', 1)
    try:
        exp = int(exp_str)
    except ValueError:
        return False
    if exp < int(time.time()):
        return False
    payload = f"{filename}:{exp}".encode('utf-8')
    expected_sig = hmac.new(secret, payload, hashlib.sha256).hexdigest()
    try:
        return hmac.compare_digest(expected_sig.encode('utf-8'), sig.encode('utf-8'))
    except Exception:
        return False


def _validate_static_token(request: web.Request) -> bool:
    if request.method not in ('GET', 'HEAD') or not request.path.startswith('/static/'):
        return False
    filename = request.path[len('/static/'):]
    token = request.query.get('token', '')
    return verify_static_token(request.app.get('static_token_secret'), filename, token)


def _is_registered_cookie_file(file_path: str) -> bool:
    """校验传入的 Cookie 文件路径是否已登记在 bilibili-cookies 或现有投稿模板中"""
    if not file_path:
        return False
    with SessionLocal() as db:
        registered = db.scalars(
            select(Configuration.value).where(Configuration.key == 'bilibili-cookies')
        ).all()
        if file_path in registered:
            return True
        template_cookies = db.scalars(
            select(UploadStreamers.user_cookie).where(
                UploadStreamers.user_cookie.is_not(None),
                UploadStreamers.user_cookie != ''
            )
        ).all()
    return file_path in template_cookies


def set_global_password(password: str):
    """
    设置全局密码
    :param password: 要设置的密码
    """
    global GLOBAL_PASSWORD
    GLOBAL_PASSWORD = password


def check_url_auth(auth_header: str) -> bool:
    """
    验证URL参数中的认证信息
    :param auth_header: 包含认证信息的Authorization头
    :return: 认证是否成功
    """
    # 使用全局密码变量
    if not GLOBAL_PASSWORD:
        return True  # 如果没有设置密码，则允许访问
    
    # 使用现有的 check_access 函数验证认证信息
    auth_dict = {'biliup': GLOBAL_PASSWORD}
    return check_access(auth_dict, auth_header)

@routes.get('/api/basic')
async def get_basic_config(request):
    res = {
        "line": config.data.get('lines', 'AUTO'),
        "limit": config.data.get('threads', 3),
    }
    if config.data.get("toml"):
        res['toml'] = True
    else:
        res['user'] = {
            "SESSDATA": config.data.get('user', {}).get('cookies', {}).get('SESSDATA', ''),
            "bili_jct": config.data.get('user', {}).get('cookies', {}).get('bili_jct', ''),
            "DedeUserID__ckMd5": config.data.get('user', {}).get('cookies', {}).get('DedeUserID__ckMd5', ''),
            "DedeUserID": config.data.get('user', {}).get('cookies', {}).get('DedeUserID', ''),
            "access_token": config.data.get('user', {}).get('access_token', ''),
        }

    return web.json_response(res)

@routes.get('/url-status')
async def url_status(request):
    from biliup.app import context
    plugin_info = context.get('PluginInfo')
    return web.json_response(plugin_info.url_status if plugin_info is not None else {})

@routes.post('/api/setbasic')
async def set_basic_config(request):
    post_data = await request.json()
    config.data['lines'] = post_data.get('line', 'AUTO')
    if config.data['lines'] == 'cos':
        config.data['lines'] = 'cos-internal'
    config.data['threads'] = post_data.get('limit', 3)
    if not config.data.get("toml"):
        cookies = {
            "SESSDATA": str(post_data.get('user', {}).get('SESSDATA', '')),
            "bili_jct": str(post_data.get('user', {}).get('bili_jct', '')),
            "DedeUserID__ckMd5": str(post_data.get('user', {}).get('DedeUserID__ckMd5', '')),
            "DedeUserID": str(post_data.get('user', {}).get('DedeUserID', '')),
        }
        config.data['user'] = config.data.get('user', {})
        config.data['user']['cookies'] = cookies
        config.data['user']['access_token'] = str(post_data.get('user', {}).get('access_token', ''))
    return web.json_response({"status": 200})

@routes.get('/api/getconfig')
async def get_streamer_config(request):
    return web.json_response(config.data.get('streamers', {}))

@routes.get('/api/save')
async def save_config(request):
    config.save()
    biliup.common.reload.global_reloader.triggered = True
    import logging
    logger = logging.getLogger('biliup')
    logger.info("配置保存，将在进程空闲时重启")
    return web.json_response({"status": 200}, status=200)

@routes.get('/')
async def root_handler(request):
    return web.HTTPFound('/index.html')

@routes.get('/api/login_by_cookie')
async def cookie_login(request):
    if config.data.get("toml"):
        logger.debug("trying to login by cookie")
        try:
            await asyncio.to_thread(stream_gears.login_by_cookies, 'cookies.json', None)
        except Exception:
            logger.exception("cookie_login failed (toml)")
            return web.HTTPBadRequest(text="login failed")
    else:
        cookie = (config.data.get('user') or {}).get('cookies') or {}
        if not cookie:
            return web.HTTPBadRequest(text="login failed: missing cookies")
        try:
            def _do_login():
                with _bili_client() as bili:
                    bili.login_by_cookies(cookie)

            await asyncio.to_thread(_do_login)
        except Exception:
            logger.exception("cookie_login failed")
            return web.HTTPBadRequest(text="login failed")
    return web.json_response({"status": 200})


# async def sms_login(request):
#     pass


# async def sms_send(request):
#     # post_data = await request.json()

#     pass


def check_similar_remark(json_data):
    '''
    :return: similar remark or None
    '''
    _cache = copy.deepcopy(config.get('streamers', {}))
    for fname, data in _cache.items():
        if (
            json_data['remark'] in fname
            or
            fname in json_data['remark']
        ) and (
            json_data['url'] != data['url']
        ):
            return fname
    return None


@routes.get('/v1/get_qrcode')
async def qrcode_get(request):
    try:
        raw = await asyncio.to_thread(stream_gears.get_qrcode, None)
        r = json.loads(raw)
    except Exception:
        logger.exception("get qrcode error")
        return web.HTTPBadRequest(text="get qrcode failed")
    return web.json_response(r)


_qr_pool = None
_qr_pool_lock = threading.Lock()


def _get_qr_pool():
    global _qr_pool
    with _qr_pool_lock:
        if _qr_pool is None:
            mp_ctx = multiprocessing.get_context('spawn')
            _qr_pool = concurrent.futures.ProcessPoolExecutor(max_workers=2, mp_context=mp_ctx)
        return _qr_pool


def _shutdown_qr_pool():
    global _qr_pool
    with _qr_pool_lock:
        pool_ref = _qr_pool
        _qr_pool = None
    if pool_ref is not None:
        processes = list((getattr(pool_ref, '_processes', None) or {}).values())
        pool_ref.shutdown(wait=False, cancel_futures=True)
        for proc in processes:
            try:
                if proc.is_alive():
                    proc.terminate()
            except Exception:
                pass
        for proc in processes:
            try:
                proc.join(timeout=3)
                if proc.is_alive():
                    proc.kill()
                    proc.join(timeout=1)
            except Exception:
                pass


atexit.register(_shutdown_qr_pool)


@routes.post('/v1/login_by_qrcode')
async def qrcode_login(request):
    post_data = await request.json()
    try:
        loop = asyncio.get_running_loop()
        qr_pool = _get_qr_pool()
        task = loop.run_in_executor(qr_pool, stream_gears.login_by_qrcode, json.dumps(post_data), None)
        res = await asyncio.wait_for(task, 180)
        data = json.loads(res)
        mid = str(data["token_info"]["mid"])
        if not mid.isdigit():
            raise ValueError("invalid mid")
        os.makedirs('data', exist_ok=True)
        filename = f'data/{mid}.json'
        fd, tmp_path = tempfile.mkstemp(prefix=f'.{mid}.json.', suffix='.tmp', dir='data')
        try:
            if os.name != 'nt':
                os.chmod(tmp_path, 0o600)
            with os.fdopen(fd, 'w', encoding='utf-8') as file:
                fd = None
                file.write(res)
                file.flush()
                os.fsync(file.fileno())
            os.replace(tmp_path, filename)
            tmp_path = None
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
            if tmp_path is not None:
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
        return web.json_response({
            'filename': filename
        })
    except Exception:
        logger.exception('login_by_qrcode')
        return web.HTTPBadRequest(text="login failed")

@routes.get('/api/check_tag')
async def tag_check(request):
    tag = request.rel_url.query['tag']

    def _do_check_tag():
        with _bili_client() as bili:
            return bili.check_tag(tag)

    try:
        passed = await asyncio.to_thread(_do_check_tag)
    except requests.Timeout:
        logger.warning('check_tag timeout for tag=%s', tag)
        return web.HTTPGatewayTimeout(text="tag check timeout")
    if passed:
        return web.json_response({"status": 200})
    return web.HTTPBadRequest(text="标签违禁")


@routes.get('/v1/videos')
async def streamers(request):
    media_extensions = ['.mp4', '.flv', '.3gp', '.webm', '.mkv', '.ts']
    _blacklist = ['next-env.d.ts']
    # 获取文件列表
    file_list = []
    i = 1
    for file_name in os.listdir('.'):
        if file_name in _blacklist:
            continue
        name, ext = os.path.splitext(file_name)
        if ext in media_extensions:
            file_list.append({'key': i, 'name': file_name, 'updateTime': os.path.getmtime(file_name),
                              'size': os.path.getsize(file_name)})
            i += 1
    return web.json_response(file_list)


@routes.get('/v1/streamer-info')
async def streamers(request):
    res = []
    with SessionLocal() as db:
        result = db.scalars(select(StreamerInfo))
        for s_info in result:
            streamer_info = s_info.as_dict()
            streamer_info['files'] = []
            for file in s_info.filelist:
                tmp = file.as_dict()
                del tmp['streamer_info_id']
                streamer_info['files'].append(tmp)
            streamer_info['date'] = int(streamer_info['date'].timestamp())
            res.append(streamer_info)
    return web.json_response(res)


@routes.get('/v1/streamers')
async def streamers(request):
    from biliup.app import context
    from biliup.common.util import check_timerange
    res = []
    with SessionLocal() as db:
        result = db.scalars(select(LiveStreamers))
        for ls in result:
            temp = ls.as_dict()
            url = temp['url']
            status = 'Idle'
            if context['PluginInfo'].url_status.get(url) == 1:
                status = 'Working'
            if not check_timerange(temp['remark']):
                status = 'OutOfSchedule'
            if context['url_upload_count'].get(url, 0) > 0:
                status = 'Inspecting'
            temp['status'] = status
            if temp.get("upload_streamers_id"):  # 返回 upload_id 而不是 upload_streamers
                temp["upload_id"] = temp["upload_streamers_id"]
            temp.pop("upload_streamers_id")
            res.append(temp)
    return web.json_response(res)


@routes.post('/v1/streamers')
async def add_lives(request):
    from biliup.app import context
    json_data = await request.json()
    similar_remark = check_similar_remark(json_data)
    if similar_remark:
        return web.HTTPBadRequest(text=f"{json_data['remark']} 与现存备注 {similar_remark} 存在部分重复，禁止添加")
    uid = json_data.get('upload_id')
    with SessionLocal() as db:
        if uid:
            us = db.get(UploadStreamers, uid)
            to_save = LiveStreamers(**LiveStreamers.filter_parameters(json_data), upload_streamers_id=us.id)
        else:
            to_save = LiveStreamers(**LiveStreamers.filter_parameters(json_data))
        try:
            db.add(to_save)
            db.commit()
        # db.flush(to_save)
        except Exception as e:
            logger.exception("Error handling request")
            return web.HTTPBadRequest(text=str(e))
        config.load_from_db(db)
        context['PluginInfo'].add(json_data['remark'], json_data['url'])
        return web.json_response(to_save.as_dict())


@routes.put('/v1/streamers')
async def lives(request):
    from biliup.app import context
    json_data = await request.json()
    # old = LiveStreamers.get_by_id(json_data['id'])
    # if check_similar_remark(json_data):
    #     return web.HTTPBadRequest(text=f"{json_data['remark']} 与现存备注存在部分重复，禁止修改")
    with SessionLocal() as db:
        old = db.get(LiveStreamers, json_data['id'])
        if old is None:
            return web.HTTPNotFound(text="Streamer not found")
        old_url = old.url
        # 如果备注修改，才需要检查是否与现存备注存在部分重复
        if json_data['remark'] != old.remark:
            similar_remark = check_similar_remark(json_data)
            if similar_remark:
                return web.HTTPBadRequest(text=f"{json_data['remark']} 与现存备注 {similar_remark} 存在部分重复，禁止修改")
        uid = json_data.get('upload_id')
        # semi-ui 不能直接为 ArrayField 设置空默认值
        # 当前端更新后，应移除这里的数据修改
        new_data = {key: json_data.get(key, None) for key in old.as_dict() if key != 'upload_streamers_id'}
        try:
            if uid:
                us = db.get(UploadStreamers, uid)
                if us is None:
                    return web.HTTPBadRequest(text=f"Upload streamer {uid} not found")
                db.execute(update(LiveStreamers), [{**new_data, "upload_streamers_id": us.id}])
                db.commit()
            else:
                db.execute(update(LiveStreamers), [{**new_data, "upload_streamers_id": None}])
                db.commit()
        except Exception as e:
            return web.HTTPBadRequest(text=str(e))
        config.load_from_db(db)
        context['PluginInfo'].delete(old_url)
        context['PluginInfo'].add(json_data['remark'], json_data['url'])
        # return web.json_response(LiveStreamers.get_dict(id=json_data['id']))
        return web.json_response(db.get(LiveStreamers, json_data['id']).as_dict())


@routes.delete('/v1/streamers/{id}')
async def streamers(request):
    from biliup.app import context
    with SessionLocal() as db:
        org = db.get(LiveStreamers, request.match_info['id'])
        if org is None:
            return web.HTTPNotFound(text="Streamer not found")
        org_url = org.url
        db.delete(org)
        db.commit()
        try:
            config.load_from_db(db)
        finally:
            context['PluginInfo'].delete(org_url)
    return web.HTTPOk()


@routes.get('/v1/upload/streamers')
async def get_streamers(request):
    with SessionLocal() as db:
        res = db.scalars(select(UploadStreamers))
        return web.json_response([resp.as_dict() for resp in res])


@routes.get('/v1/upload/streamers/{id}')
async def streamers_id(request):
    _id = request.match_info['id']
    with SessionLocal() as db:
        res = db.get(UploadStreamers, _id).as_dict()
        return web.json_response(res)


@routes.delete('/v1/upload/streamers/{id}')
async def streamers(request):
    with SessionLocal() as db:
        us = db.get(UploadStreamers, request.match_info['id'])
        if us is None:
            return web.HTTPNotFound(text="Upload streamer not found")
        db.delete(us)
        db.commit()
        config.load_from_db(db)
    return web.HTTPOk()


@routes.post('/v1/upload/streamers')
async def streamers_post(request):
    json_data = await request.json()
    with SessionLocal() as db:
        if "id" in json_data.keys():  # 前端未区分更新和新建, 暂时从后端区分
            db.execute(update(UploadStreamers), [json_data])
            id = json_data["id"]
        else:
            to_save = UploadStreamers(**UploadStreamers.filter_parameters(json_data))
            db.add(to_save)
            db.commit()
            id = to_save.id
        db.commit()
        config.load_from_db(db)
        # res = to_save.as_dict()
        # return web.json_response(res)
        return web.json_response(db.get(UploadStreamers, id).as_dict())


@routes.put('/v1/upload/streamers')
async def streamers_put(request):
    json_data = await request.json()
    with SessionLocal() as db:
        # UploadStreamers.update(**json_data)
        db.execute(update(UploadStreamers), [json_data])
        db.commit()
        config.load_from_db(db)
        # return web.json_response(UploadStreamers.get_dict(id=json_data['id']))
        return web.json_response(db.get(UploadStreamers, json_data['id']).as_dict())


@routes.get('/v1/users')
async def users(request):
    # records = Configuration.select().where(Configuration.key == 'bilibili-cookies')
    res = []
    with SessionLocal() as db:
        records = db.scalars(
            select(Configuration).where(Configuration.key == 'bilibili-cookies'))

        for record in records:
            res.append({
                'id': record.id,
                'name': record.value,
                'value': record.value,
                'platform': record.key,
            })
    return web.json_response(res)


@routes.post('/v1/users')
async def users(request):
    json_data = await request.json()
    to_save = Configuration(key=json_data['platform'], value=json_data['value'])
    with SessionLocal() as db:
        db.add(to_save)
        db.flush()
        resp = {
            'id': to_save.id,
            'name': to_save.value,
            'value': to_save.value,
            'platform': to_save.key,
        }
        db.commit()
        return web.json_response([resp])


@routes.delete('/v1/users/{id}')
async def users(request):
    # Configuration.delete_by_id(request.match_info['id'])
    with SessionLocal() as db:
        configuration = db.get(Configuration, request.match_info['id'])
        db.delete(configuration)
        db.commit()
    return web.HTTPOk()


@routes.get('/v1/configuration')
async def users(request):
    try:
        # record = Configuration.get(Configuration.key == 'config')
        with SessionLocal() as db:
            record = db.execute(
                select(Configuration).where(Configuration.key == 'config')
            ).scalar_one()
            return web.json_response(json.loads(record.value))
    except NoResultFound:
        return web.json_response({})
    except MultipleResultsFound as e:
        return web.json_response({"status": 500, 'error': f"有多个空间配置同时存在: {e}"}, status=500)


@routes.put('/v1/configuration')
async def users(request):
    json_data = await request.json()
    with SessionLocal() as db:
        try:
            # record = Configuration.get(Configuration.key == 'config')
            record = db.execute(
                select(Configuration).where(Configuration.key == 'config')
            ).scalar_one()  # 判断是否只有一行空间配置
            record.value = json.dumps(json_data)
            # db.flush(record)
            resp = record.as_dict()
            # to_save = Configuration(key='config', value=json.dumps(json_data), id=record.id)
        except NoResultFound:  # 如果数据库中没有空间配置行，新建
            to_save = Configuration(key='config', value=json.dumps(json_data))
            # to_save.save()
            db.add(to_save)
            db.commit()
            # db.flush(to_save)
            resp = to_save.as_dict()
        except MultipleResultsFound as e:  # 如果有多行，报错
            return web.json_response({"status": 500, 'error': f"有多个空间配置同时存在: {e}"}, status=500)
        db.commit()
        config.load_from_db(db)
    return web.json_response(resp)


@routes.post('/v1/uploads')
async def m_upload(request):
    from ..uploader import biliup_uploader
    json_data = await request.json()
    json_data['params']['uploader'] = 'stream_gears'
    json_data['params']['name'] = json_data['params']['template_name']
    # json_data['params']['extra_fields'] = "{\"is_only_self\": 1}"
    threading.Thread(target=biliup_uploader, args=(json_data['files'], json_data['params'])).start()
    return web.json_response({'status': 'ok'})


@routes.post('/v1/dump')
async def dump_config(request):
    json_data = await request.json()
    with SessionLocal() as db:
        config.load_from_db(db)
    file = config.dump(json_data['path'])
    return web.json_response({'path': file})


@routes.get('/v1/status')
async def app_status(request):
    from biliup.app import context
    from biliup.config import Config
    from biliup.app import PluginInfo
    from biliup import __version__
    res = {'version': __version__, }
    for key, value in context.items():  # 遍历删除不能被 json 序列化的键值对
        if isinstance(value, Config):
            continue
        if isinstance(value, PluginInfo):
            continue
        res[key] = value
    return web.json_response(res)


def _fetch_pre_archive(path: str):
    cookies = _load_cookies_from_file(path)
    with _bili_client() as bili:
        return bili.tid_archive(cookies)


@routes.get('/bili/archive/pre')
async def pre_archive(request):
    with SessionLocal() as db:
        paths = [
            conf.value
            for conf in db.scalars(
                select(Configuration).where(Configuration.key == 'bilibili-cookies')
            )
        ]
    for path in paths:
        try:
            res = await asyncio.to_thread(_fetch_pre_archive, path)
            if isinstance(res, dict) and res.get('code') == 0:
                return web.json_response(res)
        except Exception:
            logger.exception('pre_archive')
            continue
    return web.json_response({"status": 500, 'error': "无可用 cookie 文件"}, status=500)


def _fetch_seasons(file: str):
    cookies = _load_cookies_from_file(file)
    with _bili_client() as bili:
        requests.utils.add_dict_to_cookiejar(bili._BiliBili__session.cookies, cookies)
        seasons_data = bili.list_seasons()
    result = []
    for item in seasons_data.get('seasons', []):
        season = item.get('season', {})
        sections = item.get('sections', {}).get('sections', [])
        section_id = sections[0]['id'] if sections else None
        result.append({
            'season_id': season.get('id'),
            'title': season.get('title', ''),
            'section_id': section_id,
        })
    return result


@routes.get('/bili/seasons')
async def get_seasons(request):
    """获取用户的合集列表，需要传入 user cookie 文件路径"""
    file = request.query.get('user')
    if not file:
        return web.json_response({"status": 400, 'error': "缺少 user 参数"}, status=400)
    if not _is_registered_cookie_file(file):
        return web.json_response({"status": 403, 'error': "未登记的 Cookie 文件路径"}, status=403)
    try:
        result = await asyncio.to_thread(_fetch_seasons, file)
        return web.json_response(result)
    except FileNotFoundError:
        return web.json_response({"status": 500, 'error': f"{file} 文件不存在"}, status=500)
    except requests.Timeout:
        logger.warning('get_seasons timeout for user=%s', file)
        return web.json_response({"status": 504, 'error': "请求上游超时"}, status=504)
    except Exception as e:
        logger.exception('get_seasons')
        return web.json_response({"status": 500, 'error': str(e)}, status=500)


def _fetch_myinfo(file: str):
    cookies = _load_cookies_from_file(file)
    with _bili_client() as bili:
        return bili.myinfo(cookies)


@routes.get('/bili/space/myinfo')
async def myinfo(request):
    file = request.query.get('user')
    if not file:
        return web.json_response({"status": 400, 'error': "缺少 user 参数"}, status=400)
    if not _is_registered_cookie_file(file):
        return web.json_response({"status": 403, 'error': "未登记的 Cookie 文件路径"}, status=403)
    try:
        res = await asyncio.to_thread(_fetch_myinfo, file)
        return web.json_response(res)
    except FileNotFoundError:
        return web.json_response({"status": 500, 'error': f"{file} 文件不存在"}, status=500)
    except requests.Timeout:
        logger.warning('myinfo timeout for user=%s', file)
        return web.json_response({"status": 504, 'error': "请求上游超时"}, status=504)
    except Exception as e:
        logger.exception('myinfo')
        return web.json_response({"status": 500, 'error': str(e)}, status=500)


@routes.get('/bili/proxy')
async def proxy(request):
    raw_url = request.query.get('url')
    if not raw_url:
        return web.HTTPBadRequest(reason="Missing url parameter")
    url = raw_url
    parsed_url = urlparse(url)

    if (
        parsed_url.scheme not in ('http', 'https')
        or not parsed_url.hostname
        or not parsed_url.hostname.lower().endswith('.hdslb.com')
        or parsed_url.username
        or parsed_url.password
    ):
        return web.HTTPForbidden(reason="Access to the requested domain or scheme is forbidden")

    timeout = ClientTimeout(total=15)
    async with ClientSession(timeout=timeout) as session:
        try:
            async with session.get(url, allow_redirects=False) as response:
                content_length = response.headers.get('Content-Length')
                if content_length is not None:
                    try:
                        if int(content_length) > MAX_PROXY_BODY_SIZE:
                            return web.HTTPRequestEntityTooLarge(
                                max_size=MAX_PROXY_BODY_SIZE,
                                actual_size=int(content_length),
                            )
                    except ValueError:
                        pass

                chunks = bytearray()
                async for chunk in response.content.iter_chunked(64 * 1024):
                    chunks.extend(chunk)
                    if len(chunks) > MAX_PROXY_BODY_SIZE:
                        return web.HTTPRequestEntityTooLarge(
                            max_size=MAX_PROXY_BODY_SIZE,
                            actual_size=len(chunks),
                        )

                resp_headers = {}
                content_type = response.headers.get('Content-Type')
                if content_type:
                    resp_headers['Content-Type'] = content_type
                return web.Response(body=bytes(chunks), status=response.status, headers=resp_headers or None)
        except asyncio.TimeoutError:
            return web.HTTPGatewayTimeout(reason="Upstream request timed out")
        except Exception as e:
            return web.HTTPBadRequest(reason=str(e))


def read_last_n_lines(file_path, n=50):
    """高效读取文件最后n行，避免一次性读取整个文件"""
    with open(file_path, 'rb') as f:
        # 设置初始偏移量和缓冲区大小
        file_size = f.seek(0, os.SEEK_END)
        block_size = 1024
        data = b''
        lines = []

        while file_size > 0 and len(lines) <= n:
            # 计算要读取的字节数
            read_size = min(block_size, file_size)
            f.seek(file_size - read_size)
            buffer = f.read(read_size)
            data = buffer + data
            file_size -= read_size
            lines = data.splitlines()

        # 只保留最后 n 行，并解码为字符串列表
        return [line.decode('utf-8', errors='replace') for line in lines[-n:]]


WS_LOG_MAX_CHUNK_BYTES = 1024 * 1024  # 单次增量读取上限（1MB），避免超长无换行日志耗尽内存


def _utf8_safe_prefix_len(data: bytes) -> int:
    """返回不包含末尾被截断多字节 UTF-8 序列的前缀长度（最多回退 3 字节）。"""
    length = len(data)
    if length == 0:
        return 0
    start = length - 1
    while start >= max(0, length - 4) and (data[start] & 0xC0) == 0x80:
        start -= 1
    if start < 0:
        return length
    lead = data[start]
    if lead < 0x80:
        expected = 1
    elif (lead & 0xE0) == 0xC0:
        expected = 2
    elif (lead & 0xF0) == 0xE0:
        expected = 3
    elif (lead & 0xF8) == 0xF0:
        expected = 4
    else:
        return length
    if length - start < expected:
        return start
    return length


def _read_last_n_complete_lines(file_path, n=50):
    """单次二进制读取最后 n 行完整日志（仅消费到最后一个换行符边界），
    返回 (lines, consumed_offset, observed_size, file_id)。"""
    with open(file_path, 'rb') as f:
        st = os.fstat(f.fileno())
        observed_size = st.st_size
        file_id = (st.st_dev, st.st_ino)
        if observed_size == 0:
            return [], 0, 0, file_id

        block_size = 4096
        pos = observed_size
        consumed_offset = 0
        while pos > 0:
            read_size = min(block_size, pos)
            pos -= read_size
            f.seek(pos)
            chunk = f.read(read_size)
            nl_idx = chunk.rfind(b'\n')
            if nl_idx != -1:
                consumed_offset = pos + nl_idx + 1
                break

        if consumed_offset == 0:
            return [], 0, observed_size, file_id

        read_pos = consumed_offset
        data = b''
        lines = []
        while read_pos > 0 and len(lines) <= n:
            read_size = min(block_size, read_pos)
            read_pos -= read_size
            f.seek(read_pos)
            data = f.read(read_size) + data
            lines = data.splitlines()

        return [line.decode('utf-8', errors='replace') for line in lines[-n:]], consumed_offset, observed_size, file_id


@routes.get('/v1/static-token')
async def get_static_token(request):
    """签发仅限单文件短时有效的静态资源访问令牌（用于视频播放）"""
    filename = request.query.get('file', '')
    if not filename or '/' in filename or '\\' in filename or filename in ('.', '..') or '\x00' in filename:
        return web.json_response({"status": 400, 'error': "非法文件名"}, status=400)
    if not request.app.get('password'):
        return web.json_response({"token": None})
    token = generate_static_token(request.app['static_token_secret'], filename)
    return web.json_response({"token": token})


@routes.get('/ws/logs')
async def websocket_logs(request):
    app_password = request.app['password'] if 'password' in request.app else GLOBAL_PASSWORD
    if app_password:
        auth_value = request.query.get('authorization') or request.headers.get('Authorization', '')
        if not auth_value or not check_access({'biliup': app_password}, auth_value):
            return web.HTTPUnauthorized(reason="Unauthorized")

    ws = web.WebSocketResponse()
    try:
        await ws.prepare(request)
    except ConnectionResetError as e:
        logger.warning(e)
        return ws

    # 获取请求参数，默认为 ds_update.log
    file_param = request.query.get('file', 'ds_update.log')

    # 安全检查：限制只能查看特定的日志文件
    allowed_files = ['ds_update.log', 'download.log', 'upload.log']
    if file_param not in allowed_files:
        await ws.send_str(f"不允许访问请求的文件: {file_param}")
        await ws.close()
        return ws

    log_file = file_param

    # 发送初始内容（最后50行完整日志）
    try:
        last_lines, file_size, last_observed_size, file_id = _read_last_n_complete_lines(log_file, 50)
        for line in last_lines:
            await ws.send_str(line)
    except FileNotFoundError:
        await ws.send_str(f"日志文件 {log_file} 不存在")
        await ws.close()
        return ws
    except Exception as e:
        await ws.send_str(f"读取日志文件错误: {str(e)}")
        logger.exception(f"读取日志文件错误: {str(e)}")
        await ws.close()
        return ws

    # 监控文件变化并发送新内容
    try:
        while True:
            try:
                # 使用带超时的接收消息，可以检测客户端是否发送关闭请求
                msg = await asyncio.wait_for(ws.receive(), timeout=0.5)

                # 如果客户端发送关闭请求，正常响应并退出循环
                if msg.type == web.WSMsgType.CLOSE:
                    await ws.close()
                    break
                elif msg.type == web.WSMsgType.ERROR:
                    logger.error(f"WebSocket连接错误: {ws.exception()}")
                    break
            except asyncio.TimeoutError:
                # 超时只是表示客户端没有发送消息，不是错误
                pass

            # 检查连接是否关闭
            if ws.closed:
                logger.debug("WebSocket连接已关闭")
                break

            # 检查文件是否存在并获取元信息
            try:
                st = os.stat(log_file)
            except FileNotFoundError:
                await ws.send_str(f"日志文件 {log_file} 不再存在")
                break

            current_size = st.st_size
            current_id = (st.st_dev, st.st_ino)
            file_replaced = file_id[1] != 0 and current_id != file_id

            # 如果文件被截断或发生轮转替换，重新读取最后50行完整日志
            if file_replaced or current_size < last_observed_size or current_size < file_size:
                await ws.send_str("日志文件被截断，重新加载...")
                last_lines, file_size, last_observed_size, file_id = _read_last_n_complete_lines(log_file, 50)
                for line in last_lines:
                    await ws.send_str(line)
                continue

            last_observed_size = current_size

            # 如果文件增长，按二进制逐块增量读取到最后一个换行符边界，避免多字节 UTF-8 半截写入报错
            if current_size > file_size:
                lines_to_send = []
                with open(log_file, 'rb') as f:
                    f.seek(file_size)
                    raw_data = f.read(WS_LOG_MAX_CHUNK_BYTES)
                    last_newline = raw_data.rfind(b'\n')
                    if last_newline != -1:
                        consume_len = last_newline + 1
                        f.seek(file_size + consume_len)
                        file_size = f.tell()
                        complete_bytes = raw_data[:consume_len]
                        lines_to_send = complete_bytes.decode('utf-8', errors='replace').splitlines()
                    elif len(raw_data) >= WS_LOG_MAX_CHUNK_BYTES:
                        safe_len = _utf8_safe_prefix_len(raw_data)
                        if safe_len == 0:
                            safe_len = len(raw_data)
                        f.seek(file_size + safe_len)
                        file_size = f.tell()
                        lines_to_send = [raw_data[:safe_len].decode('utf-8', errors='replace')]
                for line in lines_to_send:
                    await ws.send_str(line)
    except Exception as e:
        if not ws.closed:
            await ws.send_str(f"监控日志文件错误: {str(e)}")
            logger.exception(f"websocket_logs错误: {str(e)}")
    finally:
        # 确保在所有情况下连接都被正确关闭
        if not ws.closed:
            await ws.close()
        logger.debug(f"WebSocket日志会话结束: {file_param}")

    return ws


def find_all_folders(directory):
    result = []
    for foldername, subfolders, filenames in os.walk(directory):
        for subfolder in subfolders:
            result.append(os.path.relpath(os.path.join(foldername, subfolder), directory))
    return result


def create_app(password=None, static_dir=None):
    app = web.Application()
    app.add_routes(routes)
    app.add_routes([web.static('/static', '.', show_index=False)])
    if static_dir:
        app.add_routes([web.static('/', static_dir, show_index=False)])
    else:
        public_dir = pathlib.Path(files('biliup.web').joinpath('public'))
        if public_dir.exists():
            res = []
            for fdir in public_dir.glob('**/*.html'):
                fname = fdir.relative_to(public_dir)

                def _copy(fname):
                    async def static_view(request):
                        return web.FileResponse(public_dir.joinpath(fname))

                    return static_view

                res.append(web.get('/' + str(fname.with_suffix('')).replace('\\', '/'), _copy(fname)))
            res.append(web.static('/', public_dir))
            app.add_routes(res)

    app['password'] = password or None
    app['static_token_secret'] = secrets.token_bytes(32)
    if password:
        # 保护API及静态文件下载路径，不保护前端页面路由；/static/ 允许凭单文件短时签名 ?token= 访问
        app.middlewares.append(
            basic_auth_middleware(
                ('/api/', '/url-status', '/v1/', '/bili/', '/static/'),
                {'biliup': password},
                token_validator=_validate_static_token,
            )
        )

    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            allow_methods="*",
            expose_headers="*",
            allow_headers="*"
        )
    })

    for route in list(app.router.routes()):
        cors.add(route)

    setup_middlewares(app)

    async def _on_cleanup(_app):
        _shutdown_qr_pool()

    app.on_cleanup.append(_on_cleanup)
    return app


async def service(args):
    password = getattr(args, 'password', None)
    set_global_password(password or None)
    app = create_app(password=password, static_dir=getattr(args, 'static_dir', None))

    if args.no_access_log:
        runner = web.AppRunner(app, access_log=None)
    else:
        runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=args.host, port=args.port)
    await site.start()
    log_startup(args.host, args.port)
    return runner


async def handle_404(request):
    not_found_path = pathlib.Path(files('biliup.web').joinpath('public').joinpath('404.html'))
    if not_found_path.is_file():
        return web.FileResponse(not_found_path, status=404)
    return web.Response(status=404, text='Not Found')


async def handle_500(request):
    return web.json_response({"status": 500, 'error': "Error handling request"}, status=500)


def create_error_middleware(overrides):
    @web.middleware
    async def error_middleware(request, handler):
        try:
            return await handler(request)
        except web.HTTPException as ex:
            override = overrides.get(ex.status)
            if override:
                return await override(request)

            raise
        except Exception:
            request.protocol.logger.exception("Error handling request")
            return await overrides[500](request)

    return error_middleware


def setup_middlewares(app):
    @web.middleware
    async def file_type_check_middleware(request, handler):
        allowed_extensions = {'.mp4', '.flv', '.3gp', '.webm', '.mkv', '.ts', '.xml', '.log'}

        if request.path.startswith('/static/'):
            filename = request.match_info.get('filename')
            if filename:
                extension = '.' + filename.split('.')[-1]
                if extension not in allowed_extensions:
                    return web.HTTPForbidden(reason="File type not allowed")
        return await handler(request)

    error_middleware = create_error_middleware({
        404: handle_404,
        500: handle_500,
    })
    app.middlewares.append(error_middleware)
    app.middlewares.append(file_type_check_middleware)


def log_startup(host, port) -> None:
    """Show information about the address when starting the server."""
    messages = ['WebUI 已启动，请浏览器访问']
    host = host if host else "0.0.0.0"
    scheme = "http"
    display_hostname = host

    if host in {"0.0.0.0", "::"}:
        messages.append(f" * Running on all addresses ({host})")
        if host == "0.0.0.0":
            localhost = "127.0.0.1"
            display_hostname = get_interface_ip(socket.AF_INET)
        else:
            localhost = "[::1]"
            display_hostname = get_interface_ip(socket.AF_INET6)

        messages.append(f" * Running on {scheme}://{localhost}:{port}")

    if ":" in display_hostname:
        display_hostname = f"[{display_hostname}]"

    messages.append(f" * Running on {scheme}://{display_hostname}:{port}")

    print("\n".join(messages))


def get_interface_ip(family: socket.AddressFamily) -> str:
    """Get the IP address of an external interface. Used when binding to
    0.0.0.0 or ::1 to show a more useful URL.

    :meta private:
    """
    # arbitrary private address
    host = "fd31:f903:5ab5:1::1" if family == socket.AF_INET6 else "10.253.155.219"

    with socket.socket(family, socket.SOCK_DGRAM) as s:
        try:
            s.connect((host, 58162))
        except OSError:
            return "::1" if family == socket.AF_INET6 else "127.0.0.1"

        return s.getsockname()[0]  # type: ignore
