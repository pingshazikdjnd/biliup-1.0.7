import time
import json
import re
import asyncio
from urllib.parse import urlparse

from biliup.common.util import client
from biliup.config import config
from . import match1, logger, wbi
from biliup.Danmaku import DanmakuClient
from ..engine.decorators import Plugin
from ..engine.download import DownloadBase


OFFICIAL_API = "https://api.live.bilibili.com"
STREAM_NAME_REGEXP = r"/live-bvc/\d+/(live_[^/\.]+)"
WBI_WEB_LOCATION = "444.8"
_BILIUP_PENALTY_STATE = {}  # 新增：全局状态机字典，用于记录各直播间的惩罚等级与拉流时间

@Plugin.download(regexp=r'https?://(b23\.tv|live\.bilibili\.com)')
class Bililive(DownloadBase):
    def __init__(self, fname, url, suffix='flv'):
        super().__init__(fname, url, suffix)
        self.live_start_time = 0
        self.bilibili_danmaku = config.get('bilibili_danmaku', False)
        self.bilibili_danmaku_detail = config.get('bilibili_danmaku_detail', False)
        self.bilibili_danmaku_raw = config.get('bilibili_danmaku_raw', False)
        self.__real_room_id = None
        self.__login_mid = 0
        self.__anchor_mid = 0
        user_conf = config.get('user') or {}
        self.bili_cookie = user_conf.get('bili_cookie')
        self.bili_cookie_file = user_conf.get('bili_cookie_file')
        self.bili_qn = int(config.get('bili_qn') or 25000)
        self.bili_protocol = config.get('bili_protocol') or 'stream'
        self.bili_cdn = config.get('bili_cdn') or []
        hls_timeout = config.get('bili_hls_transcode_timeout', 60)
        self.bili_hls_transcode_timeout = 60 if hls_timeout is None else hls_timeout
        self.bili_liveapi = config.get('bili_liveapi') or OFFICIAL_API
        self.bili_fallback_api = config.get('bili_fallback_api') or OFFICIAL_API
        self._custom_bili_api_list = None
        self.bili_anonymous_origin = config.get('bili_anonymous_origin', False)
        self.bili_cdn_fallback = config.get('bili_cdn_fallback', False)
        self.bili_wait_transcode = config.get('bili_wait_transcode', False)
        try:
            self.bili_wait_transcode_timeout = max(0, int(config.get('bili_wait_transcode_timeout', 120)))
        except (ValueError, TypeError):
            self.bili_wait_transcode_timeout = 120

    @property
    def bili_hls_timeout(self):
        if 'bili_hls_timeout' in self.__dict__:
            val = self.__dict__['bili_hls_timeout']
            return 60 if val is None else val
        return 60 if self.bili_hls_transcode_timeout is None else self.bili_hls_transcode_timeout

    @bili_hls_timeout.setter
    def bili_hls_timeout(self, value):
        self.bili_hls_transcode_timeout = value
        self.__dict__.pop('bili_hls_timeout', None)

    @property
    def bili_api_list(self):
        if 'bili_api_list' in self.__dict__ and self.__dict__['bili_api_list'] is not None:
            return list(self.__dict__['bili_api_list'])
        if self._custom_bili_api_list is not None:
            return list(self._custom_bili_api_list)
        return [
            normalize_url(self.bili_liveapi or OFFICIAL_API).rstrip('/'),
            normalize_url(self.bili_fallback_api or OFFICIAL_API).rstrip('/'),
        ]

    @bili_api_list.setter
    def bili_api_list(self, value):
        self._custom_bili_api_list = None if value is None else list(value)
        self.__dict__.pop('bili_api_list', None)

    async def acheck_stream(self, is_check=False):
        try:
            self.bili_qn = int(self.bili_qn) if self.bili_qn is not None else 25000
        except (ValueError, TypeError):
            self.bili_qn = 25000
        try:
            self.bili_wait_transcode_timeout = max(0, int(self.bili_wait_transcode_timeout))
        except (ValueError, TypeError):
            self.bili_wait_transcode_timeout = 120

        if "b23.tv" in self.url:
            try:
                resp = await client.get(self.url, follow_redirects=False)
                if resp.status_code not in {301, 302}:
                    raise Exception("不支持的链接")
                url = str(resp.next_request.url)
                if "live.bilibili" not in url:
                    raise Exception("不支持的链接")
                self.url = url
            except Exception as e:
                logger.error(f"{self.plugin_msg}: {e}")
                return False

        room_id: str = match1(self.url, r'bilibili.com/(\d+)')
        if self.bili_cookie:
            self.fake_headers['cookie'] = self.bili_cookie
        if self.bili_cookie_file:
            try:
                with open(self.bili_cookie_file, encoding='utf-8') as stream:
                    cookies = json.load(stream)["cookie_info"]["cookies"]
                    cookies_str = ''
                    for i in cookies:
                        cookies_str += f"{i['name']}={i['value']};"
                    self.fake_headers['cookie'] = cookies_str
            except Exception:
                logger.exception("load_cookies error")
        self.fake_headers['referer'] = self.url

        if int(time.time()) - wbi.last_update >= wbi.UPDATE_INTERVAL:
            await self.update_wbi()

        # 获取直播状态与房间标题
        try:
            params = {
                "room_id": room_id,
                "web_location": WBI_WEB_LOCATION,
            }
            wbi.sign(params)
            room_info = await client.get(
                f"{OFFICIAL_API}/xlive/web-room/v1/index/getInfoByRoom",
                params=params,
                headers=self.fake_headers)
            room_info.raise_for_status()
            room_info = room_info.json()
        except Exception as e:
            logger.error(f"{self.plugin_msg}: {e}", exc_info=True)
            return False
        if room_info['code'] != 0:
            logger.error(f"{self.plugin_msg}: {room_info}")
            return False
        else:
            room_info = room_info['data']
        if room_info['room_info']['live_status'] != 1:
            logger.debug(f"{self.plugin_msg}: 未开播")
            self.raw_stream_url = None
            return False

        self.live_cover_url = room_info['room_info']['cover']
        self.room_title = room_info['room_info']['title']
        self.__real_room_id = room_info['room_info']['room_id']
        self.__anchor_mid = room_info['room_info']['uid']
        live_start_time = room_info['room_info']['live_start_time']
        special_type = room_info['room_info']['special_type'] # 0: 公开直播, 1: 付费直播, 199: 纯净页面
        if live_start_time > self.live_start_time:
            self.live_start_time = live_start_time
            is_new_live = True
        else:
            is_new_live = False

        if is_check:
            return True
        else:
            self.__login_mid = await self.check_login_status()

        # 复用原画 m3u8 流
        if  self.raw_stream_url is not None \
            and ".m3u8" in self.raw_stream_url \
            and self.bili_qn >= 10000 \
            and not is_new_live:
            url = await self.acheck_url_healthy(self.raw_stream_url)
            if url is not None:
                logger.debug(f"{self.plugin_msg}: 复用 {url}")
                return True
            else:
                self.raw_stream_url = None

        # ================= 新增：状态机分级惩罚与600秒自愈窗口判定 =================
        room_id_str = str(self.__real_room_id)
        state = _BILIUP_PENALTY_STATE.setdefault(room_id_str, {'level': 0, 'last_time': 0})
        current_time = time.time()
        last_time = state['last_time']

        if last_time > 0:
            if (current_time - last_time) >= 600:
                if state['level'] != 0:
                    logger.info(f"{self.plugin_msg}: 录制已稳定超过10分钟，网络自愈，惩罚等级重置为 Level 0 (正常模式)")
                state['level'] = 0
            else:
                if state['level'] == 0:
                    state['level'] = 1
                    logger.warning(f"{self.plugin_msg}: 距离上次拉流不足10分钟即断开！升级至 Level 1 惩罚期，优先使用回退 API。")
                elif state['level'] == 1:
                    state['level'] = 2
                    logger.warning(f"{self.plugin_msg}: 连回退 API 也出现快速断流！升级至 Level 2 探测期，强制换回官方 API 以打破死锁。")
                elif state['level'] == 2:
                    state['level'] = 0
                    logger.warning(f"{self.plugin_msg}: 探测期依然快速断流，重置惩罚等级回 Level 0 重新循环。")
        # =========================================================================

        # --- 等待转码流逻辑 (替换原第 154 行) ---
        start_wait_time = time.time()
        empty_retry_count = 0
        transcode_qn_whitelist = {80, 150, 250, 400, 401}

        while True:
            stream_urls = await self.aget_stream(self.bili_qn, self.bili_protocol, special_type)

            # 1. 空流处理：仅在等待转码功能开启时做有限重试，否则直接break交给原有逻辑
            if not stream_urls:
                if self.bili_wait_transcode and is_new_live:
                    empty_retry_count += 1
                    if empty_retry_count <= 3 and (time.time() - start_wait_time) < self.bili_wait_transcode_timeout:
                        logger.warning(f"{self.plugin_msg}: 获取流为空 (可能网络抖动)，将在 15 秒后重试 ({empty_retry_count}/3)...")
                        await asyncio.sleep(15)
                        continue
                break  # 功能关闭或重试耗尽，交给原有 155-165 行处理

            # 2. 正常流：重置抖动计数器
            empty_retry_count = 0

            # 3. 等待转码判定（四个条件全部满足才等待）
            if (self.bili_wait_transcode
                    and is_new_live
                    and self.bili_qn in transcode_qn_whitelist
                    and self.bili_qn not in stream_urls):

                elapsed = time.time() - start_wait_time
                if elapsed < self.bili_wait_transcode_timeout:
                    logger.warning(
                        f"{self.plugin_msg}: 期望转码画质 {self.bili_qn} 暂未生成 (转码中)，已等待 {int(elapsed)}秒，"
                        f"将在 15 秒后重试..."
                    )
                    await asyncio.sleep(15)
                    continue
                else:
                    logger.warning(
                        f"{self.plugin_msg}: 等待转码画质 {self.bili_qn} 超时 ({self.bili_wait_transcode_timeout}秒)，"
                        f"将直接以当前可用最高画质开始录制。"
                    )
            break
        # --- 等待转码流逻辑结束 ---
        if not stream_urls:
            if self.bili_protocol == 'hls_fmp4':
                try:
                    hls_timeout = int(self.bili_hls_timeout)
                except (ValueError, TypeError):
                    hls_timeout = 60
                if int(time.time()) - live_start_time <= hls_timeout:
                    logger.warning(f"{self.plugin_msg}: 暂未提供 hls_fmp4 流，等待下一次检测")
                    return False
                else:
                    # 回退首个可用格式
                    stream_urls = await self.aget_stream(self.bili_qn, 'stream', special_type)
                    if not stream_urls:
                        logger.error(f"{self.plugin_msg}: hls_fmp4 回退获取 stream 流失败")
                        return False
            else:
                logger.error(f"{self.plugin_msg}: 获取{self.bili_protocol}流失败")
                return False

        target_quality_stream = stream_urls.get(
            self.bili_qn, next(iter(stream_urls.values()))
        )
        stream_url = {}
        if self.bili_cdn is not None:
            for cdn in self.bili_cdn:
                stream_info = target_quality_stream.get(cdn)
                if stream_info is not None:
                    current_cdn = cdn
                    stream_url = stream_info['url']
                    break
        if not stream_url:
            current_cdn, stream_info = next(iter(target_quality_stream.items()))
            stream_url = stream_info['url']
            logger.debug(f"{self.plugin_msg}: 使用 {current_cdn} 流")

        self.raw_stream_url = f"{stream_url['host']}{stream_url['base_url']}{stream_url['extra']}"

        # 回退
        if self.bili_cdn_fallback:
            __url = await self.acheck_url_healthy(self.raw_stream_url)
            if __url is None:
                for cdn, stream_info in target_quality_stream.items():
                    stream_url = stream_info['url']
                    __fallback_url = f"{stream_url['host']}{stream_url['base_url']}{stream_url['extra']}"
                    try:
                        __url = await self.acheck_url_healthy(__fallback_url)
                        if __url is not None:
                            self.raw_stream_url = __url
                            logger.info(f"{self.plugin_msg}: cdn_fallback 回退到 {cdn} - {__fallback_url}")
                            break
                    except Exception as e:
                        logger.error(f"{self.plugin_msg}: cdn_fallback {e} - {__fallback_url}")
                        continue
                else:
                    logger.error(f"{self.plugin_msg}: 所有 cdn 均不可用")
                    self.raw_stream_url = None
                    return False
            else:
                self.raw_stream_url = __url

        # ================= 新增：成功拉流全局打卡更新时间戳 =================
        _BILIUP_PENALTY_STATE[str(self.__real_room_id)]['last_time'] = time.time()
        # ====================================================================

        return True

    def danmaku_init(self):
        if self.bilibili_danmaku:
            self.danmaku = DanmakuClient(
                self.url, self.gen_download_filename(), {
                    'room_id': self.__real_room_id,
                    'cookie': self.fake_headers.get('cookie', ''),
                    'detail': self.bilibili_danmaku_detail,
                    'raw': self.bilibili_danmaku_raw,
                    'uid': self.__login_mid
                }
            )


    async def get_play_info(self, api: str, qn: int = 10000) -> dict:
        full_url = f"{api}/xlive/web-room/v2/index/getRoomPlayInfo"
        try:
            params = {
                'room_id': str(self.__real_room_id),
                # 'no_playurl': '0',
                # 'mask': '1',
                'qn': str(qn),
                'platform': 'html5',  # 平台名称，web, html5, android, ios
                'protocol': '0,1',  # 流协议，0: http_stream(flv), 1: http_hls
                'format': '0,1,2',  # 编码格式，0: flv, 1: ts, 2: fmp4
                'codec': '0',  # 编码器，0: avc, 1: hevc, 2: av1
                # 'ptype': '8', # P2P配置，-1: disable, 8: WebRTC, 8192: MisakaTunnel
                'dolby': '5', # 杜比格式，5: 杜比音频
                # 'panorama': '1', # 全景(不支持 html5)
                # 'hdr_type': '0,1', # HDR类型(不支持 html5)，0: SDR, 1: PQ
                # 'req_reason': '0', # 请求原因，0: Normal, 1: PlayError
                # 'http': '1', # 优先 http 协议
                'web_location': WBI_WEB_LOCATION,
            }
            wbi.sign(params)
            api_res = await client.get(
                full_url, params=params, headers=self.fake_headers
            )
            api_res = json.loads(api_res.text)
            if api_res['code'] != 0:
                logger.error(f"{self.plugin_msg}: {api} 返回内容错误: {api_res}")
                return {}
            return api_res['data']
        except json.JSONDecodeError:
            logger.error(f"{self.plugin_msg}: {api} 返回内容错误: {api_res.text}")
        except Exception as e:
            logger.error(f"{self.plugin_msg}: {api} 获取 play_info 失败 -> {e}", exc_info=True)
        return {}

    async def get_master_m3u8(self, api: str) -> dict:
        full_url = f"{api}/xlive/play-gateway/master/url"
        params = {
            "cid": self.__real_room_id,
            "mid": self.__login_mid or self.__anchor_mid,
            "pt": "web", # platform
            "p2p_type": "-1",
            "net": 0,
            "free_type": 0,
            "build": 0,
            "feature": 2,
            "qn": self.bili_qn,
            "drm_type": 0,
            "codec": "0,1",
        }
        try:
            m3u8_res = await client.get(
                full_url, params=params, headers=self.fake_headers
            )
            if m3u8_res.status_code == 200 and m3u8_res.text.startswith("#EXTM3U"):
                return self.parse_master_m3u8(m3u8_res.text)
        except Exception as e:
            logger.error(f"{self.plugin_msg}: {api} 获取 m3u8 失败 -> {e}", exc_info=True)
        return {}

    async def aget_stream(self, qn: int = 10000, protocol: str = 'stream', special_type: int = 0) -> dict:
        """
        :param qn: 目标画质
        :param protocol: 流协议
        :param special_type: 特殊直播类型
        :return: 流信息
        """
        stream_urls = {}

        # ================= 新增：根据状态机分级动态编排 API 路由优先级 =================
        room_id_str = str(self.__real_room_id)
        level = _BILIUP_PENALTY_STATE.get(room_id_str, {}).get('level', 0)
        
        api_list = list(self.bili_api_list)
        if level == 1:
            api_list.reverse()
            logger.info(f"{self.plugin_msg}: 处于 Level 1 惩罚期，优先编排回退 API 链路")
        elif level == 2:
            logger.info(f"{self.plugin_msg}: 处于 Level 2 探测期，强行激活官方 API 重新探测")
        # =============================================================================

        for api in api_list:  # 修改：遍历重排后的 api_list
            play_info = await self.get_play_info(api, qn)
            if not play_info or check_areablock(play_info):
                # logger.error(f"{self.plugin_msg}: {api} 返回内容错误: {play_info}")
                continue
            streams = play_info['playurl_info']['playurl']['stream']
            if protocol == 'hls_fmp4':
                if self.bili_anonymous_origin:
                    if special_type in play_info['all_special_types'] and not self.__login_mid:
                        logger.warn(f"{self.plugin_msg}: 特殊直播{special_type}")
                    else:
                        stream_urls = await self.get_master_m3u8(api)
                        if stream_urls:
                            break
                # 处理 API 信息
                stream = streams[1] if len(streams) > 1 else streams[0]
                for format in stream['format']:
                    if format['format_name'] == 'fmp4':
                        stream_urls = self.parse_stream_url(format['codec'][0])
                        # fmp4 可能没有原画
                        if qn in {10000, 25000} and qn not in stream_urls.keys():
                            stream_urls = {}
            else:
                stream_urls = self.parse_stream_url(streams[0]['format'][0]['codec'][0])
            if stream_urls:
                break
        # 空字典照常返回，重试交给上层方法处理
        return stream_urls

    async def get_user_status(self) -> dict:
        try:
            nav_res = await client.get(
                'https://api.bilibili.com/x/web-interface/nav',
                headers=self.fake_headers
            )
            nav_res.raise_for_status()
            nav_res = json.loads(nav_res.text)
            if (
                nav_res['code'] == 0 or
                (nav_res['code'] == -101 and nav_res['message'] == '账号未登录')
            ):
                return nav_res['data']
            logger.error(f"{self.plugin_msg}: 获取 nav 失败-{nav_res}")
        except:
            logger.error(f"{self.plugin_msg}: 获取 nav 失败", exc_info=True)
        return {}

    async def update_wbi(self):
        def _extract_key(url):
            if not url:
                return None
            slash = url.rfind('/')
            dot = url.find('.', slash)
            if slash == -1 or dot == -1:
                return None
            return url[slash + 1:dot]
        data = await self.get_user_status()
        wbi_key = data.get('wbi_img')
        if wbi_key:
            img_key = _extract_key(wbi_key.get('img_url'))
            sub_key = _extract_key(wbi_key.get('sub_url'))
            if img_key and sub_key:
                wbi.update_key(img_key, sub_key)
            else:
                logger.warning(f"img_key-{img_key}, sub_key-{sub_key}")
        else:
            logger.warning(f"Can not get wbi key by {data}")

    async def check_login_status(self) -> int:
        """
        检查B站登录状态
        :return: 当前登录用户 mid
        """
        try:
            data = await self.get_user_status()
            if data.get('isLogin'):
                logger.info(f"用户名：{data['uname']}, mid: {data['mid']}")
                return data['mid']
            else:
                logger.warning(f"{self.plugin_msg}: 未登录，或将只能录制到最低画质。")
        except Exception as e:
            logger.error(f"{self.plugin_msg}: 登录态校验失败 {e}")
        return 0

    def parse_stream_url(self, *args) -> dict:
        suffix_regexp = r'suffix=([^&]+)'
        if isinstance(args[0], str):
            url = args[0]
            parsed = urlparse(url)
            scheme = parsed.scheme or 'https'
            host = f"{scheme}://{parsed.netloc}" if parsed.netloc else ''
            base_url = f"{parsed.path}?" if parsed.query else parsed.path
            stream_url = {
                'host': host,
                'base_url': base_url,
                'extra': parsed.query,
            }
            return {
                'url': stream_url,
                'stream_name': match1(url, STREAM_NAME_REGEXP),
                'suffix': match1(url, suffix_regexp)
            }
        elif isinstance(args[0], dict):
            streams = {}
            current_qn = args[0]['current_qn']
            streams.setdefault(current_qn, {})
            base_url = args[0]['base_url']
            for info in args[0]['url_info']:
                cdn_name = match1(info['extra'], r'cdn=([^&]+)')
                stream_url = {
                    'host': info['host'],
                    'base_url': base_url,
                    'extra': info['extra']
                }
                streams[current_qn].setdefault(cdn_name, {
                    'url': stream_url,
                    'stream_name': match1(base_url, STREAM_NAME_REGEXP),
                    'suffix': match1(info['extra'], suffix_regexp)
                })
            return streams


    def parse_master_m3u8(self, m3u8_content: str) -> dict:
        """
        Returns:
            {
                "qn值": {
                    "cdn名称": {
                        "url": parsed_stream_url,
                        "stream_name": "流名称",
                        "suffix": "二压后缀"
                    }
                }
            }
        """
        lines = m3u8_content.strip().splitlines()
        current_qn = None
        result = {}

        if not lines or not lines[0].startswith('#EXTM3U'):
            raise ValueError('Invalid m3u8 file')

        for line in lines:
            if line.startswith('#EXT-X-STREAM-INF:'):
                codec = match1(line, r'CODECS="([^"]+)"')
                qn_str = match1(line, r'BILI-QN=(\d+)')
                current_qn = int(qn_str) if qn_str else None

                if codec and current_qn:
                    if 'avc' in codec.lower():
                        result.setdefault(current_qn, {})
                    else:
                        current_qn = None

            elif line.startswith('http') and current_qn is not None:
                cdn_name = match1(line, r'cdn=([^&]+)')
                if cdn_name:
                    result[current_qn].setdefault(cdn_name, self.parse_stream_url(line))

        return dict(sorted(result.items(), key=lambda x: int(x[0]), reverse=True))

# Copy from room-player.js
def check_areablock(data):
    '''
    :return: True if area block
    '''
    if not data['playurl_info']['playurl']:
        logger.error('Sorry, bilibili is currently not available in your country according to copyright restrictions.')
        logger.error('非常抱歉，根据版权方要求，您所在的地区无法观看本直播')
        return True
    return False

def normalize_url(url: str) -> str:
    return url if url.startswith(('http://', 'https://')) else 'http://' + url
