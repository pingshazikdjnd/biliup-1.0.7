import multiprocessing as mp
import time

import stream_gears

from typing import List, Optional, Union
from . import SubmitOption
from ..engine import Plugin
from ..engine.upload import UploadBase, logger


@Plugin.upload(platform="biliup-rs")
class BiliWeb(UploadBase):
    def __init__(
            self, principal, data, submit_api: Optional[Union[SubmitOption, str]] = None,
            copyright=2, postprocessor=None, dtime=None,
            dynamic='', lines='AUTO', threads=3, tid=122, tags=None, cover_path=None, description='',
            dolby=0, hires=0, no_reprint=0, is_only_self=0, charging_pay=0, credits=None,
            user_cookie='cookies.json', copyright_source=None, extra_fields = "", section_id=None
    ):
        super().__init__(principal, data, persistence_path='bili.cookie', postprocessor=postprocessor)
        self.section_id = section_id
        if tags is None:
            tags = []
        else:
            tags = [str(tag).format(streamer=self.data['name']) for tag in tags]
        self.lines = lines
        self.submit_api = submit_api
        self.threads = threads
        self.tid = tid
        self.tags = tags
        if cover_path:
            self.cover_path = cover_path
        elif "live_cover_path" in self.data:
            self.cover_path = self.data["live_cover_path"]
        else:
            self.cover_path = None
        self.desc = description
        self.credits = credits if credits else []
        self.dynamic = dynamic
        self.copyright = copyright
        self.dtime = dtime
        self.dolby = dolby
        self.hires = hires
        self.no_reprint = no_reprint
        self.is_only_self = is_only_self
        self.charging_pay = charging_pay
        self.user_cookie = user_cookie
        self.copyright_source = copyright_source

        if 'extra_fields' in data:
            self.extra_fields = data.get('extra_fields', '')
        else:
            self.extra_fields = extra_fields

    def upload(self, file_list: List[UploadBase.FileInfo]) -> List[UploadBase.FileInfo]:
        if self.credits:
            desc_v2 = self.creditsToDesc_v2()
        else:
            desc_v2 = [{
                "raw_text": self.desc,
                "biz_id": "",
                "type": 1
            }]

        ex_parent_conn, ex_child_conn = mp.Pipe()
        upload_args = {
            "ex_conn": ex_child_conn,
            "lines": self.lines,
            "submit": self.submit_api,
            "video_path": [file.video for file in file_list],
            "cookie_file": self.user_cookie,
            "title": self.data["format_title"][:80],
            "tid": self.tid,
            "tag": ','.join(self.tags),
            "copyright": self.copyright,
            "source": self.copyright_source if self.copyright_source else self.data["url"],
            "desc": self.desc,
            "dynamic": self.dynamic,
            "cover": self.cover_path if self.cover_path is not None else "",
            "dolby": self.dolby,
            "lossless_music": self.hires,
            "no_reprint": self.no_reprint,
            "charging_pay": self.charging_pay,
            "limit": self.threads,
            "desc_v2": desc_v2,
            "extra_fields": self.extra_fields,
            "dtime": int(time.time() + self.dtime) if self.dtime else None,
        }
        upload_process = mp.get_context('spawn').Process(target=stream_gears_upload, daemon=True, kwargs=upload_args)
        upload_process.start()
        upload_process.join()
        if ex_parent_conn.poll():
            raise RuntimeError(ex_parent_conn.recv())

        logger.info(f"上传成功: {self.principal}")

        if self.section_id:
            try:
                from .bili_webup import BiliBili, Data
                video_data = Data()
                with BiliBili(video_data) as bili:
                    # 手动解析 biliup-rs 的 cookie 格式并注入 session
                    import json
                    with open(self.user_cookie, encoding='utf-8') as f:
                        cookie_data = json.load(f)
                        cookies_dict = {c["name"]: c["value"] for c in cookie_data.get("cookie_info", {}).get("cookies", [])}
                        import requests
                        requests.utils.add_dict_to_cookiejar(bili._BiliBili__session.cookies, cookies_dict)

                    import time
                    print(f"[Biliup合集插件] 准备将视频加入合集 ID: {self.section_id}")
                    
                    # 循环重试机制，应对B站处理延迟
                    aid = None
                    cid = None
                    title = None
                    for i in range(5):
                        time.sleep(5)
                        print(f"[Biliup合集插件] 第 {i+1} 次尝试获取最近投稿列表...")
                        arc_list = bili._BiliBili__session.get(
                            'https://member.bilibili.com/x/web/archives',
                            params={'pn': 1, 'ps': 5, 'status': 'is_pubing,pubed,not_pubed'},
                            timeout=10
                        ).json()
                        
                        if arc_list.get('code') == 0:
                            archives = arc_list.get('data', {}).get('arc_audits', [])
                            if archives:
                                videos = archives[0].get('Videos')
                                if not videos:
                                    print("[Biliup合集插件] 投稿已出现，但B站暂未生成视频分P信息(Videos为空)，继续等待...")
                                    continue
                                latest = archives[0]['Archive']
                                aid = latest['aid']
                                cid = videos[0].get('cid')
                                title = latest['title']
                                print(f"[Biliup合集插件] 查到了最新投稿: {title} (aid: {aid}, cid: {cid})")
                                break
                            else:
                                print("[Biliup合集插件] 投稿列表为空，继续等待...")
                        else:
                            print(f"[Biliup合集插件] 查询投稿列表报错: {arc_list}")
                            
                    if aid and cid:
                        try:
                            print(f"[Biliup合集插件] 正在提交合集添加请求 (aid: {aid})...")
                            res = bili.add_to_season(self.section_id, [{
                                'aid': aid,
                                'cid': cid,
                                'title': title,
                                'charging_pay': 0
                            }])
                            print(f"[Biliup合集插件] B站返回合集添加结果: {res}")
                            logger.info(f"添加到合集成功: {res}")
                        except Exception as e:
                            print(f"[Biliup合集插件] 添加到合集异常: {e}")
                            logger.warning(f"添加到合集失败: {e}")
                    else:
                        print("[Biliup合集插件] 未查到刚上传的视频，放弃添加合集")
                        logger.warning("未找到最近的投稿，无法自动添加到合集")
            except Exception as e:
                logger.error(f"添加到合集失败(section_id={self.section_id}): {e}")

        return file_list

    def creditsToDesc_v2(self):
        desc_v2 = []
        desc_v2_tmp = self.desc
        for credit in self.credits:
            try:
                num = desc_v2_tmp.index("@credit")
                desc_v2.append({
                    "raw_text": " " + desc_v2_tmp[:num],
                    "biz_id": "",
                    "type": 1
                })
                desc_v2.append({
                    "raw_text": credit["username"],
                    "biz_id": str(credit["uid"]),
                    "type": 2
                })
                self.desc = self.desc.replace(
                    "@credit", "@" + credit["username"] + "  ", 1)
                desc_v2_tmp = desc_v2_tmp[num + 7:]
            except IndexError:
                logger.error('简介中的@credit占位符少于credits的数量,替换失败')
        desc_v2.append({
            "raw_text": " " + desc_v2_tmp,
            "biz_id": "",
            "type": 1
        })
        desc_v2[0]["raw_text"] = desc_v2[0]["raw_text"][1:]  # 开头空格会导致识别简介过长
        return desc_v2


def stream_gears_upload(ex_conn, lines, *args, **kwargs):
    try:
        if lines == 'bda':
            kwargs['line'] = stream_gears.UploadLine.Bda
        elif lines == 'bda2':
            kwargs['line'] = stream_gears.UploadLine.Bda2
        elif lines == 'qn':
            kwargs['line'] = stream_gears.UploadLine.Qn
        elif lines == 'tx':
            kwargs['line'] = stream_gears.UploadLine.Tx
        elif lines == 'txa':
            kwargs['line'] = stream_gears.UploadLine.Txa
        elif lines == 'bldsa':
            kwargs['line'] = stream_gears.UploadLine.Bldsa

        stream_gears.upload(*args, **kwargs)
    except Exception as e:
        ex_conn.send(e)
