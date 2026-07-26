import time

import stream_gears
from typing import List, Optional, Union
import json
import os

from . import SubmitOption
from ..engine import Plugin
from ..engine.upload import UploadBase, logger


@Plugin.upload(platform="stream_gears")
class BiliWeb(UploadBase):
    def __init__(
            self, principal, data, submit_api: Optional[Union[SubmitOption, str]] = None,
            copyright=2, postprocessor=None, dtime=None,
            dynamic='', lines='AUTO', threads=3, tid=122, tags=None, cover_path=None, description='',
            dolby=0, hires=0, no_reprint=0, charging_pay=0, credits=None,
            user_cookie='cookies.json', copyright_source=None, extra_fields = "",
            section_id: Optional[int] = None,
    ):
        super().__init__(principal, data, persistence_path='bili.cookie', postprocessor=postprocessor)
        if credits is None:
            credits = []
        if tags is None:
            tags = []
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
        self.credits = credits
        self.dynamic = dynamic
        self.copyright = copyright
        self.dtime = dtime
        self.dolby = dolby
        self.hires = hires
        self.no_reprint = no_reprint
        self.charging_pay = charging_pay
        self.user_cookie = user_cookie
        self.copyright_source = copyright_source

        if "extra_fields" in self.data:
            self.extra_fields = self.data["extra_fields"]
        else:
            self.extra_fields = extra_fields
        self.section_id = section_id


    def upload(self, file_list) -> List[UploadBase.FileInfo]:
        line = None
        if self.lines == 'bda':
            line = stream_gears.UploadLine.Bda
        elif self.lines == 'bda2':
            line = stream_gears.UploadLine.Bda2
        elif self.lines == 'qn':
            line = stream_gears.UploadLine.Qn
        elif self.lines == 'tx':
            line = stream_gears.UploadLine.Tx
        elif self.lines == 'txa':
            line = stream_gears.UploadLine.Txa
        elif self.lines == 'bldsa':
            line = stream_gears.UploadLine.Bldsa
        tag = ','.join(self.tags)
        if self.credits:
            desc_v2 = self.creditsToDesc_v2()
        else:
            desc_v2 = [{
                "raw_text": self.desc,
                "biz_id": "",
                "type": 1
            }]

        source = self.copyright_source if self.copyright_source else "https://github.com/biliup/biliup"
        cover = self.cover_path if self.cover_path is not None else ""
        dtime = None
        if self.dtime:
            dtime = int(time.time() + self.dtime)
        stream_gears.upload(
            video_path=file_list,
            cookie_file=self.user_cookie,
            title=self.data["format_title"][:80],
            tid=self.tid,
            tag=tag,
            copyright=self.copyright,
            source=source,
            desc=self.desc,
            dynamic=self.dynamic,
            cover=cover,
            dolby=self.dolby,
            lossless_music=self.hires,
            no_reprint=self.no_reprint,
            charging_pay=self.charging_pay,
            limit=self.threads,
            desc_v2=desc_v2,
            dtime=dtime,
            line=line,
            extra_fields=self.extra_fields,
            submit=self.submit_api,
        )
        logger.info(f"上传成功: {self.principal}")

        # 如果配置了 section_id，自动将视频添加到合集
        if self.section_id:
            try:
                from .bili_webup import BiliBili, Data
                video_data = Data()
                with BiliBili(video_data) as bili:
                    # 手动解析 biliup-rs 的 cookie 格式并注入 session，绕过旧版 bili.login 的格式不兼容问题
                    with open(self.user_cookie, encoding='utf-8') as f:
                        cookie_data = json.load(f)
                        cookies_dict = {c["name"]: c["value"] for c in cookie_data.get("cookie_info", {}).get("cookies", [])}
                        import requests
                        requests.utils.add_dict_to_cookiejar(bili._BiliBili__session.cookies, cookies_dict)
                    
                    import time
                    print(f"[Biliup合集插件] 准备将视频加入合集 ID: {self.section_id}")
                    
                    # B站服务器处理有延迟，增加循环重试机制，最多查 5 次，每次等 5 秒
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
                                latest = archives[0]['Archive']
                                # 为了防止拿到的是上一期老视频，我们必须验证一下（这里假设最新查出来的就是刚才上传的）
                                aid = latest['aid']
                                cid = archives[0].get('Videos', [{}])[0].get('cid')
                                title = latest['title']
                                print(f"[Biliup合集插件] 查到了最新投稿: {title} (aid: {aid}, cid: {cid})")
                                break
                            else:
                                print("[Biliup合集插件] 投稿列表为空，继续等待...")
                        else:
                            print(f"[Biliup合集插件] 查询投稿列表接口报错: {arc_list}")
                            
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
                            print(f"[Biliup合集插件] 添加到合集遇到异常报错: {e}")
                            logger.warning(f"添加到合集失败: {e}")
                    else:
                        print("[Biliup合集插件] 严重警告：等待了 25 秒仍未在 B 站查到刚上传的视频，放弃添加合集！")
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
