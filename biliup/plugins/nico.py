import asyncio
import random
import re
import subprocess

import biliup.common.util
from biliup.config import config
from ..common import tools
from ..engine.decorators import Plugin
from ..engine.download import DownloadBase
from ..plugins import logger


@Plugin.download(regexp=r'(?:https?://)?(?:(?:www|m|live)\.)?nicovideo\.jp')
class Nico(DownloadBase):
    def __init__(self, fname, url, suffix='flv'):
        super().__init__(fname, url, suffix)
        self.proc = None

    async def acheck_stream(self, is_check=False):
        try:
            response = await biliup.common.util.client.get(self.url, timeout=5)
            if response.status_code != 200:
                logger.debug(f"Nico - {self.url}: status_code {response.status_code}")
                return False
            # 正则表达式
            pattern = r'"name":"(.*?)","description":"(.*?)"'
            # 执行匹配
            matches = re.findall(pattern, response.text)
            if matches:
                self.room_title = matches[0][0]
        except Exception as e:
            logger.info(f"获取标题失败: {e}")

        if is_check:
            return True

        user_conf = config.get('user') or {}
        extra_args = []
        for opt_key in ('niconico-email', 'niconico-password', 'niconico-user-session'):
            val = user_conf.get(opt_key)
            if val is not None and val != '':
                extra_args.extend([f'--{opt_key}', str(val)])
        purge_cred = user_conf.get('niconico-purge-credentials')
        if purge_cred and str(purge_cred).lower() not in ('false', '0'):
            extra_args.append('--niconico-purge-credentials')

        port = random.randint(1025, 65535)
        stream_shell = [
            "streamlink",
            *extra_args,
            "--player-external-http",  # 为外部程序提供流媒体数据
            "--player-external-http-port", str(port),  # 对外部输出流的端口
            self.url, "best"  # 流链接
        ]
        self.proc = subprocess.Popen(stream_shell)
        self.raw_stream_url = f"http://localhost:{port}"
        i = 0
        while i < 5:
            if self.proc.poll() is not None:
                return False
            await asyncio.sleep(1)
            i += 1
        return True

    def close(self):
        try:
            if self.proc is not None:
                self.proc.terminate()
        except:
            logger.exception(f'terminate {self.fname} failed')
