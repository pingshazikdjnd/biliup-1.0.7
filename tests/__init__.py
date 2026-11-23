import asyncio
from biliup.config import config

# 1. 兼容 Python 无事件循环时 util.py 模块导入
_orig_get_running_loop = asyncio.get_running_loop
_test_loop = asyncio.new_event_loop()


def _safe_get_running_loop():
    try:
        return _orig_get_running_loop()
    except RuntimeError:
        return _test_loop


asyncio.get_running_loop = _safe_get_running_loop

# 2. 预置 streamers，保证 app.py 中的 @event_manager.server() 装饰 PluginInfo 时能够正常检索 context['streamers']
if 'streamers' not in config:
    config['streamers'] = {}

# 3. 预先导入 app，打断 biliup.plugins.* <-> biliup.engine.download 循环导入链
import biliup.app
