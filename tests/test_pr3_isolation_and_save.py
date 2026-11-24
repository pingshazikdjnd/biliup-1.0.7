import asyncio
import copy
import json
import os
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import yaml
from biliup.config import config


def _bootstrap_biliup():
    orig_get_running_loop = asyncio.get_running_loop
    temp_loop = asyncio.new_event_loop()

    def _safe_get_running_loop():
        try:
            return orig_get_running_loop()
        except RuntimeError:
            return temp_loop

    asyncio.get_running_loop = _safe_get_running_loop
    try:
        if 'streamers' not in config:
            config['streamers'] = {}
        import biliup.app  # noqa: F401
        import biliup.common.util as util_mod
        from biliup.engine.download import DownloadBase
        from biliup.plugins.bilibili import Bililive
        import biliup.plugins.huya as huya_mod
        from biliup.plugins.kuaishou import Kuaishou
        return util_mod, DownloadBase, Bililive, huya_mod, Kuaishou
    finally:
        asyncio.get_running_loop = orig_get_running_loop
        temp_loop.close()


util_mod, DownloadBase, Bililive, huya_mod, Kuaishou = _bootstrap_biliup()


class TestPR3IsolationAndSave(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._orig_cwd = os.getcwd()
        self._config_snapshot = {
            k: (v if k == 'PluginInfo' else copy.deepcopy(v))
            for k, v in config.data.items()
        }
        self._headers_snapshot = dict(util_mod.client.headers)
        self._tmpdir = tempfile.TemporaryDirectory()
        os.chdir(self._tmpdir.name)
        config.data.clear()
        config.data['streamers'] = {}

    async def asyncTearDown(self):
        os.chdir(self._orig_cwd)
        self._tmpdir.cleanup()
        util_mod.client.headers.clear()
        util_mod.client.headers.update(self._headers_snapshot)
        config.data.clear()
        config.data.update(self._config_snapshot)

    async def test_acheck_url_healthy_does_not_pollute_global_client_headers(self):
        class DummyDownloader(DownloadBase):
            async def acheck_stream(self, is_check=False):
                return True

        dl = DummyDownloader('test_streamer', 'https://example.com/room', suffix='flv')
        dl.fake_headers['cookie'] = 'SESSDATA=secret_leak_check'
        dl.fake_headers['Cookie'] = 'SESSDATA=secret_leak_check'
        # 模拟 Huya 等插件在 update_headers 中附加自定义拉流头
        dl.update_headers = lambda h: h.update({'user-agent': 'HYSDK-Custom'})

        # 确认全局 client.headers 初始无 cookie
        util_mod.client.headers.pop('cookie', None)
        util_mod.client.headers.pop('Cookie', None)

        captured_stream_headers = []
        captured_get_headers = []

        class _FakeStreamCM:
            def __init__(self, status_code=200, headers=None):
                self._resp = MagicMock()
                self._resp.status_code = status_code
                self._resp.headers = headers or {}
                self._resp.raise_for_status = MagicMock()

            async def __aenter__(self):
                return self._resp

            async def __aexit__(self, exc_type, exc, tb):
                return False

        def fake_stream(method, url, **kwargs):
            captured_stream_headers.append(dict(kwargs.get('headers') or {}))
            return _FakeStreamCM(status_code=200)

        async def fake_get(url, **kwargs):
            captured_get_headers.append(dict(kwargs.get('headers') or {}))
            resp = MagicMock()
            resp.status_code = 200
            resp.text = '#EXTM3U\n#EXT-X-TARGETDURATION:10\n'
            resp.raise_for_status = MagicMock()
            return resp

        with patch.object(util_mod.client, 'stream', side_effect=fake_stream), \
                patch.object(util_mod.client, 'get', side_effect=fake_get):
            flv_res = await dl.acheck_url_healthy('https://cdn.example.com/live/stream.flv')
            m3u8_res = await dl.acheck_url_healthy('https://cdn.example.com/live/stream.m3u8')

        self.assertEqual(flv_res, 'https://cdn.example.com/live/stream.flv')
        self.assertEqual(m3u8_res, 'https://cdn.example.com/live/stream.m3u8')

        # 1. 全局 client.headers 严禁被写入 cookie / Cookie 或自定义 UA
        global_lower_keys = {k.lower() for k in util_mod.client.headers.keys()}
        self.assertNotIn('cookie', global_lower_keys)
        self.assertNotEqual(util_mod.client.headers.get('user-agent'), 'HYSDK-Custom')

        # 2. 单次请求显式携带 stream_headers + update_headers 结果，且不包含 Cookie
        self.assertEqual(len(captured_stream_headers), 1)
        self.assertEqual(captured_stream_headers[0].get('user-agent'), 'HYSDK-Custom')
        self.assertNotIn('cookie', {k.lower() for k in captured_stream_headers[0]})

        self.assertEqual(len(captured_get_headers), 1)
        self.assertEqual(captured_get_headers[0].get('user-agent'), 'HYSDK-Custom')
        self.assertNotIn('cookie', {k.lower() for k in captured_get_headers[0]})

    async def test_bililive_get_play_info_still_passes_cookie_per_request(self):
        config['user'] = {'bili_cookie': 'SESSDATA=bili_secret;bili_jct=123'}
        bili = Bililive('bili_room', 'https://live.bilibili.com/123456')
        bili.fake_headers['cookie'] = 'SESSDATA=bili_secret;bili_jct=123'
        bili.fake_headers['referer'] = 'https://live.bilibili.com/123456'

        captured = []

        async def fake_get(url, **kwargs):
            captured.append((url, dict(kwargs.get('headers') or {})))
            resp = MagicMock()
            resp.text = json.dumps({'code': 0, 'data': {'playurl_info': {'ok': True}}})
            return resp

        with patch('biliup.plugins.bilibili.wbi.sign'), \
                patch.object(util_mod.client, 'get', side_effect=fake_get):
            data = await bili.get_play_info('https://proxy.example.com')

        self.assertEqual(data, {'playurl_info': {'ok': True}})
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0][1].get('cookie'), 'SESSDATA=bili_secret;bili_jct=123')
        self.assertNotIn('cookie', {k.lower() for k in util_mod.client.headers.keys()})

    async def test_kuaishou_and_huya_do_not_pollute_global_client_headers(self):
        # 1. 快手插件
        config['kuaishou_cookie'] = 'ks_secret=abc'
        ks = Kuaishou('ks_streamer', 'https://live.kuaishou.com/u/test_ks')
        ks_calls = []

        async def fake_ks_get(url, **kwargs):
            ks_calls.append((url, dict(kwargs.get('headers') or {})))
            resp = MagicMock()
            if 'livedetail' in url:
                resp.json.return_value = {
                    'data': {
                        'result': 1,
                        'liveStream': {
                            'caption': '快手测试',
                            'playUrls': [{'adaptationSet': {'representation': [{'url': 'https://ks.cdn/live.flv'}]}}],
                        },
                    }
                }
            else:
                resp.text = '<html>ok</html>'
            return resp

        with patch('biliup.plugins.kuaishou.time.sleep'), \
                patch.object(util_mod.client, 'get', side_effect=fake_ks_get):
            ok_ks = await ks.acheck_stream()

        self.assertTrue(ok_ks)
        self.assertEqual(len(ks_calls), 3)
        for _, req_headers in ks_calls:
            self.assertEqual(req_headers.get('Cookie'), 'ks_secret=abc')
        self.assertNotIn('cookie', {k.lower() for k in util_mod.client.headers.keys()})

        # 2. 虎牙插件（含 _get_real_rid 缓存及二次调用）
        huya_mod._get_real_rid.cache_clear()
        huya = huya_mod.Huya('huya_streamer', 'https://www.huya.com/my_slug_room')
        huya.fake_headers['cookie'] = 'huya_secret=xyz'
        huya_calls = []

        async def fake_huya_get(url, **kwargs):
            huya_calls.append((url, dict(kwargs.get('headers') or {})))
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if 'my_slug_room' in url:
                resp.text = 'var TT_ROOM_DATA = {"profileRoom": 888999};'
            else:
                resp.text = ''
            return resp

        with patch.object(util_mod.client, 'get', side_effect=fake_huya_get), \
                patch.object(huya, 'get_room_profile', new=AsyncMock(return_value={'live': False, 'message': 'off'})):
            await huya.acheck_stream(is_check=True)
            # 再次触发缓存路径验证稳定性
            huya._Huya__room_id = 'my_slug_room'
            await huya.acheck_stream(is_check=True)

        self.assertEqual(len(huya_calls), 1)  # _get_real_rid 命中缓存仅请求 1 次
        self.assertEqual(huya_calls[0][1].get('cookie'), 'huya_secret=xyz')
        self.assertNotIn('cookie', {k.lower() for k in util_mod.client.headers.keys()})

    def test_config_save_preserves_fields_handles_empty_and_atomic_replace(self):
        # 1. 保留未修改字段并更新目标字段
        initial_yaml = {
            'event_loop_interval': 45,
            'downloader': 'ffmpeg',
            'user': {
                'account': {'username': 'keep_me'},
                'cookies': {'SESSDATA': 'old'},
                'access_token': 'old_tok',
            },
            'lines': 'qn',
            'threads': 2,
            'streamers': {'old': {'url': 'http://old'}},
        }
        with open('config.yaml', 'w', encoding='utf-8') as f:
            yaml.safe_dump(initial_yaml, f, allow_unicode=True)

        config.data.update({
            'user': {
                'cookies': {'SESSDATA': 'new_sess', 'bili_jct': 'new_jct'},
                'access_token': 'new_tok',
            },
            'lines': 'bda2',
            'threads': 5,
            'streamers': {'room1': {'url': 'https://live.bilibili.com/1'}},
        })
        config.save()

        with open('config.yaml', 'r', encoding='utf-8') as f:
            saved = yaml.safe_load(f)

        self.assertEqual(saved['event_loop_interval'], 45)
        self.assertEqual(saved['downloader'], 'ffmpeg')
        self.assertEqual(saved['user']['account'], {'username': 'keep_me'})
        self.assertEqual(saved['user']['cookies'], {'SESSDATA': 'new_sess', 'bili_jct': 'new_jct'})
        self.assertEqual(saved['user']['access_token'], 'new_tok')
        self.assertEqual(saved['lines'], 'bda2')
        self.assertEqual(saved['threads'], 5)
        self.assertEqual(saved['streamers'], {'room1': {'url': 'https://live.bilibili.com/1'}})
        self.assertEqual([n for n in os.listdir('.') if n.startswith('.config.yaml.')], [])

        # 2. 空 config.yaml (0 字节) 不抛 TypeError
        with open('config.yaml', 'w', encoding='utf-8') as f:
            f.write('')
        config.save()
        with open('config.yaml', 'r', encoding='utf-8') as f:
            saved_empty = yaml.safe_load(f)
        self.assertEqual(saved_empty['lines'], 'bda2')
        self.assertEqual(saved_empty['user']['access_token'], 'new_tok')

        # 3. 写入异常时不截断/破坏原有 config.yaml 且清理临时文件
        with open('config.yaml', 'r', encoding='utf-8') as f:
            before_content = f.read()
        with patch('yaml.safe_dump', side_effect=RuntimeError('disk write error')):
            with self.assertRaises(RuntimeError):
                config.save()
        with open('config.yaml', 'r', encoding='utf-8') as f:
            after_content = f.read()
        self.assertEqual(after_content, before_content)
        self.assertEqual([n for n in os.listdir('.') if n.startswith('.config.yaml.')], [])

        # 4. 保留原文件权限 (shutil.copymode) 与软链接目标原子替换
        with patch('biliup.config.shutil.copymode') as mock_copymode:
            config.save()
            mock_copymode.assert_called_once()
            self.assertEqual(mock_copymode.call_args[0][0], 'config.yaml')

        os.makedirs('linked_dir', exist_ok=True)
        real_target = os.path.abspath(os.path.join('linked_dir', 'real_config.yaml'))
        with open(real_target, 'w', encoding='utf-8') as f:
            yaml.safe_dump({'keep_target': True}, f)
        if os.path.exists('config.yaml'):
            os.remove('config.yaml')

        used_real_symlink = False
        try:
            os.symlink(real_target, 'config.yaml')
            used_real_symlink = True
        except OSError:
            pass

        if used_real_symlink:
            config.save()
            self.assertTrue(os.path.islink('config.yaml'))
        else:
            with patch('biliup.config.os.path.islink', side_effect=lambda p: p == 'config.yaml'), \
                    patch('biliup.config.os.path.realpath', return_value=real_target):
                config.save()

        with open(real_target, 'r', encoding='utf-8') as f:
            linked_saved = yaml.safe_load(f)
        self.assertTrue(linked_saved['keep_target'])
        self.assertEqual(linked_saved['lines'], 'bda2')


if __name__ == '__main__':
    unittest.main()
