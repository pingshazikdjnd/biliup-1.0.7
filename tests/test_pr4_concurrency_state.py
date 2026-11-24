import asyncio
import copy
import json
import os
import tempfile
import threading
import unittest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from biliup.common.tools import NamedLock
from biliup.config import config
from biliup.database.models import (
    BaseModel,
    Configuration,
    FileList,
    LiveStreamers,
    StreamerInfo,
    UploadStreamers,
)


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
        import biliup.app as app_mod
        import biliup.engine.upload as upload_mod
        import biliup.web as web_mod
        return app_mod, upload_mod, web_mod
    finally:
        asyncio.get_running_loop = orig_get_running_loop
        temp_loop.close()


app_mod, upload_mod, web_mod = _bootstrap_biliup()


class TestPR4ConcurrencyAndState(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self._orig_cwd = os.getcwd()
        self._config_snapshot = {
            k: (v if k == 'PluginInfo' else copy.deepcopy(v))
            for k, v in config.data.items()
        }
        self._lock_keys_before = set(NamedLock._lock_dict.keys())
        self._tmpdir = tempfile.TemporaryDirectory()
        os.chdir(self._tmpdir.name)

        self._engine = create_engine('sqlite:///:memory:', connect_args={'check_same_thread': False})
        BaseModel.metadata.create_all(self._engine)
        self._TestSession = sessionmaker(bind=self._engine)

        config['streamers'] = {}
        app_mod.context['url_upload_count'] = {}
        app_mod.context['upload_filename'] = []
        app_mod.context['file_upload_count'] = {}

    async def asyncTearDown(self):
        os.chdir(self._orig_cwd)
        self._tmpdir.cleanup()
        self._engine.dispose()
        config.data.clear()
        config.data.update(self._config_snapshot)
        for k in list(NamedLock._lock_dict.keys()):
            if k not in self._lock_keys_before:
                NamedLock._lock_dict.pop(k, None)

    def test_named_lock_thread_safety_uniqueness(self):
        lock_name = 'pr4_concurrent_unique_lock'
        num_threads = 20
        barrier = threading.Barrier(num_threads)
        acquired_locks = []
        list_guard = threading.Lock()

        def worker():
            barrier.wait()
            lk = NamedLock(lock_name)
            with list_guard:
                acquired_locks.append(lk)

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        self.assertEqual(len(acquired_locks), num_threads)
        self.assertEqual(len({id(lk) for lk in acquired_locks}), 1)

    def test_upload_base_start_does_not_release_other_threads_lock(self):
        principal = 'lock_safety_streamer'
        lock = NamedLock(f'upload_file_list_{principal}')
        in_upload_evt = threading.Event()
        b_acquired_evt = threading.Event()
        errors = []

        class DummyUploader(upload_mod.UploadBase):
            def upload(self, file_list):
                in_upload_evt.set()
                # 等待线程 B 获取同一把命名锁
                if not b_acquired_evt.wait(timeout=5):
                    raise TimeoutError('Thread B did not acquire lock')
                return file_list

        dummy_file = upload_mod.UploadBase.FileInfo(video='lock_test.mp4', danmaku=None)
        up = DummyUploader(principal, {'format_title': 't'})

        def run_a():
            try:
                up.start()
            except Exception as exc:
                errors.append(exc)

        with patch.object(upload_mod.UploadBase, 'file_list', return_value=[dummy_file]):
            thread_a = threading.Thread(target=run_a)
            thread_a.start()

            self.assertTrue(in_upload_evt.wait(timeout=5))
            # 此时线程 A 已经释放了扫描锁进入 self.upload()，线程 B 应能成功获取锁
            self.assertTrue(lock.acquire(timeout=2))
            try:
                b_acquired_evt.set()
                thread_a.join(timeout=5)
                # 核心断言：线程 A 退出 finally 后不能误释放线程 B 持有的锁，也不能抛出 RuntimeError
                self.assertEqual(errors, [])
                self.assertTrue(lock.locked())
            finally:
                if lock.locked():
                    lock.release()

    def test_file_list_protects_part_during_recording_and_processes_when_idle(self):
        streamer = 'rec_streamer'
        url = 'https://live.test/rec_1'
        config['streamers'] = {streamer: {'url': url}}
        config['filtering_threshold'] = 0

        fake_plugin_info = MagicMock()
        fake_plugin_info.url_status = {url: 1}  # 正在录制中
        fake_plugin_info.inverted_index = {url: streamer}
        app_mod.context['PluginInfo'] = fake_plugin_info

        with open(f'{streamer}_0.flv', 'wb') as f:
            f.write(b'x' * 2048)
        with open(f'{streamer}_1.flv.part', 'wb') as f:
            f.write(b'')  # 若未跳过，<= filtering_threshold(0) 会被重命名并直接删除
        with open(f'{streamer}_1.xml', 'w', encoding='utf-8') as f:
            f.write('<i/>')

        with patch.object(upload_mod, 'SessionLocal', self._TestSession):
            res_recording = upload_mod.UploadBase.file_list(streamer)

        self.assertEqual(res_recording, [upload_mod.UploadBase.FileInfo(video=f'{streamer}_0.flv', danmaku=None)])
        self.assertTrue(os.path.exists(f'{streamer}_1.flv.part'))
        self.assertFalse(os.path.exists(f'{streamer}_1.flv'))
        self.assertTrue(os.path.exists(f'{streamer}_1.xml'))

        # 兼容 list 类型的 url 配置且列表内其他地址处于录制中（不能抛 TypeError）
        config['streamers'][streamer]['url'] = [url, 'https://live.test/rec_2']
        fake_plugin_info.url_status = {url: 0, 'https://live.test/rec_2': 1}
        with patch.object(upload_mod, 'SessionLocal', self._TestSession):
            res_list_url = upload_mod.UploadBase.file_list(streamer)
        self.assertEqual(res_list_url, [upload_mod.UploadBase.FileInfo(video=f'{streamer}_0.flv', danmaku=None)])
        self.assertTrue(os.path.exists(f'{streamer}_1.flv.part'))

        # 录制结束后（url_status=0），.part 正常去后缀并被识别
        fake_plugin_info.url_status = {url: 0, 'https://live.test/rec_2': 0}
        with open(f'{streamer}_1.flv.part', 'wb') as f:
            f.write(b'y' * 2048)

        with patch.object(upload_mod, 'SessionLocal', self._TestSession):
            res_idle = upload_mod.UploadBase.file_list(streamer)

        self.assertEqual(len(res_idle), 2)
        self.assertFalse(os.path.exists(f'{streamer}_1.flv.part'))
        self.assertTrue(os.path.exists(f'{streamer}_1.flv'))
        self.assertIn(
            upload_mod.UploadBase.FileInfo(video=f'{streamer}_1.flv', danmaku=f'{streamer}_1.xml'),
            res_idle,
        )

    def test_file_list_db_fallback_matches_custom_filename_without_streamer(self):
        streamer = 'custom_streamer'
        config['streamers'] = {streamer: {'url': 'https://live.test/custom'}}
        config['filtering_threshold'] = 0

        fake_plugin_info = MagicMock()
        fake_plugin_info.url_status = {'https://live.test/custom': 0}
        fake_plugin_info.inverted_index = {}
        app_mod.context['PluginInfo'] = fake_plugin_info

        with self._TestSession() as db:
            si = StreamerInfo(
                name=streamer,
                url='https://live.test/custom',
                title='自定义标题',
                date=datetime.now(),
                live_cover_path='',
            )
            db.add(si)
            db.flush()
            db.add(FileList(file='2026.11.24_custom_video.flv', streamer_info_id=si.id))
            db.add(FileList(file='2026.11.24.part2.flv.part', streamer_info_id=si.id))
            db.commit()

        # 文件名中完全不含 streamer 名称，且包含多段点号
        with open('2026.11.24_custom_video.flv', 'wb') as f:
            f.write(b'a' * 2048)
        with open('2026.11.24_custom_video.xml', 'w', encoding='utf-8') as f:
            f.write('<i/>')
        with open('2026.11.24.part2.flv', 'wb') as f:
            f.write(b'b' * 2048)

        with patch.object(upload_mod, 'SessionLocal', self._TestSession):
            res = upload_mod.UploadBase.file_list(streamer)

        self.assertEqual(
            {(item.video, item.danmaku) for item in res},
            {
                ('2026.11.24_custom_video.flv', '2026.11.24_custom_video.xml'),
                ('2026.11.24.part2.flv', None),
            },
        )

    async def test_singleton_check_does_not_block_event_loop(self):
        name = 'async_loop_streamer'
        url = 'https://live.test/async'
        fake_plugin_info = MagicMock()
        fake_plugin_info.url_status = {url: 0}
        fake_plugin_info.inverted_index = {url: name}
        app_mod.context['PluginInfo'] = fake_plugin_info

        lock = NamedLock(f'upload_file_list_{name}')
        self.assertTrue(lock.acquire(timeout=2))

        class DummyPlatform:
            def __init__(self, n, u):
                pass

            async def acheck_stream(self, is_check=False):
                return True

            def should_record(self):
                return True

        sent_events = []
        watchdog_triggered = threading.Event()

        def _watchdog_release():
            watchdog_triggered.set()
            if lock.locked():
                lock.release()

        watchdog = threading.Timer(3.0, _watchdog_release)
        watchdog.daemon = True
        watchdog.start()
        try:
            with patch.object(app_mod.event_manager, 'send_event', side_effect=lambda ev: sent_events.append(ev)):
                check_task = asyncio.create_task(app_mod.singleton_check(DummyPlatform, name, url))

                # 让出事件循环使 singleton_check 走到 NamedLock 等待处
                await asyncio.sleep(0.05)
                self.assertFalse(check_task.done())

                # 同事件循环上的其他协程必须能立即推进而不被锁死
                heartbeat_ran = False

                async def heartbeat():
                    nonlocal heartbeat_ran
                    await asyncio.sleep(0.01)
                    heartbeat_ran = True

                await asyncio.wait_for(heartbeat(), timeout=1.0)
                self.assertTrue(heartbeat_ran)
                self.assertFalse(check_task.done())
        finally:
            watchdog.cancel()
            if lock.locked():
                lock.release()

        self.assertFalse(watchdog_triggered.is_set(), 'watchdog fired — singleton_check blocked event loop')
        with patch.object(app_mod.event_manager, 'send_event', side_effect=lambda ev: sent_events.append(ev)):
            await asyncio.wait_for(check_task, timeout=2.0)

        self.assertTrue(any(ev.type_ == 'pre_download' for ev in sent_events))

    async def test_singleton_check_cancelled_or_removed_url_skips_pre_download(self):
        name = 'cancel_or_removed_streamer'
        url = 'https://live.test/cancel_removed'
        fake_plugin_info = MagicMock()
        fake_plugin_info.url_status = {url: 0}
        fake_plugin_info.inverted_index = {url: name}
        app_mod.context['PluginInfo'] = fake_plugin_info

        orig_to_thread = asyncio.to_thread
        loop = asyncio.get_running_loop()
        worker_started = asyncio.Event()
        worker_done = asyncio.Event()

        async def _tracked_to_thread(func, *args, **kwargs):
            def _wrapped():
                loop.call_soon_threadsafe(worker_started.set)
                try:
                    return func(*args, **kwargs)
                finally:
                    loop.call_soon_threadsafe(worker_done.set)

            return await orig_to_thread(_wrapped)

        class DummyPlatform:
            def __init__(self, n, u):
                pass

            async def acheck_stream(self, is_check=False):
                return True

            def should_record(self):
                return True

        # A. 任务在等锁期间被取消，释放锁后不能再派发 PRE_DOWNLOAD
        lock = NamedLock(f'upload_file_list_{name}')
        sent_events = []
        self.assertTrue(lock.acquire(timeout=2))
        try:
            with patch.object(app_mod.asyncio, 'to_thread', side_effect=_tracked_to_thread), \
                 patch.object(app_mod.event_manager, 'send_event', side_effect=lambda ev: sent_events.append(ev)):
                check_task = asyncio.create_task(app_mod.singleton_check(DummyPlatform, name, url))
                await asyncio.wait_for(worker_started.wait(), timeout=2.0)
                check_task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await check_task
                lock.release()
                await asyncio.wait_for(worker_done.wait(), timeout=2.0)
        finally:
            if lock.locked():
                lock.release()
        self.assertEqual([ev for ev in sent_events if ev.type_ == 'pre_download'], [])

        # B. 主播 URL 在等锁期间从 inverted_index 移除，释放锁后不能派发旧 URL 的 PRE_DOWNLOAD
        worker_started.clear()
        worker_done.clear()
        sent_events.clear()
        self.assertTrue(lock.acquire(timeout=2))
        try:
            with patch.object(app_mod.asyncio, 'to_thread', side_effect=_tracked_to_thread), \
                 patch.object(app_mod.event_manager, 'send_event', side_effect=lambda ev: sent_events.append(ev)):
                check_task = asyncio.create_task(app_mod.singleton_check(DummyPlatform, name, url))
                await asyncio.wait_for(worker_started.wait(), timeout=2.0)
                fake_plugin_info.inverted_index.pop(url, None)
                lock.release()
                await asyncio.wait_for(check_task, timeout=2.0)
                await asyncio.wait_for(worker_done.wait(), timeout=2.0)
        finally:
            if lock.locked():
                lock.release()
        self.assertEqual([ev for ev in sent_events if ev.type_ == 'pre_download'], [])

    def test_config_load_from_db_atomicity_and_falsy_zero_override(self):
        with self._TestSession() as db:
            cfg_row = Configuration(
                key='config',
                value=json.dumps({'dolby': 1, 'no_reprint': 1, 'lines': 'AUTO'}),
            )
            db.add(cfg_row)
            tpl = UploadStreamers(
                template_name='tpl0',
                tags=['tag1'],
                dolby=0,
                no_reprint=0,
                uploader='bili_web',
            )
            db.add(tpl)
            db.flush()
            db.add(LiveStreamers(
                remark='atomic_streamer',
                url='https://live.test/atomic',
                upload_streamers_id=tpl.id,
            ))
            db.commit()

            fake_pi = MagicMock()
            config.data['PluginInfo'] = fake_pi

            stop_evt = threading.Event()
            reader_started = threading.Event()
            read_errors = []
            read_count = 0

            def concurrent_reader():
                nonlocal read_count
                reader_started.set()
                while not stop_evt.is_set():
                    try:
                        # 获取单次引用快照并校验跨字段一致性不变量
                        snap = config.data
                        if snap['PluginInfo'] is not fake_pi:
                            raise AssertionError('PluginInfo lost or mismatched')
                        streamer_cfg = snap['streamers']['atomic_streamer']
                        expected = {
                            'AUTO': (0, 'bili_web'),
                            'WS': (1, 'biliup-rs'),
                        }.get(snap.get('lines'))
                        if expected is None or (streamer_cfg.get('dolby'), streamer_cfg.get('uploader')) != expected:
                            raise AssertionError(
                                f"Inconsistent state observed: lines={snap.get('lines')}, "
                                f"dolby={streamer_cfg.get('dolby')}, uploader={streamer_cfg.get('uploader')}"
                            )
                        read_count += 1
                    except Exception as exc:
                        read_errors.append(exc)
                        break

            config.load_from_db(db)
            reader = threading.Thread(target=concurrent_reader)
            reader.start()
            self.assertTrue(reader_started.wait(timeout=2))
            try:
                # 在两套关联配置之间交替重载，最终回到 falsy 0 覆盖状态
                for i in range(40):
                    if i % 2 == 0:
                        cfg_row.value = json.dumps({'dolby': 1, 'no_reprint': 1, 'lines': 'WS'})
                        tpl.dolby = 1
                        tpl.uploader = 'biliup-rs'
                    else:
                        cfg_row.value = json.dumps({'dolby': 1, 'no_reprint': 1, 'lines': 'AUTO'})
                        tpl.dolby = 0
                        tpl.uploader = 'bili_web'
                    db.commit()
                    config.load_from_db(db)
            finally:
                stop_evt.set()
                reader.join(timeout=5)

            self.assertEqual(read_errors, [])
            self.assertGreaterEqual(read_count, 1)
            self.assertIs(config.data['PluginInfo'], fake_pi)
            self.assertEqual(config['streamers']['atomic_streamer']['dolby'], 0)
            self.assertEqual(config['streamers']['atomic_streamer']['no_reprint'], 0)
            self.assertEqual(config['streamers']['atomic_streamer']['uploader'], 'bili_web')

    async def test_web_put_delete_status_sync_and_404(self):
        fake_pi = MagicMock()
        app_mod.context['PluginInfo'] = fake_pi

        with self._TestSession() as db:
            db.add(Configuration(key='config', value=json.dumps({'lines': 'AUTO'})))
            tpl = UploadStreamers(
                template_name='tpl_web',
                title='模板标题',
                tags=['t1'],
                uploader='biliup-rs',
            )
            db.add(tpl)
            db.flush()
            ls = LiveStreamers(
                remark='web_streamer',
                url='https://live.test/web1',
                upload_streamers_id=tpl.id,
            )
            ls_keep = LiveStreamers(
                remark='web_streamer_2',
                url='https://live.test/web2',
                upload_streamers_id=tpl.id,
            )
            db.add_all([ls, ls_keep])
            db.commit()
            tpl_id = tpl.id
            ls_id = ls.id
            config.load_from_db(db)

        app = web.Application()
        app.add_routes(web_mod.routes)
        client = TestClient(TestServer(app))
        await client.start_server()
        self.addAsyncCleanup(client.close)

        with patch.object(web_mod, 'SessionLocal', self._TestSession):
            # 1. DELETE 不存在记录返回 404
            self.assertEqual((await client.delete('/v1/streamers/99999')).status, 404)
            self.assertEqual((await client.delete('/v1/upload/streamers/99999')).status, 404)

            # 2. PUT /v1/streamers 传入 upload_id=None 解绑模板并同步内存
            put_resp = await client.put('/v1/streamers', json={
                'id': ls_id,
                'remark': 'web_streamer',
                'url': 'https://live.test/web1',
                'upload_id': None,
            })
            self.assertEqual(put_resp.status, 200)
            put_body = await put_resp.json()
            self.assertIsNone(put_body['upload_streamers_id'])
            self.assertNotIn('title', config['streamers']['web_streamer'])

            # 3. DELETE /v1/streamers/{id} 同步清理内存与 PluginInfo
            del_ls_resp = await client.delete(f'/v1/streamers/{ls_id}')
            self.assertEqual(del_ls_resp.status, 200)
            self.assertNotIn('web_streamer', config['streamers'])
            fake_pi.delete.assert_called_with('https://live.test/web1')

            # 4. DELETE /v1/upload/streamers/{id} 同步更新仍关联该模板的主播内存配置
            self.assertIn('title', config['streamers']['web_streamer_2'])
            del_tpl_resp = await client.delete(f'/v1/upload/streamers/{tpl_id}')
            self.assertEqual(del_tpl_resp.status, 200)
            self.assertNotIn('title', config['streamers']['web_streamer_2'])


if __name__ == '__main__':
    unittest.main()
