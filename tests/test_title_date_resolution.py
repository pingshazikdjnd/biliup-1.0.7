import copy
import io
import os
import tempfile
import time
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import tests  # noqa: F401
import biliup.app as app_mod
import biliup.database.db as db_mod
import biliup.engine.download as download_mod
import biliup.handler as handler_mod
from biliup.config import config
from biliup.database.models import BaseModel, FileList, StreamerInfo
from biliup.engine.event import Event
from biliup.engine.upload import UploadBase


class _FakeDummyDownloader(download_mod.DownloadBase):
    async def acheck_stream(self, is_check=False):
        return True


class TestTitleDateResolution(unittest.TestCase):
    def setUp(self):
        self._orig_cwd = os.getcwd()
        self._tmpdir = tempfile.TemporaryDirectory()
        os.chdir(self._tmpdir.name)
        self._config_snapshot = {
            k: (v if k == 'PluginInfo' else copy.deepcopy(v))
            for k, v in config.data.items()
        }
        self._url_upload_snapshot = copy.deepcopy(app_mod.context.get('url_upload_count', {}))
        self._file_upload_snapshot = copy.deepcopy(app_mod.context.get('file_upload_count', {}))
        self._upload_filename_snapshot = list(app_mod.context.get('upload_filename', []))
        self._plugin_info_snapshot = app_mod.context.get('PluginInfo')

        config.data.clear()
        config.data.update({
            'streamers': {},
            'url_upload_count': {},
            'upload_filename': [],
            'file_upload_count': {},
            'sync_downloader_map': {},
            'delay': 0,
            'max_upload_limit': 5,
        })
        app_mod.context['url_upload_count'] = {}
        app_mod.context['file_upload_count'] = {}
        app_mod.context['upload_filename'] = []
        app_mod.context['PluginInfo'] = MagicMock()
        app_mod.context['PluginInfo'].url_status = {}

        db_file = os.path.join(self._tmpdir.name, 'test_title_date.sqlite3')
        self._engine = create_engine(f'sqlite:///{db_file}', connect_args={'check_same_thread': False})
        BaseModel.metadata.create_all(self._engine)
        self._TestSession = sessionmaker(bind=self._engine, autocommit=False)

    def tearDown(self):
        self._engine.dispose()
        os.chdir(self._orig_cwd)
        self._tmpdir.cleanup()
        config.data.clear()
        config.data.update(self._config_snapshot)
        app_mod.context['url_upload_count'] = self._url_upload_snapshot
        app_mod.context['file_upload_count'] = self._file_upload_snapshot
        app_mod.context['upload_filename'] = self._upload_filename_snapshot
        if self._plugin_info_snapshot is not None:
            app_mod.context['PluginInfo'] = self._plugin_info_snapshot
        else:
            app_mod.context.pop('PluginInfo', None)

    def test_cross_midnight_reconnect_uses_first_part_db_date(self):
        streamer = '【赫萝老师录播】'
        url = 'https://live.bilibili.com/17961'
        p1_name = f'{streamer}2026-11-24 22_00_00.flv'
        p2_name = f'{streamer}2026-11-25 00_15_00.flv'
        for fname in (p1_name, p2_name):
            with open(fname, 'wb') as f:
                f.write(b'flvdata')

        day1_time = time.strptime('2026-11-24 22:00:00', '%Y-%m-%d %H:%M:%S')
        day2_time = time.strptime('2026-11-25 00:15:00', '%Y-%m-%d %H:%M:%S')

        with self._TestSession() as db:
            row1_id = db_mod.add_stream_info(db, streamer, url, day1_time)
            db_mod.update_room_title(db, row1_id, '第一天标题')
            db_mod.update_file_list(db, row1_id, p1_name)

            row2_id = db_mod.add_stream_info(db, streamer, url, day2_time)
            db_mod.update_room_title(db, row2_id, '第二天重连标题')
            db_mod.update_file_list(db, row2_id, p2_name)

        config['streamers'] = {
            streamer: {
                'url': url,
                'title': '{streamer} %Y-%m-%d {title}',
                'uploader': 'Noop',
            }
        }
        app_mod.context['url_upload_count'][url] = 0
        app_mod.context['PluginInfo'].url_status = {url: 2}

        captured_titles = []

        class _FakeNoopUploader:
            def __init__(self, principal, data, **kwargs):
                self.data = data

            def start(self, allowed_files=None):
                captured_titles.append(self.data['format_title'])
                return [UploadBase.FileInfo(video=f, danmaku=None) for f in (allowed_files or [])]

        fake_files = [
            UploadBase.FileInfo(video=p1_name, danmaku=None),
            UploadBase.FileInfo(video=p2_name, danmaku=None),
        ]
        stream_info = {
            'name': streamer,
            'url': url,
            'title': '第二天重连标题',
            'date': day2_time,
        }

        with patch('biliup.handler.SessionLocal', self._TestSession), \
             patch('biliup.handler.UploadBase.file_list', return_value=fake_files), \
             patch('biliup.handler.uploaded'), \
             patch('biliup.uploader.Plugin.upload_plugins', {'Noop': _FakeNoopUploader}):
            handler_mod.process_upload(Event(handler_mod.UPLOAD, (stream_info,)))

        self.assertEqual(captured_titles, ['【赫萝老师录播】 2026-11-24 第二天重连标题'])

    def test_missing_title_and_first_part_not_in_db_uses_file_ctime_not_later_part_date(self):
        streamer = '【七海Nana7mi录播】'
        url = 'https://live.bilibili.com/21452505'
        p1_name = f'{streamer}2026-11-24 23_30_00.flv'
        p2_name = f'{streamer}2026-11-25 00_30_00.flv'
        for fname in (p1_name, p2_name):
            with open(fname, 'wb') as f:
                f.write(b'flvdata')

        day1_ts = time.mktime(time.strptime('2026-11-24 23:30:00', '%Y-%m-%d %H:%M:%S'))
        day2_time = time.strptime('2026-11-25 00:30:00', '%Y-%m-%d %H:%M:%S')

        with self._TestSession() as db:
            # 仅 p2 入库（模拟 p1 因异常 .part 恢复未入库）
            row2_id = db_mod.add_stream_info(db, streamer, url, day2_time)
            db_mod.update_room_title(db, row2_id, '歌回')
            db_mod.update_file_list(db, row2_id, p2_name)

        config['streamers'] = {
            streamer: {
                'url': url,
                'title': '{streamer} %Y-%m-%d {title}',
                'uploader': 'Noop',
            }
        }
        app_mod.context['url_upload_count'][url] = 0
        app_mod.context['PluginInfo'].url_status = {url: 2}

        captured_titles = []

        class _FakeNoopUploader:
            def __init__(self, principal, data, **kwargs):
                self.data = data

            def start(self, allowed_files=None):
                captured_titles.append(self.data['format_title'])
                return [UploadBase.FileInfo(video=f, danmaku=None) for f in (allowed_files or [])]

        fake_files = [
            UploadBase.FileInfo(video=p1_name, danmaku=None),
            UploadBase.FileInfo(video=p2_name, danmaku=None),
        ]
        # 模拟 singleton_check 巡检/重启触发的无 title、无 date 事件
        bare_stream_info = {'name': streamer, 'url': url}

        with patch('biliup.handler.SessionLocal', self._TestSession), \
             patch('biliup.handler.UploadBase.file_list', return_value=fake_files), \
             patch('biliup.handler.get_file_create_timestamp', return_value=day1_ts), \
             patch('biliup.handler.uploaded'), \
             patch('biliup.uploader.Plugin.upload_plugins', {'Noop': _FakeNoopUploader}):
            handler_mod.process_upload(Event(handler_mod.UPLOAD, (bare_stream_info,)))

        self.assertEqual(captured_titles, ['【七海Nana7mi录播】 2026-11-24 歌回'])

    def test_db_lookup_handles_duplicates_wildcards_and_remuxed_stem(self):
        with self._TestSession() as db:
            s_other = StreamerInfo(
                name='其他主播',
                url='https://live.bilibili.com/999',
                title='其他标题',
                date=datetime(2026, 11, 25, 1, 0, 0),
                live_cover_path='',
            )
            s_old = StreamerInfo(
                name='主播A',
                url='https://live.bilibili.com/111',
                title='旧场次',
                date=datetime(2026, 11, 23, 21, 0, 0),
                live_cover_path='',
            )
            s_day1 = StreamerInfo(
                name='主播A',
                url='https://live.bilibili.com/111',
                title='带通配符标题',
                date=datetime(2026, 11, 24, 23, 10, 0),
                live_cover_path='',
            )
            db.add_all([s_other, s_old, s_day1])
            db.flush()

            fn = '主播_100%_测试2026-11-24 23_10_00.flv'
            db.add_all([
                FileList(file=fn, streamer_info_id=s_old.id),
                FileList(file=fn, streamer_info_id=s_day1.id),
                FileList(file=fn, streamer_info_id=s_other.id),
                # 若 LIKE 未正确转义 _ 和 %，该行不应被误匹配
                FileList(file='主播X100abc测试2026-11-24 23_10_00.flv', streamer_info_id=s_other.id),
            ])
            db.commit()

            # 1. 精确文件名存在多行时不抛 MultipleResultsFound，且按 streamer_name 取最新行
            exact_res = db_mod.get_stream_info_by_filename(db, fn, streamer_name='主播A')
            self.assertEqual(exact_res['title'], '带通配符标题')
            self.assertEqual(exact_res['date'].tm_mday, 24)

            # 2. 后处理转码为 .mp4 后按 stem 匹配仍能准确命中同一条记录
            remuxed_res = db_mod.get_stream_info_by_filename(
                db,
                '主播_100%_测试2026-11-24 23_10_00.mp4',
                streamer_name='主播A',
            )
            self.assertEqual(remuxed_res['title'], '带通配符标题')
            self.assertEqual(remuxed_res['date'].tm_mday, 24)

            # 3. 含通配符但实际不存在的 stem 不应误匹配
            miss_res = db_mod.get_stream_info_by_filename(
                db,
                '主播_100xyz_测试2026-11-24 23_10_00.mp4',
                streamer_name='其他主播',
            )
            self.assertEqual(miss_res, {})

    def test_ffmpeg_download_nonzero_exit_registers_nonempty_part(self):
        streamer = '主播B'
        url = 'https://live.bilibili.com/222'
        day1_time = time.strptime('2026-11-24 23:50:00', '%Y-%m-%d %H:%M:%S')

        with self._TestSession() as db:
            row_id = db_mod.add_stream_info(db, streamer, url, day1_time)
            db_mod.update_room_title(db, row_id, '跨夜测试')

        dl = _FakeDummyDownloader(streamer, url, suffix='flv')
        dl.raw_stream_url = 'https://cdn.example.com/live/test.flv'
        dl.room_title = '跨夜测试'
        dl.database_row_id = row_id

        class _FakeProc:
            def __init__(self, args, **kwargs):
                out_part = args[-1]
                with open(out_part, 'wb') as f:
                    f.write(b'partial-flv-bytes')
                self.stdout = io.BytesIO(b'')
                self.returncode = 1

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with patch('biliup.engine.download.SessionLocal', self._TestSession), \
             patch('biliup.engine.download.subprocess.Popen', side_effect=lambda args, **kw: _FakeProc(args, **kw)), \
             patch.object(dl, 'gen_download_filename', return_value=f'{streamer}2026-11-24T23_50_00'):
            ok = dl.ffmpeg_download()

        self.assertFalse(ok)
        with self._TestSession() as db:
            info = db_mod.get_stream_info_by_filename(
                db, f'{streamer}2026-11-24T23_50_00.flv', streamer_name=streamer
            )
        self.assertEqual(info['title'], '跨夜测试')
        self.assertEqual(info['date'].tm_mday, 24)

    def test_missing_title_merges_first_part_cover_and_date_with_later_part_title(self):
        streamer = '【盖乃希亞Galaxy录播】'
        url = 'https://live.bilibili.com/22864117'
        p1_name = f'{streamer}2026-11-24 23_55_00.flv'
        p2_name = f'{streamer}2026-11-25 00_20_00.flv'
        for fname in (p1_name, p2_name):
            with open(fname, 'wb') as f:
                f.write(b'flvdata')

        day1_time = time.strptime('2026-11-24 23:55:00', '%Y-%m-%d %H:%M:%S')
        day2_time = time.strptime('2026-11-25 00:20:00', '%Y-%m-%d %H:%M:%S')

        with self._TestSession() as db:
            row1_id = db_mod.add_stream_info(db, streamer, url, day1_time)
            db_mod.update_cover_path(db, row1_id, 'cover_day1.jpg')
            db_mod.update_file_list(db, row1_id, p1_name)

            row2_id = db_mod.add_stream_info(db, streamer, url, day2_time)
            db_mod.update_room_title(db, row2_id, '跨夜聊天')
            db_mod.update_file_list(db, row2_id, p2_name)

        config['streamers'] = {
            streamer: {
                'url': url,
                'title': '{streamer} %Y-%m-%d {title}',
                'uploader': 'Noop',
            }
        }
        app_mod.context['url_upload_count'][url] = 0
        app_mod.context['PluginInfo'].url_status = {url: 2}

        captured_data = []

        class _FakeNoopUploader:
            def __init__(self, principal, data, **kwargs):
                self.data = data

            def start(self, allowed_files=None):
                captured_data.append(dict(self.data))
                return [UploadBase.FileInfo(video=f, danmaku=None) for f in (allowed_files or [])]

        fake_files = [
            UploadBase.FileInfo(video=p1_name, danmaku=None),
            UploadBase.FileInfo(video=p2_name, danmaku=None),
        ]
        bare_stream_info = {'name': streamer, 'url': url, 'title': ''}

        with patch('biliup.handler.SessionLocal', self._TestSession), \
             patch('biliup.handler.UploadBase.file_list', return_value=fake_files), \
             patch('biliup.handler.uploaded'), \
             patch('biliup.uploader.Plugin.upload_plugins', {'Noop': _FakeNoopUploader}):
            handler_mod.process_upload(Event(handler_mod.UPLOAD, (bare_stream_info,)))

        self.assertEqual(len(captured_data), 1)
        self.assertEqual(captured_data[0]['format_title'], '【盖乃希亞Galaxy录播】 2026-11-24 跨夜聊天')
        self.assertEqual(captured_data[0]['live_cover_path'], 'cover_day1.jpg')


if __name__ == '__main__':
    unittest.main()
