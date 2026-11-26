import asyncio
import copy
import importlib
import json
import multiprocessing as mp
import os
import shutil
import smtplib
import ssl
import sys
import tempfile
import threading
import time
import unittest
from email import message_from_string
from unittest.mock import MagicMock, patch, call
from collections import namedtuple

import requests

import tests  # noqa: F401
import biliup.app as app_mod
import biliup.handler as handler_mod
import biliup.plugins.bili_webup as bili_webup_mod
import biliup.plugins.biliuprs as biliuprs_mod
from biliup.config import config
from biliup.app import context
from biliup.common.notify import (
    send_notification,
    clear_cooldown,
    _send_email_sync,
    is_notify_enabled,
)
from biliup.engine.decorators import Plugin
from biliup.engine.download import DownloadBase
from biliup.engine.upload import UploadBase
from biliup.engine.event import Event
from biliup.handler import (
    clear_notified_exhausted_files,
    clear_pending_soft_upload_urls,
    is_unrecoverable_error,
    process,
    processed,
    process_upload,
    DOWNLOAD,
    DOWNLOADED,
    UPLOAD,
)
import biliup.uploader as uploader


class TestPR1UploadSafety(unittest.TestCase):
    def setUp(self):
        clear_cooldown()
        clear_notified_exhausted_files()
        clear_pending_soft_upload_urls()
        UploadBase.reset_last_claimed_videos()
        self._config_snapshot = {
            k: (v if k == 'PluginInfo' else copy.deepcopy(v))
            for k, v in config.data.items()
        }
        config['streamers'] = {}
        context['url_upload_count'] = {}
        context['upload_filename'] = []
        context['file_upload_count'] = {}
        context['sync_downloader_map'] = {}

    def tearDown(self):
        config.data.clear()
        config.data.update(self._config_snapshot)
        clear_cooldown()
        clear_notified_exhausted_files()
        clear_pending_soft_upload_urls()
        UploadBase.reset_last_claimed_videos()

    # 1. biliuprs.py upload_process exitcode 检查及 Pipe try/finally 资源关闭
    @patch('multiprocessing.get_context')
    @patch('multiprocessing.Pipe')
    def test_biliuprs_exitcode_nonzero_raises_and_closes_pipe(self, mock_pipe, mock_get_context):
        from biliup.plugins.biliuprs import BiliWeb

        mock_parent_conn = MagicMock()
        mock_child_conn = MagicMock()
        mock_pipe.return_value = (mock_parent_conn, mock_child_conn)

        mock_proc = MagicMock()
        mock_proc.exitcode = 1  # 模拟非零退出码（如 panic / OOM）
        mock_parent_conn.poll.return_value = False
        mock_ctx = MagicMock()
        mock_ctx.Process.return_value = mock_proc
        mock_get_context.return_value = mock_ctx

        bili_web = BiliWeb(
            'test_streamer',
            {'format_title': 'test_title', 'url': 'http://example.com'},
            user_cookie='fake_cookie.json'
        )

        fake_files = [UploadBase.FileInfo(video='test.mp4', danmaku=None)]

        with self.assertRaises(RuntimeError) as ctx_err:
            bili_web.upload(fake_files)

        self.assertIn("exitcode: 1", str(ctx_err.exception))
        mock_proc.start.assert_called_once()
        mock_proc.join.assert_called_once()
        # 确保 Pipe 在 finally 中被关闭
        mock_parent_conn.close.assert_called_once()
        mock_child_conn.close.assert_called_once()

    # 2. uploader.py: 不要吞掉异常，记录日志后向上 re-raise
    @patch('biliup.uploader.Plugin.upload_plugins')
    def test_uploader_reraises_exception(self, mock_upload_plugins):
        mock_plugin_cls = MagicMock()
        mock_instance = MagicMock()
        mock_instance.start.side_effect = RuntimeError("网络崩溃或子进程退出")
        mock_plugin_cls.return_value = mock_instance
        mock_upload_plugins.get.return_value = mock_plugin_cls

        config['streamers'] = {'test_streamer': {'uploader': 'bili_web'}}
        data = {'name': 'test_streamer', 'url': 'http://test.com', 'date': time.localtime()}

        with self.assertRaises(RuntimeError) as ctx_err:
            uploader.upload(data)

        self.assertIn("网络崩溃或子进程退出", str(ctx_err.exception))

    # 3. 错误分级函数 is_unrecoverable_error 识别
    def test_is_unrecoverable_error(self):
        # 不可自愈错误
        self.assertTrue(is_unrecoverable_error(RuntimeError("code: -101, message: 账号未登录")))
        self.assertTrue(is_unrecoverable_error("B站返回未登录"))
        self.assertTrue(is_unrecoverable_error("Cookie失效，请重新登录"))
        self.assertTrue(is_unrecoverable_error(FileNotFoundError("文件不存在: test.mp4")))
        self.assertTrue(is_unrecoverable_error(RuntimeError("error code: 21015, 风控拦截")))
        self.assertTrue(is_unrecoverable_error("code: 21016, 触发验证码"))
        self.assertTrue(is_unrecoverable_error("21001 标签不合法"))
        self.assertTrue(is_unrecoverable_error("21044 违禁分区"))
        self.assertTrue(is_unrecoverable_error("21045 参数非法"))
        self.assertTrue(is_unrecoverable_error('{"code":-101}'))
        self.assertTrue(is_unrecoverable_error("The system cannot find the file specified. (os error 2)"))
        self.assertTrue(is_unrecoverable_error("os error 2: file missing"))

        # 瞬态错误、资源临时受限 (os error 24/28) 及文件名/URL含数字不应误判为不可自愈（应返回 False）
        self.assertFalse(is_unrecoverable_error(ConnectionResetError("Connection reset by peer")))
        self.assertFalse(is_unrecoverable_error(TimeoutError("Read timed out")))
        self.assertFalse(is_unrecoverable_error("502 Bad Gateway"))
        self.assertFalse(is_unrecoverable_error("Too many open files (os error 24)"))
        self.assertFalse(is_unrecoverable_error("No space left on device (os error 28)"))
        self.assertFalse(is_unrecoverable_error("Resource temporarily unavailable (os error 20)"))
        self.assertFalse(is_unrecoverable_error(RuntimeError("upload failed for clip_21001_room-101.flv: 502 Bad Gateway")))
        self.assertFalse(is_unrecoverable_error(None))

    # 4. handler.py process_upload: 子进程退出或上传失败绝不调用 uploaded() 删录像
    @patch('biliup.handler.uploaded')
    @patch('biliup.handler.upload')
    @patch('biliup.handler.UploadBase.file_list')
    @patch('biliup.handler.UploadBase.get_max_upload_count')
    def test_process_upload_failure_does_not_delete_files(self, mock_get_count, mock_file_list, mock_upload, mock_uploaded):
        mock_file = UploadBase.FileInfo(video='test_video.mp4', danmaku=None)
        mock_file_list.return_value = [mock_file]
        mock_get_count.return_value = 0
        mock_upload.side_effect = RuntimeError("子进程异常退出 exitcode: 1")

        context['url_upload_count']['http://test.com'] = 0
        context['PluginInfo'] = MagicMock()
        context['PluginInfo'].url_status = {'http://test.com': 2}

        stream_info = {'name': 'test_streamer', 'url': 'http://test.com', 'title': 'test'}
        process_upload(Event(UPLOAD, (stream_info,)))

        # 确认确实执行了 upload 且失败后绝对不能调用 uploaded 删除录像文件
        mock_upload.assert_called_once()
        mock_uploaded.assert_not_called()

    # 5. handler.py process_upload: 达到重试上限绝不调用 uploaded() 删录像，并发送告警
    @patch('biliup.handler.send_notification')
    @patch('biliup.handler.uploaded')
    @patch('biliup.handler.upload')
    @patch('biliup.handler.UploadBase.file_list')
    @patch('biliup.handler.UploadBase.get_max_upload_count')
    def test_process_upload_max_limit_reached_does_not_delete_files(self, mock_get_count, mock_file_list, mock_upload, mock_uploaded, mock_send_notify):
        mock_file = UploadBase.FileInfo(video='test_video.mp4', danmaku=None)
        mock_file_list.return_value = [mock_file]
        config['max_upload_limit'] = 3
        mock_get_count.return_value = 3  # 已经达到阈值

        context['url_upload_count']['http://test.com'] = 0
        context['PluginInfo'] = MagicMock()
        context['PluginInfo'].url_status = {'http://test.com': 2}

        stream_info = {'name': 'test_streamer', 'url': 'http://test.com', 'title': 'test'}
        process_upload(Event(UPLOAD, (stream_info,)))

        # 不会上传，也不会删除录像
        mock_upload.assert_not_called()
        mock_uploaded.assert_not_called()
        # 触发达到重试上限告警
        mock_send_notify.assert_called_once()
        self.assertEqual(mock_send_notify.call_args.kwargs['error_type'], 'max_upload_limit_reached')

    # 6. handler.py process_upload: 不可自愈 -101 错误首次熔断、写回最大计数、传真实错误至 Webhook/SMTP
    @patch('requests.post')
    @patch('biliup.handler.send_notification')
    @patch('biliup.handler.uploaded')
    @patch('biliup.handler.upload')
    @patch('biliup.handler.UploadBase.file_list')
    @patch('biliup.handler.UploadBase.get_max_upload_count')
    def test_unrecoverable_error_circuit_break_and_webhook_real_error(self, mock_get_count, mock_file_list, mock_upload, mock_uploaded, mock_send_notify, mock_requests_post):
        mock_file = UploadBase.FileInfo(video='test_video.mp4', danmaku=None)
        mock_file_list.return_value = [mock_file]
        config['max_upload_limit'] = 5
        config['streamers'] = {
            'test_streamer': {
                'postprocessor': [{'webhook': 'http://my-webhook.test/api'}]
            }
        }
        mock_get_count.return_value = 0
        error_msg = "bilibili api error: -101 账号未登录"
        mock_upload.side_effect = RuntimeError(error_msg)

        context['url_upload_count']['http://test.com'] = 0
        context['file_upload_count'] = {'test_video.mp4': 0}
        context['PluginInfo'] = MagicMock()
        context['PluginInfo'].url_status = {'http://test.com': 2}

        mock_post_res = MagicMock()
        mock_post_res.text = "success"
        mock_requests_post.return_value = mock_post_res

        stream_info = {'name': 'test_streamer', 'url': 'http://test.com', 'title': 'test'}
        process_upload(Event(UPLOAD, (stream_info,)))

        # 1. 验证 uploaded 未被调用
        mock_uploaded.assert_not_called()

        # 2. 验证文件上传计数被置为 max_upload_limit，实现首次熔断不再重试
        self.assertEqual(context['file_upload_count']['test_video.mp4'], 5)

        # 3. 验证 Webhook 接收到了真实 caught_error，且包含 timeout=15
        mock_requests_post.assert_called_once()
        post_kwargs = mock_requests_post.call_args.kwargs
        self.assertEqual(post_kwargs.get('timeout'), 15)
        self.assertEqual(post_kwargs.get('url'), 'http://my-webhook.test/api')
        self.assertIn(error_msg, post_kwargs.get('json', {}).get('caught_error', ''))

        # 4. 验证发送了不可自愈告警通知
        mock_send_notify.assert_called_once()
        self.assertEqual(mock_send_notify.call_args.kwargs['error_type'], 'unrecoverable_upload_error')
        self.assertIn(error_msg, mock_send_notify.call_args.kwargs['content'])

    # 7. 邮件告警 notify.py: 支持 SSL 465 / STARTTLS 587，防轰炸冷却机制
    @patch('smtplib.SMTP_SSL')
    @patch('smtplib.SMTP')
    def test_notify_email_ssl_and_starttls_and_cooldown(self, mock_smtp_cls, mock_smtp_ssl_cls):
        mock_ssl_inst = MagicMock()
        mock_smtp_ssl_cls.return_value = mock_ssl_inst

        mock_tls_inst = MagicMock()
        mock_smtp_cls.return_value = mock_tls_inst

        config['notify_email_enabled'] = True
        config['notify_smtp_host'] = 'smtp.qq.com'
        config['notify_smtp_user'] = 'test@qq.com'
        config['notify_smtp_pass'] = 'auth_code'
        config['notify_email_to'] = 'receiver@qq.com'

        # 465 端口走 SSL，且显式传入 ssl.SSLContext 校验证书
        config['notify_smtp_port'] = 465
        clear_cooldown()
        res1 = send_notification("标题1", "正文1", streamer_name="主播A", error_type="oom", sync=True)
        self.assertTrue(res1)
        mock_smtp_ssl_cls.assert_called_once()
        ssl_args, ssl_kwargs = mock_smtp_ssl_cls.call_args
        self.assertEqual(ssl_args, ('smtp.qq.com', 465))
        self.assertEqual(ssl_kwargs.get('timeout'), 15)
        self.assertIsInstance(ssl_kwargs.get('context'), ssl.SSLContext)
        mock_ssl_inst.login.assert_called_with('test@qq.com', 'auth_code')
        mock_ssl_inst.sendmail.assert_called_once()
        sent_msg = message_from_string(mock_ssl_inst.sendmail.call_args[0][2])
        self.assertEqual(sent_msg['From'], 'biliup <test@qq.com>')
        self.assertEqual(sent_msg['To'], 'receiver@qq.com')

        # 同主播 + 同类错误 30 分钟冷却
        res_cooldown = send_notification("标题1-重复", "正文1-重复", streamer_name="主播A", error_type="oom", sync=True)
        self.assertFalse(res_cooldown)  # 被冷却拦截
        self.assertEqual(mock_ssl_inst.sendmail.call_count, 1)

        # 不同错误类型不拦截
        res_diff_error = send_notification("标题2", "正文2", streamer_name="主播A", error_type="disk_full", sync=True)
        self.assertTrue(res_diff_error)

        # 587 端口走 STARTTLS，且显式传入 ssl.SSLContext
        config['notify_smtp_port'] = 587
        clear_cooldown()
        res_587 = send_notification("标题3", "正文3", streamer_name="主播B", error_type="test", sync=True)
        self.assertTrue(res_587)
        mock_smtp_cls.assert_called_with('smtp.qq.com', 587, timeout=15)
        mock_tls_inst.starttls.assert_called_once()
        self.assertIsInstance(mock_tls_inst.starttls.call_args.kwargs.get('context'), ssl.SSLContext)
        mock_tls_inst.login.assert_called_with('test@qq.com', 'auth_code')

        # 非 465/587 端口（如 25）同样强制 STARTTLS；若服务器不支持 STARTTLS 则严禁明文 login()
        config['notify_smtp_port'] = 25
        clear_cooldown()
        mock_plain_inst = MagicMock()
        mock_plain_inst.starttls.side_effect = smtplib.SMTPNotSupportedError("STARTTLS extension not supported")
        mock_smtp_cls.return_value = mock_plain_inst
        res_25 = send_notification("标题4", "正文4", streamer_name="主播C", error_type="test", sync=True)
        self.assertFalse(res_25)
        mock_plain_inst.starttls.assert_called_once()
        mock_plain_inst.login.assert_not_called()
        mock_plain_inst.sendmail.assert_not_called()

    # 8. 磁盘保护：软水位与硬水位触发逻辑及 OSError 容错
    @patch('shutil.disk_usage')
    def test_disk_soft_and_hard_watermarks(self, mock_disk_usage):
        Usage = namedtuple('Usage', ['total', 'used', 'free'])

        class DummyDownload(DownloadBase):
            def __init__(self, fname, url):
                self.fname = fname
                self.url = url
                self.room_title = "测试直播间"
                self.plugin_msg = "test"
                self.live_cover_path = None
                self.database_row_id = 1
                self.segment_processor_thread = []
                self.is_download = False
                self.platform = "dummy"
                self.use_live_cover = False
                self.live_cover_url = None
                self.callback_called = 0

            async def acheck_stream(self, is_check=False):
                return True

            def download_cover(self, fmtname):
                pass

            def download_success_callback(self):
                self.callback_called += 1

            def gen_download_filename(self):
                return "test"

            def close(self):
                pass

        downloader = DummyDownload("test_streamer", "http://live.test/123")

        # 测试 8.1: 硬水位 should_record 逻辑
        # 配置硬水位 5GB，当前剩余 2GB -> 拒绝录制，触发告警
        config['disk_stop_threshold_gb'] = 5.0
        mock_disk_usage.return_value = Usage(total=100 * 1024**3, used=98 * 1024**3, free=2 * 1024**3)

        with patch('biliup.common.notify.send_notification') as mock_notify:
            can_record = downloader.should_record()
            self.assertFalse(can_record)
            mock_notify.assert_called_once()
            self.assertEqual(mock_notify.call_args.kwargs['error_type'], 'disk_stop_threshold_reached')

        # 当前剩余 10GB (> 5GB) -> 允许录制
        mock_disk_usage.return_value = Usage(total=100 * 1024**3, used=90 * 1024**3, free=10 * 1024**3)
        self.assertTrue(downloader.should_record())

        # disk_usage 抛出 OSError 时不应阻断 should_record
        mock_disk_usage.side_effect = OSError("disk stats unavailable")
        self.assertTrue(downloader.should_record())
        mock_disk_usage.side_effect = None

        # 测试 8.2: 软水位高于阈值时不提前 break，直至主播下播(run 返回 False)
        config['disk_upload_threshold_gb'] = 10.0
        mock_disk_usage.return_value = Usage(total=100 * 1024**3, used=80 * 1024**3, free=20 * 1024**3)
        above_runs = 0

        def fake_run_above():
            nonlocal above_runs
            above_runs += 1
            return above_runs < 3  # 前两段继续录制，第 3 次下播退出

        downloader.run = fake_run_above
        with patch('biliup.engine.download.SessionLocal'):
            normal_info = downloader.start()
        self.assertEqual(above_runs, 3)
        self.assertFalse(normal_info['disk_soft_triggered'])

        # 测试 8.3: 软水位低于阈值时 start 循环提前 break 且标记 disk_soft_triggered=True
        # 配置软水位 10GB，当前剩余 6GB -> run() 一次后检测到软水位不足，主动 break
        mock_disk_usage.return_value = Usage(total=100 * 1024**3, used=94 * 1024**3, free=6 * 1024**3)

        run_count = 0
        def fake_run():
            nonlocal run_count
            run_count += 1
            return True  # 模拟正常录完一个分段，原本还想继续录

        downloader.run = fake_run

        with patch('biliup.engine.download.SessionLocal'):
            stream_info = downloader.start()

        # 仅运行了一次就主动 break，未进入死循环，且返回 disk_soft_triggered=True
        self.assertEqual(run_count, 1)
        self.assertEqual(stream_info['name'], "test_streamer")
        self.assertTrue(stream_info['disk_soft_triggered'])

        # 测试 8.4: 软水位检测抛出 OSError 时不中断 start() 及 download_success_callback()
        mock_disk_usage.side_effect = OSError("disk stats unavailable")
        err_runs = 0

        def fake_run_oserror():
            nonlocal err_runs
            err_runs += 1
            return err_runs < 2

        downloader.run = fake_run_oserror
        before_cb = downloader.callback_called
        with patch('biliup.engine.download.SessionLocal'):
            downloader.start()
        self.assertEqual(err_runs, 2)
        self.assertEqual(downloader.callback_called, before_cb + 1)

    # 9. 已熔断/耗尽的旧文件不得阻断同批新录制的上传
    @patch('biliup.handler.uploaded')
    @patch('biliup.handler.upload')
    @patch('biliup.handler.UploadBase.file_list')
    @patch('biliup.handler.UploadBase.get_max_upload_count')
    def test_exhausted_file_does_not_block_fresh_file_in_same_scan(
        self, mock_get_count, mock_file_list, mock_upload, mock_uploaded
    ):
        burned = UploadBase.FileInfo(video='burned.mp4', danmaku=None)
        fresh = UploadBase.FileInfo(video='fresh.mp4', danmaku=None)
        mock_file_list.return_value = [burned, fresh]
        config['max_upload_limit'] = 3
        context['file_upload_count'] = {'burned.mp4': 3, 'fresh.mp4': 0}
        context['url_upload_count']['http://test.com'] = 0
        context['PluginInfo'] = MagicMock()
        context['PluginInfo'].url_status = {'http://test.com': 2}

        mock_get_count.return_value = 0
        mock_upload.return_value = [fresh]

        stream_info = {'name': 'test_streamer', 'url': 'http://test.com', 'title': 'test'}
        process_upload(Event(UPLOAD, (stream_info,)))

        mock_get_count.assert_called_once_with([fresh])
        mock_upload.assert_called_once()
        self.assertEqual(mock_upload.call_args.kwargs.get('target_files'), ['fresh.mp4'])
        mock_uploaded.assert_called_once_with('test_streamer', None, [fresh])

        # 验证 UploadBase.start(allowed_files=...) 仅上传目标新文件
        class DummyUploader(UploadBase):
            def __init__(self, principal, data):
                super().__init__(principal, data)
                self.uploaded_batches = []

            def upload(self, file_list):
                self.uploaded_batches.append(file_list)
                return file_list

        dummy_up = DummyUploader('test_streamer', {'format_title': 'test_title'})
        res = dummy_up.start(allowed_files=['fresh.mp4'])
        self.assertEqual(res, [fresh])
        self.assertEqual(dummy_up.uploaded_batches, [[fresh]])

    # 10. 末次允许的瞬态重试失败应立即发送达到重试上限告警，无需等下一次事件
    @patch('biliup.handler.send_notification')
    @patch('biliup.handler.uploaded')
    @patch('biliup.handler.upload')
    @patch('biliup.handler.UploadBase.file_list')
    @patch('biliup.handler.UploadBase.get_max_upload_count')
    def test_final_transient_attempt_immediately_sends_limit_alert(
        self, mock_get_count, mock_file_list, mock_upload, mock_uploaded, mock_send_notify
    ):
        mock_file = UploadBase.FileInfo(video='test_video.mp4', danmaku=None)
        mock_file_list.return_value = [mock_file]
        config['max_upload_limit'] = 3
        # 第 3 次（最后 1 次允许的尝试）前计数为 2
        mock_get_count.return_value = 2
        mock_upload.side_effect = TimeoutError("Read timed out")

        context['url_upload_count']['http://test.com'] = 0
        context['PluginInfo'] = MagicMock()
        context['PluginInfo'].url_status = {'http://test.com': 2}

        stream_info = {'name': 'test_streamer', 'url': 'http://test.com', 'title': 'test'}
        process_upload(Event(UPLOAD, (stream_info,)))

        mock_upload.assert_called_once()
        mock_uploaded.assert_not_called()
        mock_send_notify.assert_called_once()
        self.assertEqual(mock_send_notify.call_args.kwargs['error_type'], 'max_upload_limit_reached')
        self.assertIn('3/3', mock_send_notify.call_args.kwargs['content'])

    # 11. 软水位切稿 (disk_soft_triggered=True) 应穿透 delay 立即上传并删录像
    @patch('biliup.handler.time.sleep')
    @patch('biliup.handler.uploaded')
    @patch('biliup.handler.upload')
    @patch('biliup.handler.UploadBase.file_list')
    @patch('biliup.handler.UploadBase.get_max_upload_count')
    def test_disk_soft_triggered_skips_delay_and_uploads(
        self, mock_get_count, mock_file_list, mock_upload, mock_uploaded, mock_sleep
    ):
        mock_file = UploadBase.FileInfo(video='soft_cut.mp4', danmaku=None)
        mock_file_list.return_value = [mock_file]
        mock_get_count.return_value = 0
        mock_upload.return_value = [mock_file]

        config['delay'] = 30
        context['url_upload_count']['http://test.com'] = 0
        context['PluginInfo'] = MagicMock()
        # 模拟新一场录制已经抢占 url_status=1
        context['PluginInfo'].url_status = {'http://test.com': 1}

        stream_info = {
            'name': 'test_streamer',
            'url': 'http://test.com',
            'title': 'test',
            'disk_soft_triggered': True,
        }
        process_upload(Event(UPLOAD, (stream_info,)))

        mock_sleep.assert_not_called()
        mock_upload.assert_called_once()
        mock_uploaded.assert_called_once_with('test_streamer', None, [mock_file])

    # 12. -101 熔断后保留文件连续巡检不重复发 max_upload_limit_reached，清空新文件失败可再告警
    @patch('biliup.handler.send_notification')
    @patch('biliup.handler.uploaded')
    @patch('biliup.handler.upload')
    @patch('biliup.handler.UploadBase.file_list')
    @patch('biliup.handler.UploadBase.get_max_upload_count')
    def test_unrecoverable_circuit_break_no_repeat_alert_until_file_cleared(
        self, mock_get_count, mock_file_list, mock_upload, mock_uploaded, mock_send_notify
    ):
        file_a = UploadBase.FileInfo(video='bad_a.mp4', danmaku=None)
        file_b = UploadBase.FileInfo(video='bad_b.mp4', danmaku=None)
        config['max_upload_limit'] = 5
        context['url_upload_count']['http://test.com'] = 0
        context['file_upload_count'] = {'bad_a.mp4': 0}
        context['PluginInfo'] = MagicMock()
        context['PluginInfo'].url_status = {'http://test.com': 2}

        mock_get_count.return_value = 0
        mock_file_list.return_value = [file_a]
        mock_upload.side_effect = RuntimeError("code: -101, message: 账号未登录")

        stream_info = {'name': 'test_streamer', 'url': 'http://test.com', 'title': 'test'}

        # 第 1 次触发 -101 熔断并告警
        process_upload(Event(UPLOAD, (stream_info,)))
        self.assertEqual(mock_send_notify.call_count, 1)
        self.assertEqual(mock_send_notify.call_args.kwargs['error_type'], 'unrecoverable_upload_error')

        # 第 2 次巡检，磁盘仍留有 bad_a.mp4，不得重复发送 max_upload_limit_reached
        process_upload(Event(UPLOAD, (stream_info,)))
        self.assertEqual(mock_send_notify.call_count, 1)
        mock_uploaded.assert_not_called()

        # 磁盘文件清空，自动清理已告警记录
        mock_file_list.return_value = []
        process_upload(Event(UPLOAD, (stream_info,)))
        self.assertEqual(mock_send_notify.call_count, 1)

        # 新文件出现并触发失败，可以正常再次告警
        context['file_upload_count'] = {'bad_b.mp4': 0}
        mock_file_list.return_value = [file_b]
        process_upload(Event(UPLOAD, (stream_info,)))
        self.assertEqual(mock_send_notify.call_count, 2)
        self.assertEqual(mock_send_notify.call_args.kwargs['error_type'], 'unrecoverable_upload_error')

    # 13. max_upload_limit=None 和 delay=None 且 url 缺失于 url_status 时不抛 TypeError/KeyError
    @patch('biliup.handler.uploaded')
    @patch('biliup.handler.upload')
    @patch('biliup.handler.UploadBase.file_list')
    @patch('biliup.handler.UploadBase.get_max_upload_count')
    def test_none_max_upload_limit_and_none_delay_defaults(
        self, mock_get_count, mock_file_list, mock_upload, mock_uploaded
    ):
        mock_file = UploadBase.FileInfo(video='ok.mp4', danmaku=None)
        mock_file_list.return_value = [mock_file]
        mock_get_count.return_value = 0
        mock_upload.return_value = [mock_file]

        config['max_upload_limit'] = None
        config['delay'] = None
        context['url_upload_count']['http://missing.com'] = 0
        context['PluginInfo'] = MagicMock()
        context['PluginInfo'].url_status = {}

        stream_info = {'name': 'test_streamer', 'url': 'http://missing.com', 'title': 'test'}
        process_upload(Event(UPLOAD, (stream_info,)))

        mock_upload.assert_called_once()
        mock_uploaded.assert_called_once_with('test_streamer', None, [mock_file])

    # 14. upload() 返回 None（并发占用）时不触发假成功 Webhook，并回退 file_upload_count
    @patch('requests.post')
    @patch('biliup.handler.uploaded')
    @patch('biliup.handler.upload')
    @patch('biliup.handler.UploadBase.file_list')
    def test_upload_none_rolls_back_file_count_and_skips_webhook(
        self, mock_file_list, mock_upload, mock_uploaded, mock_requests_post
    ):
        orig_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            try:
                video_name = 'concurrent_claimed.mp4'
                with open(video_name, 'wb') as f:
                    f.write(b'video')
                mock_file = UploadBase.FileInfo(video=video_name, danmaku=None)
                mock_file_list.return_value = [mock_file]
                mock_upload.return_value = None

                config['max_upload_limit'] = 5
                config['streamers'] = {
                    'test_streamer': {
                        'postprocessor': [{'webhook': 'http://my-webhook.test/api'}]
                    }
                }
                context['url_upload_count']['http://test.com'] = 0
                context['file_upload_count'] = {video_name: 0}
                context['PluginInfo'] = MagicMock()
                context['PluginInfo'].url_status = {'http://test.com': 2}

                stream_info = {'name': 'test_streamer', 'url': 'http://test.com', 'title': 'test'}
                process_upload(Event(UPLOAD, (stream_info,)))

                mock_upload.assert_called_once()
                mock_uploaded.assert_not_called()
                mock_requests_post.assert_not_called()
                self.assertEqual(context['file_upload_count'][video_name], 0)
            finally:
                os.chdir(orig_cwd)

    # 15. notify.py 清洗主机/收件人首尾空白、原样保留密码空白、支持多收件人并记录部分拒收
    @patch('smtplib.SMTP_SSL')
    def test_notify_email_strips_whitespace_and_multiple_recipients(self, mock_smtp_ssl_cls):
        mock_ssl_inst = MagicMock()
        mock_ssl_inst.sendmail.return_value = {}
        mock_smtp_ssl_cls.return_value = mock_ssl_inst

        config['notify_email_enabled'] = True
        config['notify_smtp_port'] = 465
        config['notify_smtp_host'] = '  smtp.qq.com  '
        config['notify_smtp_user'] = ' a@qq.com '
        config['notify_smtp_pass'] = ' secret_pass '
        config['notify_email_to'] = ' b@qq.com, c@qq.com ; d@qq.com '

        clear_cooldown()
        ok = send_notification("多收件人标题", "正文", streamer_name="主播M", error_type="test", sync=True)
        self.assertTrue(ok)
        mock_smtp_ssl_cls.assert_called_once()
        self.assertEqual(mock_smtp_ssl_cls.call_args.args, ('smtp.qq.com', 465))
        mock_ssl_inst.login.assert_called_once_with('a@qq.com', ' secret_pass ')
        mock_ssl_inst.sendmail.assert_called_once()
        from_addr, recipients, raw_msg = mock_ssl_inst.sendmail.call_args.args
        self.assertEqual(from_addr, 'a@qq.com')
        self.assertEqual(recipients, ['b@qq.com', 'c@qq.com', 'd@qq.com'])
        sent_msg = message_from_string(raw_msg)
        self.assertEqual(sent_msg['To'], 'b@qq.com, c@qq.com, d@qq.com')

        # 部分收件人被拒收时记录 warning 日志
        clear_cooldown()
        mock_ssl_inst.sendmail.return_value = {'c@qq.com': (450, b'Mailbox busy')}
        with self.assertLogs('biliup', level='WARNING') as log_ctx:
            ok_partial = send_notification("部分拒收标题", "正文", streamer_name="主播M2", error_type="test", sync=True)
        self.assertTrue(ok_partial)
        self.assertTrue(any('部分收件人被拒收' in msg and 'c@qq.com' in msg for msg in log_ctx.output))

        # 仅空白字符串应判定为配置不完整并跳过发送
        config['notify_email_to'] = '  , ;  '
        clear_cooldown()
        self.assertFalse(send_notification("空收件人", "正文", streamer_name="主播M", error_type="test", sync=True))

    # 16. SMTP 发送失败触发 on_failure 撤回已告警标记，下次巡检可重新告警；未开启或冷却期不触发 on_failure
    @patch('smtplib.SMTP_SSL')
    @patch('biliup.handler.uploaded')
    @patch('biliup.handler.upload')
    @patch('biliup.handler.UploadBase.file_list')
    def test_process_upload_retries_alert_after_smtp_failure(
        self, mock_file_list, mock_upload, mock_uploaded, mock_smtp_ssl_cls
    ):
        def _wait_notify_threads():
            for t in list(threading.enumerate()):
                if t.name.startswith('NotifyThread-'):
                    t.join(timeout=5)

        # 未开启告警或冷却期内跳过时不得触发 on_failure
        callback_hits = []
        config['notify_email_enabled'] = False
        send_notification(
            "未开启", "正文", streamer_name="主播X", error_type="test",
            sync=True, on_failure=lambda: callback_hits.append('disabled')
        )
        self.assertEqual(callback_hits, [])

        config['notify_email_enabled'] = True
        config['notify_smtp_port'] = 465
        config['notify_smtp_host'] = 'smtp.qq.com'
        config['notify_smtp_user'] = 'a@qq.com'
        config['notify_smtp_pass'] = 'pass'
        config['notify_email_to'] = 'b@qq.com'

        mock_ok_inst = MagicMock()
        mock_ok_inst.sendmail.return_value = {}
        mock_smtp_ssl_cls.return_value = mock_ok_inst
        send_notification("首次", "正文", streamer_name="主播X", error_type="test", sync=True)
        send_notification(
            "冷却跳过", "正文", streamer_name="主播X", error_type="test",
            sync=True, on_failure=lambda: callback_hits.append('cooldown')
        )
        self.assertEqual(callback_hits, [])

        # process_upload: 首次 SMTP 连接失败 -> on_failure 撤回标记 -> 下次巡检重新发送并成功 -> 第3次不再重复发
        clear_cooldown()
        clear_notified_exhausted_files()
        mock_smtp_ssl_cls.reset_mock()
        mock_file = UploadBase.FileInfo(video='stuck.mp4', danmaku=None)
        mock_file_list.return_value = [mock_file]
        config['max_upload_limit'] = 3
        context['file_upload_count'] = {'stuck.mp4': 3}
        context['url_upload_count']['http://test.com'] = 0
        context['PluginInfo'] = MagicMock()
        context['PluginInfo'].url_status = {'http://test.com': 2}

        mock_smtp_ssl_cls.side_effect = [OSError("SMTP connect timeout"), mock_ok_inst]
        stream_info = {'name': 'test_streamer', 'url': 'http://test.com', 'title': 'test'}

        # 第 1 次巡检：SMTP 抛异常 -> 触发 on_failure 撤回标记
        process_upload(Event(UPLOAD, (stream_info,)))
        _wait_notify_threads()
        mock_upload.assert_not_called()
        mock_uploaded.assert_not_called()
        self.assertEqual(mock_smtp_ssl_cls.call_count, 1)

        # 第 2 次巡检：因为上次发送失败已撤回标记，本次允许重新发送并成功
        process_upload(Event(UPLOAD, (stream_info,)))
        _wait_notify_threads()
        self.assertEqual(mock_smtp_ssl_cls.call_count, 2)
        mock_ok_inst.sendmail.assert_called_once()

        # 第 3 次巡检：上次已成功告警且文件仍在，不再重复发送
        process_upload(Event(UPLOAD, (stream_info,)))
        _wait_notify_threads()
        self.assertEqual(mock_smtp_ssl_cls.call_count, 2)

        # 不可自愈错误分支首次发送失败同样会撤回标记，下次巡检重发成功
        clear_cooldown()
        clear_notified_exhausted_files()
        mock_bad_file = UploadBase.FileInfo(video='bad_unrec.mp4', danmaku=None)
        mock_file_list.return_value = [mock_bad_file]
        context['file_upload_count'] = {'bad_unrec.mp4': 0}
        mock_upload.side_effect = RuntimeError("code: -101, message: 账号未登录")
        mock_ok_inst2 = MagicMock()
        mock_ok_inst2.sendmail.return_value = {}
        mock_smtp_ssl_cls.side_effect = [OSError("SMTP network down"), mock_ok_inst2]

        with patch('biliup.handler.UploadBase.get_max_upload_count', return_value=0):
            process_upload(Event(UPLOAD, (stream_info,)))
            _wait_notify_threads()
        self.assertEqual(mock_smtp_ssl_cls.call_count, 3)

        process_upload(Event(UPLOAD, (stream_info,)))
        _wait_notify_threads()
        self.assertEqual(mock_smtp_ssl_cls.call_count, 4)
        mock_ok_inst2.sendmail.assert_called_once()

    # 13. P1-4: bili_web 在 -101 或缺失 cookie 文件时不吞异常并命中首次熔断；成功 Cookie 登录不覆写磁盘文件；store 原子写入并清理临时文件
    def test_bili_web_unrecoverable_circuit_break_and_cookie_file_preservation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            cookie_path = os.path.join(tmpdir, '12345.json')
            raw_cookie_text = json.dumps(
                {
                    'cookie_info': {
                        'cookies': [
                            {'name': 'SESSDATA', 'value': 'sess_1'},
                            {'name': 'bili_jct', 'value': 'jct_1'},
                        ]
                    },
                    'token_info': {
                        'access_token': 'acc_tok',
                        'refresh_token': 'ref_tok',
                    },
                },
                indent=2,
            )
            with open(cookie_path, 'w', encoding='utf-8') as f:
                f.write(raw_cookie_text)

            # 13a. 正常 Cookie 登录不调用 store() 且不改写/截断磁盘 Cookie 文件
            bili = bili_webup_mod.BiliBili(bili_webup_mod.Data())
            ok_nav = MagicMock()
            ok_nav.json.return_value = {'code': 0, 'data': {'isLogin': True}}
            with patch.object(bili._BiliBili__session, 'get', return_value=ok_nav), \
                 patch.object(bili, 'store', wraps=bili.store) as store_spy:
                bili.login('bili.cookie', cookie_path)
            store_spy.assert_not_called()
            with open(cookie_path, 'r', encoding='utf-8') as f:
                self.assertEqual(f.read(), raw_cookie_text)

            # 13b. bili_web 上传在 nav 返回 -101 时触发首次熔断
            mock_file_101 = UploadBase.FileInfo(video='web_101.mp4', danmaku=None)
            config['max_upload_limit'] = 5
            config['streamers'] = {
                'test_streamer': {
                    'url': 'http://test.com',
                    'uploader': 'bili_web',
                    'user_cookie': cookie_path,
                    'title': 'test',
                }
            }
            context['url_upload_count']['http://test.com'] = 0
            context['file_upload_count'] = {'web_101.mp4': 0}
            context['PluginInfo'] = MagicMock()
            context['PluginInfo'].url_status = {'http://test.com': 2}

            err_nav = MagicMock()
            err_nav.json.return_value = {'code': -101, 'message': '账号未登录'}
            with patch('biliup.handler.SessionLocal', side_effect=RuntimeError('skip db')), \
                 patch('biliup.engine.upload.UploadBase.file_list', return_value=[mock_file_101]), \
                 patch('biliup.handler.UploadBase.get_max_upload_count', return_value=0), \
                 patch('biliup.handler.uploaded') as mock_uploaded, \
                 patch('biliup.handler.send_notification') as mock_notify, \
                 patch.object(requests.Session, 'get', return_value=err_nav):
                process_upload(Event(UPLOAD, ({'name': 'test_streamer', 'url': 'http://test.com', 'title': 'test'},)))

            mock_uploaded.assert_not_called()
            self.assertEqual(context['file_upload_count']['web_101.mp4'], 5)
            mock_notify.assert_called_once()
            self.assertEqual(mock_notify.call_args.kwargs['error_type'], 'unrecoverable_upload_error')
            self.assertIn('-101', mock_notify.call_args.kwargs['content'])

            # 13c. bili_web 上传在 cookie 文件缺失时抛 FileNotFoundError 并触发首次熔断
            clear_cooldown()
            clear_notified_exhausted_files()
            missing_cookie = os.path.join(tmpdir, 'missing_cookie.json')
            mock_file_missing = UploadBase.FileInfo(video='web_missing.mp4', danmaku=None)
            config['streamers']['test_streamer']['user_cookie'] = missing_cookie
            context['file_upload_count'] = {'web_missing.mp4': 0}

            with patch('biliup.handler.SessionLocal', side_effect=RuntimeError('skip db')), \
                 patch('biliup.engine.upload.UploadBase.file_list', return_value=[mock_file_missing]), \
                 patch('biliup.handler.UploadBase.get_max_upload_count', return_value=0), \
                 patch('biliup.handler.uploaded') as mock_uploaded, \
                 patch('biliup.handler.send_notification') as mock_notify:
                process_upload(Event(UPLOAD, ({'name': 'test_streamer', 'url': 'http://test.com', 'title': 'test'},)))

            mock_uploaded.assert_not_called()
            self.assertEqual(context['file_upload_count']['web_missing.mp4'], 5)
            mock_notify.assert_called_once()
            self.assertEqual(mock_notify.call_args.kwargs['error_type'], 'unrecoverable_upload_error')

            # 13d. store() 原子写入并在替换失败时清理临时文件且保留原文件
            bili_store = bili_webup_mod.BiliBili(bili_webup_mod.Data())
            bili_store.persistence_path = cookie_path
            bili_store.cookies = {'SESSDATA': 'new_sess'}
            bili_store.access_token = 'new_acc'
            bili_store.refresh_token = 'new_ref'

            with patch.object(bili_webup_mod.os, 'replace', side_effect=OSError('replace failed')):
                with self.assertRaises(OSError):
                    bili_store.store()
            with open(cookie_path, 'r', encoding='utf-8') as f:
                self.assertEqual(f.read(), raw_cookie_text)
            self.assertEqual([n for n in os.listdir(tmpdir) if n.startswith('.cookie_')], [])

            bili_store.store()
            with open(cookie_path, 'r', encoding='utf-8') as f:
                saved = json.load(f)
            self.assertEqual(saved['SESSDATA'], 'new_sess')
            self.assertEqual(saved['access_token'], 'new_acc')
            self.assertEqual(saved['refresh_token'], 'new_ref')
            self.assertEqual([n for n in os.listdir(tmpdir) if n.startswith('.cookie_')], [])

    # 14. P1-7: 真实 spawn 子进程发送 128KB 异常信息在 5 秒内结束并上抛 RuntimeError，不发生Pipe死锁
    def test_biliuprs_spawn_large_error_no_pipe_deadlock(self):
        # 14a. stream_gears_upload 捕获 128KB 异常并发送有界消息，finally 中关闭 ex_conn
        parent_conn, child_conn = mp.Pipe()
        worker_errors = []

        def _run_stream_gears_upload():
            try:
                biliuprs_mod.stream_gears_upload(child_conn, 'AUTO')
            except BaseException as exc:
                worker_errors.append(exc)

        try:
            with patch.object(
                biliuprs_mod.stream_gears,
                'upload',
                side_effect=RuntimeError('X' * (128 * 1024)),
            ):
                t = threading.Thread(target=_run_stream_gears_upload, daemon=True)
                t.start()
                received = parent_conn.recv()
                with self.assertRaises(EOFError):
                    parent_conn.recv()
                t.join(timeout=5)
                self.assertFalse(t.is_alive())
            self.assertEqual(worker_errors, [])
            self.assertEqual(received, 'X' * 8192)
            self.assertLessEqual(len(received), 8192)
        finally:
            try:
                parent_conn.close()
            except Exception:
                pass
            try:
                child_conn.close()
            except Exception:
                pass

        # 14b. 真实 spawn 子进程抛 128KB 字符串 RuntimeError，5 秒内结束并上抛 RuntimeError 无死锁
        fake_files = [UploadBase.FileInfo(video='test_spawn.mp4', danmaku=None)]
        up_rs = biliuprs_mod.BiliWeb(
            'test_streamer',
            {'format_title': 'test_title', 'url': 'http://example.com'},
            user_cookie='fake_cookie.json',
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            helper_path = os.path.join(tmpdir, '_spawn_pipe_helper.py')
            with open(helper_path, 'w', encoding='utf-8') as f:
                f.write(
                    "def run_large_error(ex_conn, lines, *args, **kwargs):\n"
                    "    try:\n"
                    "        raise RuntimeError('E' * (128 * 1024))\n"
                    "    except Exception as e:\n"
                    "        ex_conn.send(str(e))\n"
                    "    finally:\n"
                    "        ex_conn.close()\n"
                )
            sys.path.insert(0, tmpdir)
            try:
                helper_mod = importlib.import_module('_spawn_pipe_helper')
                with patch.object(biliuprs_mod, 'stream_gears_upload', helper_mod.run_large_error):
                    t0 = time.monotonic()
                    with self.assertRaises(RuntimeError) as ctx_err:
                        up_rs.upload(fake_files)
                    elapsed = time.monotonic() - t0
                self.assertLess(elapsed, 5.0)
                self.assertEqual(len(str(ctx_err.exception)), 128 * 1024)
            finally:
                if tmpdir in sys.path:
                    sys.path.remove(tmpdir)
                sys.modules.pop('_spawn_pipe_helper', None)

    # 15. P1-1a: process 在 downloaded_processor 完成及 UPLOAD 派发前保持 url_status == 1，阻止 singleton_check 抢跑；保留 DOWNLOADED 兼容注册
    def test_p1_1_process_holds_url_status_through_downloaded_processor_and_upload_dispatch(self):
        streamer = 'p11_proc_streamer'
        url = 'http://p11-proc.test/live'
        config['streamers'] = {
            streamer: {
                'url': url,
                'downloaded_processor': [{'run': 'echo post'}],
            }
        }
        pi = MagicMock()
        pi.url_status = {url: 0}
        pi.inverted_index = {url: streamer}
        context['PluginInfo'] = pi
        context['url_upload_count'][url] = 0

        class DummyPlatform:
            def __init__(self, n, u):
                pass

            async def acheck_stream(self, is_check=False):
                return True

            def should_record(self):
                return True

        timeline = []
        singleton_events = []

        def fake_download(n, u, cfg):
            timeline.append(('download', pi.url_status[url]))
            return {'name': n, 'url': u, 'title': 't', 'disk_soft_triggered': True}

        def fake_processor(procs, payload):
            timeline.append(('downloaded_processor', pi.url_status[url]))
            asyncio.run(app_mod.singleton_check(DummyPlatform, streamer, url))

        def capture_send_event(ev):
            if ev.type_ == UPLOAD and 'title' not in ev.args[0]:
                singleton_events.append(ev)
                return
            timeline.append(('send_upload', pi.url_status[url], ev.type_, ev.args[0].get('disk_soft_triggered')))

        with patch('biliup.handler.biliup_download', side_effect=fake_download), \
                patch('biliup.handler.UploadBase.file_list', return_value=[UploadBase.FileInfo('seg.flv', None)]), \
                patch('biliup.handler.processor', side_effect=fake_processor), \
                patch('biliup.handler.check_timerange', return_value=True), \
                patch.object(handler_mod.event_manager, 'send_event', side_effect=capture_send_event), \
                patch.object(app_mod.event_manager, 'send_event', side_effect=capture_send_event):
            process(Event(DOWNLOAD, (streamer, url)))

        self.assertEqual(singleton_events, [])
        self.assertEqual(
            timeline,
            [
                ('download', 1),
                ('downloaded_processor', 1),
                ('send_upload', 1, UPLOAD, True),
            ],
        )
        self.assertEqual(pi.url_status[url], 0)
        self.assertIn(url, handler_mod._pending_soft_upload_urls)

        # 外部直接发送 Event(DOWNLOADED, ...) 仍由 processed 兼容执行 downloaded_processor 并派发 UPLOAD
        clear_pending_soft_upload_urls()
        processed_events = []
        with patch('biliup.handler.UploadBase.file_list', return_value=[UploadBase.FileInfo('seg.flv', None)]), \
                patch('biliup.handler.processor') as mock_proc2, \
                patch.object(handler_mod.event_manager, 'send_event', side_effect=lambda ev: processed_events.append(ev)):
            processed(Event(DOWNLOADED, ({'name': streamer, 'url': url, 'title': 't', 'disk_soft_triggered': True},)))

        mock_proc2.assert_called_once()
        self.assertEqual(len(processed_events), 1)
        self.assertEqual(processed_events[0].type_, UPLOAD)
        self.assertIn(url, handler_mod._pending_soft_upload_urls)

    # 16. P1-1b: 软水位事件唤醒处于 delay 等待中的同 url 上传并穿透 url_status == 1 立即上传
    def test_p1_1_soft_upload_wakes_delay_waiter_and_bypasses_url_status_1(self):
        orig_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            try:
                video_name = 'soft_wake.mp4'
                with open(video_name, 'wb') as f:
                    f.write(b'video')
                mock_file = UploadBase.FileInfo(video=video_name, danmaku=None)

                name = 'wake_streamer'
                url = 'http://p11-wake.test/live'
                config['delay'] = 30
                config['max_upload_limit'] = 5
                config['streamers'] = {name: {'url': url}}
                context['url_upload_count'][url] = 0
                context['file_upload_count'] = {video_name: 0}
                context['PluginInfo'] = MagicMock()
                context['PluginInfo'].url_status = {url: 0}

                errors = []
                with patch('biliup.handler.SessionLocal', side_effect=RuntimeError('skip db')), \
                        patch('biliup.handler.UploadBase.file_list', return_value=[mock_file]), \
                        patch('biliup.handler.UploadBase.get_max_upload_count', return_value=0), \
                        patch('biliup.handler.upload', return_value=[mock_file]) as mock_upload, \
                        patch('biliup.handler.uploaded') as mock_uploaded:

                    def run_bare():
                        try:
                            process_upload(Event(UPLOAD, ({'name': name, 'url': url, 'title': 't'},)))
                        except Exception as exc:
                            errors.append(exc)

                    def run_soft():
                        try:
                            process_upload(Event(UPLOAD, ({'name': name, 'url': url, 'title': 't', 'disk_soft_triggered': True},)))
                        except Exception as exc:
                            errors.append(exc)

                    t1 = threading.Thread(target=run_bare)
                    t1.start()

                    deadline = time.monotonic() + 2.0
                    while time.monotonic() < deadline:
                        with handler_mod._soft_upload_lock:
                            waiting = url in handler_mod._upload_delay_events
                        if waiting:
                            break
                        time.sleep(0.01)
                    self.assertTrue(waiting)

                    # 模拟延迟期间 url_status 被置为 1
                    context['PluginInfo'].url_status[url] = 1
                    t0 = time.monotonic()
                    t2 = threading.Thread(target=run_soft)
                    t2.start()

                    t2.join(timeout=3.0)
                    t1.join(timeout=3.0)
                    elapsed = time.monotonic() - t0

                self.assertEqual(errors, [])
                self.assertFalse(t1.is_alive())
                self.assertFalse(t2.is_alive())
                self.assertLess(elapsed, 3.0)
                mock_upload.assert_called_once()
                mock_uploaded.assert_called_once_with(name, None, [mock_file])
                self.assertEqual(context['url_upload_count'][url], 0)
                self.assertNotIn(url, handler_mod._pending_soft_upload_urls)
                self.assertNotIn(url, handler_mod._upload_delay_events)
            finally:
                os.chdir(orig_cwd)

    # 17. P1-1b: 裸 UPLOAD 进入 process_upload 时若已有待处理软水位标记，直接穿透 delay 与 url_status == 1
    def test_p1_1_bare_upload_consumes_pending_soft_flag_and_skips_delay(self):
        name = 'pending_soft_streamer'
        url = 'http://p11-pending.test/live'
        mock_file = UploadBase.FileInfo(video='pending_soft.mp4', danmaku=None)
        config['delay'] = 30
        config['max_upload_limit'] = 5
        config['streamers'] = {name: {'url': url}}
        context['url_upload_count'][url] = 0
        context['PluginInfo'] = MagicMock()
        context['PluginInfo'].url_status = {url: 1}

        handler_mod._mark_pending_soft_upload(url)
        with patch('biliup.handler.SessionLocal', side_effect=RuntimeError('skip db')), \
                patch('biliup.handler.UploadBase.file_list', return_value=[mock_file]), \
                patch('biliup.handler.UploadBase.get_max_upload_count', return_value=0), \
                patch('biliup.handler._wait_upload_delay') as mock_wait_delay, \
                patch('biliup.handler.upload', return_value=[mock_file]) as mock_upload, \
                patch('biliup.handler.uploaded') as mock_uploaded:
            process_upload(Event(UPLOAD, ({'name': name, 'url': url, 'title': 't'},)))

        mock_wait_delay.assert_not_called()
        mock_upload.assert_called_once()
        mock_uploaded.assert_called_once_with(name, None, [mock_file])
        self.assertNotIn(url, handler_mod._pending_soft_upload_urls)

    # 18. P1-6a: get_max_upload_count 仅调用一次 os.listdir('.')，遇已消失文件安全跳过不抛 KeyError
    def test_p1_6_get_max_upload_count_single_listdir_and_vanished_file_safe(self):
        orig_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            try:
                with open('present.mp4', 'wb') as f:
                    f.write(b'ok')
                context['file_upload_count'] = {
                    'present.mp4': 2,
                    'vanished.mp4': 1,
                    'stale.mp4': 4,
                }
                real_listdir = os.listdir
                with patch('biliup.engine.upload.os.listdir', wraps=real_listdir) as spy_listdir:
                    max_cnt = UploadBase.get_max_upload_count([
                        UploadBase.FileInfo(video='vanished.mp4', danmaku=None),
                        UploadBase.FileInfo(video='present.mp4', danmaku=None),
                    ])
                    self.assertEqual(spy_listdir.call_count, 1)
                    self.assertEqual(max_cnt, 2)
                    self.assertEqual(context['file_upload_count'], {'present.mp4': 3})

                    max_cnt_all_vanished = UploadBase.get_max_upload_count([
                        UploadBase.FileInfo(video='vanished.mp4', danmaku=None),
                    ])
                    self.assertEqual(spy_listdir.call_count, 2)
                    self.assertEqual(max_cnt_all_vanished, 0)
                    self.assertEqual(context['file_upload_count'], {'present.mp4': 3})
            finally:
                os.chdir(orig_cwd)

    # 19. P1-6b: process_upload 在 delay 结束后剔除已消失文件；全消失时提前返回
    def test_p1_6_process_upload_refilters_vanished_files_after_delay(self):
        orig_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            try:
                with open('vanish.mp4', 'wb') as f:
                    f.write(b'v1')
                with open('keep.mp4', 'wb') as f:
                    f.write(b'v2')

                name = 'refilter_streamer'
                url = 'http://p16-refilter.test/live'
                config['delay'] = 10
                config['max_upload_limit'] = 5
                config['streamers'] = {name: {'url': url}}
                context['url_upload_count'][url] = 0
                context['file_upload_count'] = {'vanish.mp4': 0, 'keep.mp4': 0}
                context['PluginInfo'] = MagicMock()
                context['PluginInfo'].url_status = {url: 0}

                file_vanish = UploadBase.FileInfo(video='vanish.mp4', danmaku=None)
                file_keep = UploadBase.FileInfo(video='keep.mp4', danmaku=None)

                def _remove_vanish(_url, _delay):
                    os.remove('vanish.mp4')
                    return False

                with patch('biliup.handler.SessionLocal', side_effect=RuntimeError('skip db')), \
                        patch('biliup.handler.UploadBase.file_list', return_value=[file_vanish, file_keep]), \
                        patch('biliup.handler._wait_upload_delay', side_effect=_remove_vanish), \
                        patch('biliup.handler.upload', return_value=[file_keep]) as mock_upload, \
                        patch('biliup.handler.uploaded') as mock_uploaded:
                    process_upload(Event(UPLOAD, ({'name': name, 'url': url, 'title': 't'},)))

                mock_upload.assert_called_once()
                self.assertEqual(mock_upload.call_args.kwargs.get('target_files'), ['keep.mp4'])
                mock_uploaded.assert_called_once_with(name, None, [file_keep])
                self.assertEqual(context['file_upload_count'], {'keep.mp4': 1})

                # 若 delay 结束后全部待传文件都已消失，应直接跳过后续上传
                os.remove('keep.mp4')
                with open('vanish_all.mp4', 'wb') as f:
                    f.write(b'v3')
                file_all = UploadBase.FileInfo(video='vanish_all.mp4', danmaku=None)

                def _remove_all(_url, _delay):
                    os.remove('vanish_all.mp4')
                    return False

                with patch('biliup.handler.SessionLocal', side_effect=RuntimeError('skip db')), \
                        patch('biliup.handler.UploadBase.file_list', return_value=[file_all]), \
                        patch('biliup.handler._wait_upload_delay', side_effect=_remove_all), \
                        patch('biliup.handler.UploadBase.get_max_upload_count') as mock_get_count, \
                        patch('biliup.handler.upload') as mock_upload2:
                    process_upload(Event(UPLOAD, ({'name': name, 'url': url, 'title': 't'},)))

                mock_get_count.assert_not_called()
                mock_upload2.assert_not_called()
            finally:
                os.chdir(orig_cwd)

    # 20. P1-6c: 部分认领时回退未被认领文件的计数，且异常/熔断仅作用于实际认领文件
    def test_p1_6_partial_claim_rolls_back_unclaimed_counts_and_scopes_circuit_break(self):
        orig_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            try:
                with open('file_a.mp4', 'wb') as f:
                    f.write(b'a')
                with open('file_b.mp4', 'wb') as f:
                    f.write(b'b')

                file_a = UploadBase.FileInfo(video='file_a.mp4', danmaku=None)
                file_b = UploadBase.FileInfo(video='file_b.mp4', danmaku=None)
                name = 'partial_streamer'
                url = 'http://p16-partial.test/live'

                behavior = {'mode': 'ok', 'claimed': []}

                class FakePartialUploader(UploadBase):
                    def upload(self, file_list):
                        behavior['claimed'].append(list(file_list))
                        if behavior['mode'] == 'ok':
                            return list(file_list)
                        if behavior['mode'] == 'unrecoverable':
                            raise RuntimeError('code: -101, message: 账号未登录')
                        raise TimeoutError('transient timeout')

                config['max_upload_limit'] = 3
                config['delay'] = 0
                config['streamers'] = {
                    name: {'url': url, 'uploader': 'fake_partial', 'title': 't'}
                }
                context['PluginInfo'] = MagicMock()
                context['PluginInfo'].url_status = {url: 2}

                with patch.dict(Plugin.upload_plugins, {'fake_partial': FakePartialUploader}), \
                        patch('biliup.handler.SessionLocal', side_effect=RuntimeError('skip db')), \
                        patch('biliup.engine.upload.UploadBase.file_list', return_value=[file_a, file_b]):

                    # A. 部分认领成功：file_b 被手动上传占用未认领，其计数回退为 0
                    context['upload_filename'] = ['file_b']
                    context['url_upload_count'][url] = 0
                    context['file_upload_count'] = {'file_a.mp4': 0, 'file_b.mp4': 0}
                    with patch('biliup.handler.uploaded') as mock_uploaded:
                        process_upload(Event(UPLOAD, ({'name': name, 'url': url, 'title': 't'},)))
                    self.assertEqual(behavior['claimed'][-1], [file_a])
                    mock_uploaded.assert_called_once_with(name, None, [file_a])
                    self.assertEqual(context['file_upload_count']['file_a.mp4'], 1)
                    self.assertEqual(context['file_upload_count']['file_b.mp4'], 0)

                    # B. 部分认领遇 -101 不可自愈错误：仅熔断 file_a，file_b 回退且不被标记耗尽
                    clear_notified_exhausted_files()
                    behavior['mode'] = 'unrecoverable'
                    context['upload_filename'] = ['file_b']
                    context['url_upload_count'][url] = 0
                    context['file_upload_count'] = {'file_a.mp4': 0, 'file_b.mp4': 0}
                    with patch('biliup.handler.send_notification') as mock_notify:
                        process_upload(Event(UPLOAD, ({'name': name, 'url': url, 'title': 't'},)))
                    self.assertEqual(context['file_upload_count']['file_a.mp4'], 3)
                    self.assertEqual(context['file_upload_count']['file_b.mp4'], 0)
                    mock_notify.assert_called_once()
                    self.assertEqual(mock_notify.call_args.kwargs['error_type'], 'unrecoverable_upload_error')
                    self.assertEqual(handler_mod._notified_exhausted_files.get(name), {'file_a.mp4'})

                    # C. 部分认领遇瞬态失败：未认领文件 file_b 原本计数 2，回退后保持 2；file_a 仅为 1 不误报上限
                    clear_notified_exhausted_files()
                    behavior['mode'] = 'transient'
                    context['upload_filename'] = ['file_b']
                    context['url_upload_count'][url] = 0
                    context['file_upload_count'] = {'file_a.mp4': 0, 'file_b.mp4': 2}
                    with patch('biliup.handler.send_notification') as mock_notify2:
                        process_upload(Event(UPLOAD, ({'name': name, 'url': url, 'title': 't'},)))
                    self.assertEqual(context['file_upload_count']['file_a.mp4'], 1)
                    self.assertEqual(context['file_upload_count']['file_b.mp4'], 2)
                    mock_notify2.assert_not_called()
            finally:
                os.chdir(orig_cwd)

    # 21. P1: 空列表扫描期间并发到达软水位标记时，不吞事件，重扫并跳过 delay 完成上传
    def test_empty_scan_rescans_when_pending_soft_upload_arrives(self):
        name = 'empty_race_streamer'
        url = 'http://p1-empty-soft-race.test/live'
        new_seg = UploadBase.FileInfo(video='new_seg.flv', danmaku=None)
        config['delay'] = 30
        config['max_upload_limit'] = 5
        config['streamers'] = {name: {'url': url}}
        context['url_upload_count'][url] = 0
        context['PluginInfo'] = MagicMock()
        context['PluginInfo'].url_status = {url: 1}

        scan_calls = {'count': 0}

        def fake_file_list(target_name):
            scan_calls['count'] += 1
            if scan_calls['count'] == 1:
                handler_mod._mark_pending_soft_upload(url)
                return []
            return [new_seg]

        with patch('biliup.handler.SessionLocal', side_effect=RuntimeError('skip db')), \
                patch('biliup.handler.UploadBase.file_list', side_effect=fake_file_list) as mock_file_list, \
                patch('biliup.handler.UploadBase.get_max_upload_count', return_value=0), \
                patch('biliup.handler._wait_upload_delay') as mock_wait_delay, \
                patch('biliup.handler.upload', return_value=[new_seg]) as mock_upload, \
                patch('biliup.handler.uploaded') as mock_uploaded:
            process_upload(Event(UPLOAD, ({'name': name, 'url': url, 'title': 't'},)))

        self.assertEqual(mock_file_list.call_count, 2)
        mock_wait_delay.assert_not_called()
        mock_upload.assert_called_once()
        self.assertEqual(mock_upload.call_args.kwargs['target_files'], ['new_seg.flv'])
        mock_uploaded.assert_called_once_with(name, None, [new_seg])
        self.assertEqual(context['url_upload_count'][url], 0)
        self.assertNotIn(url, handler_mod._pending_soft_upload_urls)

    # 22. P1(Sourcery #5): 已是 soft 事件的空扫描期间若再次到达软水位标记，同样不应吞掉而应重扫
    def test_soft_empty_scan_rescans_when_concurrent_soft_arrives(self):
        name = 'soft_empty_race_streamer'
        url = 'http://p1-soft-empty-race.test/live'
        new_seg = UploadBase.FileInfo(video='soft_new_seg.flv', danmaku=None)
        config['delay'] = 30
        config['max_upload_limit'] = 5
        config['streamers'] = {name: {'url': url}}
        context['url_upload_count'][url] = 0
        context['PluginInfo'] = MagicMock()
        context['PluginInfo'].url_status = {url: 1}

        scan_calls = {'count': 0}

        def fake_file_list(target_name):
            scan_calls['count'] += 1
            if scan_calls['count'] == 1:
                handler_mod._mark_pending_soft_upload(url)
                return []
            return [new_seg]

        with patch('biliup.handler.SessionLocal', side_effect=RuntimeError('skip db')), \
                patch('biliup.handler.UploadBase.file_list', side_effect=fake_file_list) as mock_file_list, \
                patch('biliup.handler.UploadBase.get_max_upload_count', return_value=0), \
                patch('biliup.handler._wait_upload_delay') as mock_wait_delay, \
                patch('biliup.handler.upload', return_value=[new_seg]) as mock_upload, \
                patch('biliup.handler.uploaded') as mock_uploaded:
            process_upload(Event(UPLOAD, ({
                'name': name,
                'url': url,
                'title': 't',
                'disk_soft_triggered': True,
            },)))

        self.assertEqual(mock_file_list.call_count, 2)
        mock_wait_delay.assert_not_called()
        mock_upload.assert_called_once()
        self.assertEqual(mock_upload.call_args.kwargs['target_files'], ['soft_new_seg.flv'])
        mock_uploaded.assert_called_once_with(name, None, [new_seg])
        self.assertEqual(context['url_upload_count'][url], 0)
        self.assertNotIn(url, handler_mod._pending_soft_upload_urls)

    # 23. P1(Sourcery #5): 活跃上传执行期间若软水位事件抵达，结束释放计数后必须补发一次 soft UPLOAD
    def test_active_upload_dispatches_soft_followup_on_finish(self):
        name = 'active_soft_followup_streamer'
        url = 'http://p1-active-soft-followup.test/live'
        seg1 = UploadBase.FileInfo(video='seg1.flv', danmaku=None)
        config['delay'] = 0
        config['max_upload_limit'] = 5
        config['streamers'] = {name: {'url': url}}
        context['url_upload_count'][url] = 0
        context['PluginInfo'] = MagicMock()
        context['PluginInfo'].url_status = {url: 1}

        def fake_upload(stream_info, target_files=None):
            # 模拟正在上传 seg1 时，分片 2 触发了新的软水位 UPLOAD 事件
            process_upload(Event(UPLOAD, ({
                'name': name,
                'url': url,
                'title': 't',
                'disk_soft_triggered': True,
            },)))
            self.assertIn(url, handler_mod._pending_soft_upload_urls)
            return [seg1]

        with patch('biliup.handler.SessionLocal', side_effect=RuntimeError('skip db')), \
                patch('biliup.handler.UploadBase.file_list', return_value=[seg1]), \
                patch('biliup.handler.UploadBase.get_max_upload_count', return_value=0), \
                patch('biliup.handler.upload', side_effect=fake_upload), \
                patch('biliup.handler.uploaded'), \
                patch.object(handler_mod.event_manager, 'send_event') as mock_send_event:
            process_upload(Event(UPLOAD, ({'name': name, 'url': url, 'title': 't'},)))

        self.assertEqual(context['url_upload_count'][url], 0)
        self.assertNotIn(url, handler_mod._pending_soft_upload_urls)
        mock_send_event.assert_called_once_with(Event(UPLOAD, ({
            'name': name,
            'url': url,
            'disk_soft_triggered': True,
        },)))


if __name__ == '__main__':
    unittest.main()
