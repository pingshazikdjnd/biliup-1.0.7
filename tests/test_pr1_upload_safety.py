import copy
import os
import shutil
import smtplib
import ssl
import time
import unittest
from email import message_from_string
from unittest.mock import MagicMock, patch, call
from collections import namedtuple

import tests  # noqa: F401
from biliup.config import config
from biliup.app import context
from biliup.common.notify import (
    send_notification,
    clear_cooldown,
    _send_email_sync,
    is_notify_enabled,
)
from biliup.engine.download import DownloadBase
from biliup.engine.upload import UploadBase
from biliup.engine.event import Event
from biliup.handler import is_unrecoverable_error, process_upload, UPLOAD
import biliup.uploader as uploader


class TestPR1UploadSafety(unittest.TestCase):
    def setUp(self):
        clear_cooldown()
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

        # 瞬态错误（应返回 False）
        self.assertFalse(is_unrecoverable_error(ConnectionResetError("Connection reset by peer")))
        self.assertFalse(is_unrecoverable_error(TimeoutError("Read timed out")))
        self.assertFalse(is_unrecoverable_error("502 Bad Gateway"))
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
            downloader.start()
        self.assertEqual(above_runs, 3)

        # 测试 8.3: 软水位低于阈值时 start 循环提前 break
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

        # 仅运行了一次就主动 break，未进入死循环
        self.assertEqual(run_count, 1)
        self.assertEqual(stream_info['name'], "test_streamer")

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


if __name__ == '__main__':
    unittest.main()
