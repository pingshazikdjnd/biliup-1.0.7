import logging
import smtplib
import ssl
import threading
import time
from email.header import Header
from email.mime.text import MIMEText
from email.utils import formataddr
from typing import Optional

from biliup.config import config

logger = logging.getLogger('biliup')

_cooldown_lock = threading.Lock()
_cooldown_map = {}
DEFAULT_COOLDOWN_SECONDS = 1800  # 30 分钟防轰炸冷却


def clear_cooldown():
    """清理冷却状态（主要供测试使用）"""
    with _cooldown_lock:
        _cooldown_map.clear()


def is_notify_enabled() -> bool:
    val = config.get('notify_email_enabled', False)
    if isinstance(val, str):
        return val.lower() in ('true', '1', 'yes', 'on')
    return bool(val)


def _send_email_sync(title: str, content: str, streamer_name: str, error_type: str, cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS) -> bool:
    if not is_notify_enabled():
        return False

    host = config.get('notify_smtp_host')
    port = config.get('notify_smtp_port', 465)
    user = config.get('notify_smtp_user')
    password = config.get('notify_smtp_pass')
    to_addr = config.get('notify_email_to')

    if not (host and user and password and to_addr):
        logger.warning("邮件告警已开启，但 SMTP 配置信息不完整，跳过发送")
        return False

    try:
        port = int(port)
    except (ValueError, TypeError):
        port = 465

    key = (streamer_name or "global", error_type or "general")
    now = time.time()
    with _cooldown_lock:
        last_time = _cooldown_map.get(key, 0)
        if now - last_time < cooldown_seconds:
            logger.info(f"邮件告警 [{key[0]} - {key[1]}] 处于冷却期中，跳过发送 (剩余 {int(cooldown_seconds - (now - last_time))} 秒)")
            return False
        _cooldown_map[key] = now

    server: Optional[smtplib.SMTP] = None
    try:
        msg = MIMEText(content, 'plain', 'utf-8')
        msg['From'] = formataddr(('biliup', user))
        msg['To'] = to_addr
        msg['Subject'] = Header(title, 'utf-8')

        ssl_context = ssl.create_default_context()
        if port == 465:
            server = smtplib.SMTP_SSL(host, port, timeout=15, context=ssl_context)
        else:
            server = smtplib.SMTP(host, port, timeout=15)
            server.ehlo()
            server.starttls(context=ssl_context)
            server.ehlo()

        server.login(user, password)
        server.sendmail(user, [to_addr], msg.as_string())
        logger.info(f"邮件告警发送成功: [{title}] -> {to_addr}")
        return True
    except Exception as e:
        logger.error(f"邮件告警发送异常: {e}")
        # 发送失败时重置冷却，以便后续重试
        with _cooldown_lock:
            _cooldown_map.pop(key, None)
        return False
    finally:
        if server:
            try:
                server.quit()
            except Exception:
                pass


def send_notification(title: str, content: str, streamer_name: str = "", error_type: str = "", sync: bool = False, cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS):
    """
    发送邮件告警通知
    :param title: 邮件标题
    :param content: 邮件正文
    :param streamer_name: 主播标识（用于冷却计算）
    :param error_type: 错误类型（用于冷却计算）
    :param sync: 是否同步发送（默认 False 异步，测试可设为 True）
    :param cooldown_seconds: 冷却秒数，默认 1800 秒（30分钟）
    """
    if not is_notify_enabled():
        return

    if sync:
        return _send_email_sync(title, content, streamer_name, error_type, cooldown_seconds)
    else:
        thread = threading.Thread(
            target=_send_email_sync,
            args=(title, content, streamer_name, error_type, cooldown_seconds),
            daemon=True,
            name=f"NotifyThread-{streamer_name}-{error_type}"
        )
        thread.start()
