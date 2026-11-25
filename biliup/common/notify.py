import logging
import re
import smtplib
import ssl
import threading
import time
from email.header import Header
from email.mime.text import MIMEText
from email.utils import formataddr
from typing import Callable, Optional

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


def _send_email_sync(
    title: str,
    content: str,
    streamer_name: str,
    error_type: str,
    cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS,
    on_failure: Optional[Callable[[], None]] = None,
) -> bool:
    if not is_notify_enabled():
        return False

    raw_host = config.get('notify_smtp_host')
    host = '' if raw_host is None else str(raw_host).strip()
    port = config.get('notify_smtp_port', 465)
    raw_user = config.get('notify_smtp_user')
    user = '' if raw_user is None else str(raw_user).strip()
    raw_password = config.get('notify_smtp_pass')
    password = '' if raw_password is None else str(raw_password)
    raw_to_addr = config.get('notify_email_to')
    to_addr = '' if raw_to_addr is None else str(raw_to_addr).strip()
    recipients = [addr.strip() for addr in re.split(r'[,;]', to_addr) if addr.strip()]

    if not (host and user and password and recipients):
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
        to_header = ', '.join(recipients)
        msg = MIMEText(content, 'plain', 'utf-8')
        msg['From'] = formataddr(('biliup', user))
        msg['To'] = to_header
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
        refused = server.sendmail(user, recipients, msg.as_string())
        if refused:
            logger.warning(f"邮件告警部分收件人被拒收: {refused}")
        logger.info(f"邮件告警发送成功: [{title}] -> {to_header}")
        return True
    except Exception as e:
        logger.error(f"邮件告警发送异常: {e}")
        # 发送失败时重置冷却，以便后续重试
        with _cooldown_lock:
            _cooldown_map.pop(key, None)
        if on_failure is not None:
            try:
                on_failure()
            except Exception:
                logger.exception("邮件告警失败回调执行异常")
        return False
    finally:
        if server:
            try:
                server.quit()
            except Exception:
                pass


def send_notification(
    title: str,
    content: str,
    streamer_name: str = "",
    error_type: str = "",
    sync: bool = False,
    cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS,
    on_failure: Optional[Callable[[], None]] = None,
):
    """
    发送邮件告警通知
    :param title: 邮件标题
    :param content: 邮件正文
    :param streamer_name: 主播标识（用于冷却计算）
    :param error_type: 错误类型（用于冷却计算）
    :param sync: 是否同步发送（默认 False 异步，测试可设为 True）
    :param cooldown_seconds: 冷却秒数，默认 1800 秒（30分钟）
    :param on_failure: SMTP 发送失败（进入异常分支）时的可选回调
    """
    if not is_notify_enabled():
        return

    if sync:
        return _send_email_sync(
            title,
            content,
            streamer_name,
            error_type,
            cooldown_seconds,
            on_failure=on_failure,
        )
    else:
        thread = threading.Thread(
            target=_send_email_sync,
            args=(title, content, streamer_name, error_type, cooldown_seconds),
            kwargs={'on_failure': on_failure},
            daemon=True,
            name=f"NotifyThread-{streamer_name}-{error_type}"
        )
        thread.start()
