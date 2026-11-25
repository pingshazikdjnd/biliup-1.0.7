#!/usr/bin/python3
# coding:utf8
import argparse
import asyncio
import logging.config
import platform
import shutil

import biliup.common.reload
from biliup.config import config
from biliup import __version__, LOG_CONF, IS_FROZEN
from biliup.common.Daemon import Daemon
from biliup.common.reload import AutoReload
from biliup.common.log import DebugLevelFilter


def _strip_none_recursive(obj):
    if isinstance(obj, dict):
        return {k: _strip_none_recursive(v) for k, v in obj.items() if v is not None}
    return obj


def _deep_merge_dict(base: dict, override: dict) -> dict:
    """递归合并嵌套字典并跳过 None；若处理器/工厂的 class 或 () 类型发生变更则整体替换避免残留旧类型参数。"""
    if not isinstance(override, dict):
        return base
    for key, value in override.items():
        if value is None:
            continue
        existing = base.get(key)
        if isinstance(value, dict) and isinstance(existing, dict):
            class_changed = (
                'class' in value
                and value['class'] is not None
                and value['class'] != existing.get('class')
            )
            factory_changed = (
                '()' in value
                and value['()'] is not None
                and value['()'] != existing.get('()')
            )
            if class_changed or factory_changed:
                base[key] = _strip_none_recursive(value)
            else:
                _deep_merge_dict(existing, value)
        elif isinstance(value, dict):
            base[key] = _strip_none_recursive(value)
        else:
            base[key] = value
    return base


def arg_parser():
    daemon = Daemon('watch_process.pid', lambda: main(args))
    parser = argparse.ArgumentParser(description='Stream download and upload, not only for bilibili.')
    parser.add_argument('--version', action='version', version=f"v{__version__}")
    parser.add_argument('-H', help='web api host [default: 0.0.0.0]', dest='host')
    if IS_FROZEN:
        parser.add_argument(
            '-P',
            help='web api port (REQUIRED)',
            dest='port',
            required=True,
            type=int
        )
    else:
        parser.add_argument('-P', help='web api port [default: 19159]', default=19159, dest='port')
    parser.add_argument('--no-http', action='store_true', help='disable web api')
    parser.add_argument('--static-dir', help='web static files directory for custom ui')
    parser.add_argument('--password', help='web ui password ,default username is biliup', dest='password')
    parser.add_argument('-v', '--verbose', action="store_const", const=logging.DEBUG, help="Increase output verbosity")
    parser.add_argument('--config', type=argparse.FileType(mode='rb'),
                        help='Location of the configuration file (default "./config.yaml")')
    parser.add_argument('--no-access-log', action='store_true', help='disable web access log')
    subparsers = parser.add_subparsers(help='Windows does not support this sub-command.')
    # create the parser for the "start" command
    parser_start = subparsers.add_parser('start', help='Run as a daemon process.')
    parser_start.set_defaults(func=daemon.start)
    parser_stop = subparsers.add_parser('stop', help='Stop daemon according to "watch_process.pid".')
    parser_stop.set_defaults(func=daemon.stop)
    parser_restart = subparsers.add_parser('restart')
    parser_restart.set_defaults(func=daemon.restart)
    parser.set_defaults(func=lambda: asyncio.run(main(args)))
    args = parser.parse_args()
    biliup.common.reload.program_args = args.__dict__

    is_stop = args.func == daemon.stop

    if not is_stop:
        from biliup.database.db import SessionLocal, init
        with SessionLocal() as db:
            from_config = False
            try:
                config.load(args.config)
                from_config = True
            except FileNotFoundError:
                print(f'新版本不依赖配置文件，请访问 WebUI 修改配置')
            if init(args.no_http, from_config):
                if from_config:
                    config.save_to_db(db)
            config.load_from_db(db)
        # db.remove()
        _deep_merge_dict(LOG_CONF, config.get('LOGGING') or {})
        if args.verbose:
            LOG_CONF['loggers']['biliup']['level'] = args.verbose
            LOG_CONF['root']['level'] = args.verbose
        logging.config.dictConfig(LOG_CONF)
        logging.getLogger('httpx').addFilter(DebugLevelFilter())
        # logging.getLogger('hpack').setLevel(logging.CRITICAL)
        # logging.getLogger('httpx').setLevel(logging.CRITICAL)
    if platform.system() == 'Windows':
        if is_stop:
            return
        return asyncio.run(main(args))
    args.func()


async def main(args):
    from biliup.app import event_manager
    from biliup.engine.download import recover_stale_mesio_tmp

    # 启动时删除临时文件夹，并恢复异常退出残留的 mesio 分段录像
    shutil.rmtree('./cache/temp', ignore_errors=True)
    try:
        recover_stale_mesio_tmp()
    except Exception:
        logging.getLogger('biliup').warning('恢复残留 mesio 临时目录失败', exc_info=True)

    event_manager.start()

    interval = config.get('check_sourcecode', 15)

    if not args.no_http:
        import biliup.web
        runner = await biliup.web.service(args)
        detector = AutoReload(event_manager, runner.cleanup, interval=interval)
        biliup.common.reload.global_reloader = detector
        await detector.astart()
    else:
        import biliup.common.reload
        detector = AutoReload(event_manager, interval=interval)
        biliup.common.reload.global_reloader = detector
        await asyncio.gather(detector.astart())


class GracefulExit(SystemExit):
    code = 1


if __name__ == '__main__':
    arg_parser()
