import json
import pathlib
import shutil
import os
from collections import UserDict
from sqlalchemy import select


try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


class Config(UserDict):
    def load_cookies(self, file='cookies.json'):
        if not os.path.exists(file):
            raise FileNotFoundError(file)

        if self.data.get('user') is None:
            self.data['user'] = {'cookies': {}}
        if self.data["user"].get("cookies") is None:
            self.data['user']['cookies'] = {}
        with open(file, encoding='utf-8') as stream:
            s = json.load(stream)
            for i in s["cookie_info"]["cookies"]:
                name = i["name"]
                self.data["user"]["cookies"][name] = i["value"]
            self.data["user"]["access_token"] = s["token_info"]["access_token"]

    def load_from_db(self, db):
        from biliup.database.models import Configuration, LiveStreamers
        new_data = dict(self.data)
        context = {
            'file_upload_count': self.data.get('file_upload_count', {}),
            'url_upload_count': self.data.get('url_upload_count', {}),
            'upload_filename': self.data.get('upload_filename', []),
            'PluginInfo': self.data.get('PluginInfo'),
            'sync_downloader_map': self.data.get('sync_downloader_map', {}),
        }

        for con in db.execute(select(Configuration.value).where(Configuration.key == 'config')):
            loaded = json.loads(con.value)
            if isinstance(loaded, dict):
                new_data = loaded
        new_data.update(context)
        streamers = {}
        for ls in db.scalars(select(LiveStreamers)):
            streamer_cfg = {
                k: v for k, v in ls.as_dict().items()
                if v is not None and k not in ['upload_streamers_id', 'id', 'remark']
            }
            if ls.upload_streamers_id and ls.uploadstreamers is not None:
                streamer_cfg.update({
                    k: v for k, v in ls.uploadstreamers.as_dict().items()
                    if v is not None and k not in ['id', 'template_name']
                })
                if not streamer_cfg.get('uploader'):
                    streamer_cfg['uploader'] = 'biliup-rs'
            streamers[ls.remark] = streamer_cfg
        new_data['streamers'] = streamers
        self.data = new_data

    def save_to_db(self, db):
        from biliup.database.models import Configuration, LiveStreamers, UploadStreamers
        for k, v in self['streamers'].items():
            us = UploadStreamers(**UploadStreamers.filter_parameters(
                {"template_name": k, "tags": v.pop('tags', [k]), ** v}))
            db.add(us)
            db.flush()
            url = v.pop('url')
            urls = url if isinstance(url, list) else [url]  # 兼容 url 输入字符串和列表
            for url in urls:
                ls = LiveStreamers(**LiveStreamers.filter_parameters(
                    {"upload_streamers_id": us.id, "remark": k, "url": url, ** v}))
                db.add(ls)
        del self['streamers']
        configuration = Configuration(key='config', value=json.dumps(self.data))
        db.add(configuration)
        db.commit()

    def load(self, file):
        import yaml
        if file is None:
            if pathlib.Path('config.yaml').exists():
                file = open('config.yaml', 'rb')
            elif pathlib.Path('config.toml').exists():
                self.data['toml'] = True
                file = open('config.toml', "rb")
            else:
                raise FileNotFoundError('未找到配置文件，请先创建配置文件')
        with file as stream:
            if file.name.endswith('.toml'):
                self.data = tomllib.load(stream)
            else:
                self.data = yaml.load(stream, Loader=yaml.FullLoader)

    def create_without_config_input(self, file):
        import yaml
        if file is None:
            if pathlib.Path('config.toml').exists():
                file = open('config.toml', 'rb')
            elif pathlib.Path('config.yaml').exists():
                file = open('config.yaml', encoding='utf-8')
            else:
                try:
                    from importlib.resources import files
                except ImportError:
                    from importlib_resources import files
                shutil.copy(files("biliup.web").joinpath('public/config.toml'), '.')
                file = open('config.toml', 'rb')

            # else:
            #     try:
            #         from importlib.resources import files
            #     except ImportError:
            #         # Try backported to PY<37 `importlib_resources`.
            #         from importlib_resources import files
            #     shutil.copy(files("biliup.web").joinpath('public/config.yaml'), 'common')
            #     file = open('config.yaml', encoding='utf-8')

        with file as stream:
            if file.name.endswith('.toml'):
                self.data = tomllib.load(stream)
                self.data['toml'] = True
            else:
                self.data = yaml.load(stream, Loader=yaml.FullLoader)

    def save(self):
        if self.data.get('toml'):
            import tomli_w
            with open('config.toml', 'rb') as stream:
                old_data = tomllib.load(stream)
                old_data["lines"] = self.data["lines"]
                old_data["threads"] = self.data["threads"]
                old_data["streamers"] = self.data["streamers"]
            with open('config.toml', 'wb') as stream:
                tomli_w.dump(old_data, stream)
        else:
            import tempfile
            import yaml
            target_path = os.path.realpath('config.yaml') if os.path.islink('config.yaml') else 'config.yaml'
            target_dir = os.path.dirname(target_path) or '.'
            old_data = {}
            if os.path.exists(target_path):
                with open(target_path, 'r', encoding='utf-8') as stream:
                    loaded = yaml.safe_load(stream)
                    if isinstance(loaded, dict):
                        old_data = loaded

            user_data = self.data.get("user")
            if isinstance(user_data, dict):
                if not isinstance(old_data.get("user"), dict):
                    old_data["user"] = {}
                if "cookies" in user_data:
                    old_data["user"]["cookies"] = user_data["cookies"]
                if "access_token" in user_data:
                    old_data["user"]["access_token"] = user_data["access_token"]

            for key in ("lines", "threads", "streamers"):
                if key in self.data:
                    old_data[key] = self.data[key]

            tmp_path = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode='w',
                    encoding='utf-8',
                    dir=target_dir,
                    prefix='.config.yaml.',
                    suffix='.tmp',
                    delete=False,
                ) as tmp:
                    tmp_path = tmp.name
                    yaml.safe_dump(old_data, tmp, default_flow_style=False, allow_unicode=True)
                    tmp.flush()
                    os.fsync(tmp.fileno())
                if os.path.exists(target_path):
                    try:
                        shutil.copymode(target_path, tmp_path)
                    except OSError:
                        pass
                os.replace(tmp_path, target_path)
            except Exception:
                if tmp_path and os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass
                raise

    def dump(self, file):
        """ 转储到旧版配置文件 """
        if not file:
            file = 'config.toml'
        if os.path.exists(file):
            from datetime import datetime
            import logging
            logger = logging.getLogger('biliup')
            new_name = f'{file}.backup.{datetime.now().strftime("%Y%m%d%H%M%S")}'
            logger.info(f"{file} 文件已存在，已将原文件重命名为 {new_name}")
            os.rename(file, new_name)
        exclude_keys = ['PluginInfo', 'upload_filename', 'url_upload_count']
        temp = {k: v for k, v in self.data.items() if k not in exclude_keys}
        if self.data.get('yaml') or file.endswith(".yaml"):
            import yaml
            with open(file, 'w+', encoding='utf-8') as stream:
                yaml.dump(temp, stream, default_flow_style=False, allow_unicode=True)
        else:
            import tomli_w
            # print(config)
            with open(file, 'wb') as stream:
                tomli_w.dump(temp, stream)
        return file


config = Config()
