<div align="center">
  <img src="https://docs.biliup.rs/home.png" alt="biliup" width="300" height="300"/>
</div>

<div align="center">

[![Python](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/downloads/)
[![Release](https://img.shields.io/github/v/release/pingshazikdjnd/biliup-1.0.7)](https://github.com/pingshazikdjnd/biliup-1.0.7/releases/latest)
[![Downloads](https://img.shields.io/github/downloads/pingshazikdjnd/biliup-1.0.7/total)](https://github.com/pingshazikdjnd/biliup-1.0.7/releases)
[![License](https://img.shields.io/github/license/pingshazikdjnd/biliup-1.0.7)](https://github.com/pingshazikdjnd/biliup-1.0.7/blob/dev-1.0.7/LICENSE)

[![GitHub Issues](https://img.shields.io/github/issues/pingshazikdjnd/biliup-1.0.7?label=Issues)](https://github.com/pingshazikdjnd/biliup-1.0.7/issues)
[![GitHub Stars](https://img.shields.io/github/stars/pingshazikdjnd/biliup-1.0.7)](https://github.com/pingshazikdjnd/biliup-1.0.7/stargazers)
[![GitHub Forks](https://img.shields.io/github/forks/pingshazikdjnd/biliup-1.0.7)](https://github.com/pingshazikdjnd/biliup-1.0.7/forks)

</div>

> 基于上游 [biliup/biliup](https://github.com/biliup/biliup) 二次开发维护的稳定增强分支，保留原有开箱即用 WebUI 与多平台录播投稿能力，并重点强化了录制稳定性、上传可靠性与运维安全。

## 🛠️ 核心功能与本分支增强

### 基础能力
* 开箱即用，提供可视化 WebUI 界面与多种安装方式
* 多主播 24×7 无人值守录制 / 上传，高自定义投稿元信息
* 边录边传不落盘急速上传，节省本地硬盘空间

### 本分支新增 / 增强
* **外挂 `mesio` 下载器支持**：FLV/HLS 实时时间戳修复，解决 B 站 720p 录播单 P 时间戳跳变问题，支持全局与单主播配置覆写及自动回退 `ffmpeg`
* **主备 CDN 智能回退 & 等待转码流录制**：主 CDN 短时断流自动切换回退线路；开播时可等待目标清晰度转码完成后再开始录制
* **合集下拉选择 & 跨夜标题日期校准**：B 站投稿合集支持可视化下拉选择；跨夜连续录播以最早分段开录日期为准
* **双水位磁盘保护 & 告警分级**：可配置软水位（分段完成触发提前上传清理）与硬水位（告警并暂停新开录）；支持 QQ 邮箱 / SMTP 与 Webhook 告警，不可自愈上传错误首次熔断，杜绝上传失败误删录像
* **WebUI 全路径密码鉴权**：开启 `--password` 后对 API、静态资源、代理及实时日志 WebSocket 实施全路径鉴权保护，主页内置本分支更新日志

## 📜 更新日志与问题反馈

- **[GitHub Releases »](https://github.com/pingshazikdjnd/biliup-1.0.7/releases)**
- **[CHANGELOG.md »](./CHANGELOG.md)**
- **[问题反馈 / Issues »](https://github.com/pingshazikdjnd/biliup-1.0.7/issues)**
- 上游基础配置参考文档：[docs.biliup.rs »](https://docs.biliup.rs)（通用基础配置可参考，新增特性请以本仓库 Releases 与 WebUI 说明为准）

## 🚀 快速开始

> ⚠️ 请从本仓库 [Releases](https://github.com/pingshazikdjnd/biliup-1.0.7/releases/latest) 安装，**不要直接执行 `pip install biliup` 或 `uv tool install biliup`**（否则会安装上游官方版本）。

### Windows
- 前往本仓库 [Releases](https://github.com/pingshazikdjnd/biliup-1.0.7/releases/latest) 下载最新的 `.exe` / `.msi` 安装包直接安装；
- 或下载对应 Python 架构的 `.whl` 文件通过 `pip` 安装。

### Linux / macOS
1. 前往本仓库 [Releases](https://github.com/pingshazikdjnd/biliup-1.0.7/releases/latest) 复制对应架构的 `.whl` 下载链接（以下以 Linux x86_64 虚拟环境安装 `v1.0.8` 为例）：
   ```bash
   python3 -m venv /opt/biliup
   /opt/biliup/bin/pip install -U pip
   /opt/biliup/bin/pip install "https://github.com/pingshazikdjnd/biliup-1.0.7/releases/download/v1.0.8/biliup-1.0.8-cp38-abi3-manylinux2014_x86_64.manylinux_2_17_x86_64.whl"
   ln -sf /opt/biliup/bin/biliup /usr/local/bin/biliup
   ```
   > 也可以先将对应架构的 `.whl` 下载到本地后执行 `/opt/biliup/bin/pip install ./biliup-*.whl`。
2. 启动：
   ```bash
   biliup -P 19159 --password <你的密码> start
   ```
   （如不需要启用 WebUI 访问密码，可省略 `--password <你的密码>`）
3. 访问 WebUI：`http://your-ip:19159`

### 可选：安装 `mesio` 下载器（Linux x86_64）
如需使用 `mesio` 下载器进行 FLV/HLS 实时时间戳修复，可从 [hua0512/rust-srec Releases](https://github.com/hua0512/rust-srec/releases) 下载对应平台二进制文件（以下以 Linux x86_64 为例）：
```bash
curl -fL https://github.com/hua0512/rust-srec/releases/download/mesio-v0.8.1/mesio-x86_64-unknown-linux-musl -o /usr/local/bin/mesio && chmod +x /usr/local/bin/mesio && mesio --version
```
安装到 `/usr/local/bin/mesio` 后，WebUI 中的 `mesio_path` 留空即可自动检测。

---

## 🧑‍💻 开发

### frontend

1. 确保 Node.js 版本 ≥ 18
2. 安装依赖：`npm i`
3. 启动开发服务器：`npm run dev`
4. 访问：`http://localhost:3000`

### backend

1. 安装依赖 `maturin dev`
2. `npm run build` 确保 `/biliup/web/public` 目录存在构建好的前端静态资源
3. 启动 Biliup：`python3 -m biliup`

## 🤝 Credits
* Thanks upstream [biliup/biliup](https://github.com/biliup/biliup).
* Thanks [hua0512/rust-srec](https://github.com/hua0512/rust-srec) (`mesio`).
* Thanks `ykdl`, `yt-dlp`, `streamlink` for downloaders.
* Thanks `THMonster/danmaku`.

## ⭐ Stars
[![Star History Chart](https://api.star-history.com/svg?repos=pingshazikdjnd/biliup-1.0.7&type=Date)](https://star-history.com/#pingshazikdjnd/biliup-1.0.7&Date)
