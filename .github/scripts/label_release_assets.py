#!/usr/bin/env python3
"""Apply human-readable display labels (and clear desktop installer names) to GitHub Release assets.

Python wheels (.whl) keep their PEP 427 filenames untouched so `pip install` always works;
only their GitHub Release display `label` is updated. Windows desktop installers (.exe/.msi)
get both a clear versioned filename and a readable display label.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from typing import Optional, Tuple


def classify_asset(name: str, tag: str) -> Optional[Tuple[Optional[str], str]]:
    """Return (new_name_or_None, label) for a release asset, or None to leave unchanged."""
    lower = name.lower()

    if lower.endswith("-setup.exe") or (lower.endswith(".exe") and "setup" in lower):
        new_name = f"biliup-{tag}-windows-x64-setup.exe"
        return new_name, f"Windows 64位桌面安装包 EXE（推荐） - {new_name}"
    if lower.endswith(".msi"):
        new_name = f"biliup-{tag}-windows-x64.msi"
        return new_name, f"Windows 64位桌面安装包 MSI - {new_name}"

    if lower.endswith(".tar.gz"):
        return None, f"Python 源码包 (sdist) - {name}"

    if not lower.endswith(".whl"):
        return None

    if "win_amd64" in lower:
        return None, "Python Wheel · Windows 64位 (win_amd64)"
    if "win32" in lower:
        return None, "Python Wheel · Windows 32位 (win32)"

    if "manylinux" in lower:
        if "x86_64" in lower:
            return None, "Python Wheel · Linux x86_64 (glibc / 常见服务器推荐)"
        if "aarch64" in lower:
            return None, "Python Wheel · Linux ARM64 (glibc / 树莓派·ARM服务器)"
        if "armv7l" in lower:
            return None, "Python Wheel · Linux ARMv7 32位 (glibc)"
        if "i686" in lower:
            return None, "Python Wheel · Linux x86 32位 (glibc)"
        if "ppc64le" in lower:
            return None, "Python Wheel · Linux ppc64le (glibc)"
        if "s390x" in lower:
            return None, "Python Wheel · Linux s390x (glibc)"

    if "musllinux" in lower:
        if "x86_64" in lower:
            return None, "Python Wheel · Linux x86_64 (Alpine / musl)"
        if "aarch64" in lower:
            return None, "Python Wheel · Linux ARM64 (Alpine / musl)"
        if "armv7l" in lower:
            return None, "Python Wheel · Linux ARMv7 (Alpine / musl)"
        if "i686" in lower:
            return None, "Python Wheel · Linux x86 32位 (Alpine / musl)"

    if "macosx" in lower:
        if "arm64" in lower or "aarch64" in lower:
            return None, "Python Wheel · macOS Apple Silicon (arm64)"
        if "x86_64" in lower:
            return None, "Python Wheel · macOS Intel (x86_64)"

    if "linux_aarch64" in lower or "android" in lower:
        return None, "Python Wheel · Android Termux (aarch64)"

    return None


def api_request(method: str, url: str, token: str, payload: Optional[dict] = None) -> dict:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
            "User-Agent": "biliup-release-labeler",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def prepare_assets(target_dir: str, tag: str) -> int:
    if not os.path.isdir(target_dir):
        print(f"Asset directory not found: {target_dir}", file=sys.stderr)
        return 1

    version = tag[1:] if tag.startswith("v") else tag
    used_names = set(os.listdir(target_dir))

    for name in sorted(os.listdir(target_dir)):
        src = os.path.join(target_dir, name)
        if not os.path.isfile(src):
            continue
        result = classify_asset(name, version)
        if not result:
            print(src)
            continue

        new_name, label = result
        dst = src
        if new_name and new_name != name:
            candidate = os.path.join(target_dir, new_name)
            if new_name not in used_names:
                os.replace(src, candidate)
                used_names.discard(name)
                used_names.add(new_name)
                dst = candidate
            else:
                print(
                    f"Warning: target filename already exists, keeping {name}",
                    file=sys.stderr,
                )

        print(f"{dst}#{label}")

    return 0


def main() -> int:
    if len(sys.argv) >= 2 and sys.argv[1] == "prepare":
        if len(sys.argv) != 4:
            print(
                "Usage: label_release_assets.py prepare <dir> <tag>",
                file=sys.stderr,
            )
            return 1
        return prepare_assets(sys.argv[2], sys.argv[3])

    repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    tag = os.environ.get("RELEASE_TAG", "").strip()
    if tag.startswith("refs/tags/"):
        tag = tag[len("refs/tags/") :]

    if not repo or not token or not tag:
        print("Missing GITHUB_REPOSITORY, GITHUB_TOKEN, or RELEASE_TAG", file=sys.stderr)
        return 1

    release = api_request(
        "GET",
        f"https://api.github.com/repos/{repo}/releases/tags/{tag}",
        token,
    )
    assets = release.get("assets", [])
    existing_names = {a["name"] for a in assets}

    updated = 0
    for asset in assets:
        name = asset["name"]
        result = classify_asset(name, tag)
        if not result:
            continue
        new_name, label = result
        payload: dict = {"label": label}
        if new_name and new_name != name and new_name not in existing_names:
            payload["name"] = new_name
            existing_names.add(new_name)
        elif asset.get("label") == label:
            continue

        asset_id = asset["id"]
        try:
            api_request(
                "PATCH",
                f"https://api.github.com/repos/{repo}/releases/assets/{asset_id}",
                token,
                payload,
            )
            print(f"Updated asset {name} -> {payload}")
            updated += 1
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            print(f"Warning: failed to update asset {name}: {exc.code} {body}", file=sys.stderr)

    print(f"Done. Updated {updated} asset(s) on {repo}@{tag}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
