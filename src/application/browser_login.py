# -*- coding: utf-8 -*-
"""扫码登录：打开一个专属浏览器窗口登录抖音，自动读回 Cookie。

做法（只用标准库，不加新依赖）：
- 用电脑自带的 Edge / Chrome 打开抖音，浏览器资料单独放在
  程序目录的 Volume/browser_profile 里，不影响用户平时的浏览器；
- 浏览器启动时带调试端口，程序通过调试协议（Storage.getCookies）
  直接读取包括 HttpOnly 在内的全部 Cookie——用户在窗口里扫码
  登录成功后，程序发现 sessionid 出现，就把 Cookie 拼好交回，
  并关掉这个登录窗口。

本模块只负责“开窗 + 读 Cookie”，保存 Cookie 由调用方完成。
"""

import base64
import json
import os
import shutil
import socket
import struct
import subprocess
import time
from pathlib import Path

__all__ = ["BrowserLogin", "find_browser", "encode_frame", "FrameBuffer"]

LOGIN_URL = "https://www.douyin.com/"
_COOKIE_DOMAIN = "douyin.com"
_LOGIN_KEYS = ("sessionid", "sessionid_ss")


def find_browser() -> str | None:
    """找电脑上可用的浏览器（优先 Edge，Windows 一般自带）。"""
    candidates: list[Path] = []
    for env_name in ("ProgramFiles(x86)", "ProgramFiles", "LocalAppData"):
        base = os.environ.get(env_name)
        if not base:
            continue
        root = Path(base)
        candidates.append(root / "Microsoft" / "Edge" / "Application" / "msedge.exe")
        candidates.append(root / "Google" / "Chrome" / "Application" / "chrome.exe")
    for path in candidates:
        if path.is_file():
            return str(path)
    for name in ("msedge", "chrome", "microsoft-edge", "google-chrome"):
        if found := shutil.which(name):
            return found
    return None


# ---------- 最小 WebSocket 客户端（只够给调试协议发一条命令） ----------


def encode_frame(payload: bytes, opcode: int = 0x1) -> bytes:
    """拼一个客户端帧（客户端帧必须带掩码）。"""
    mask = os.urandom(4)
    header = bytearray([0x80 | opcode])
    length = len(payload)
    if length < 126:
        header.append(0x80 | length)
    elif length <= 0xFFFF:
        header.append(0x80 | 126)
        header += struct.pack(">H", length)
    else:
        header.append(0x80 | 127)
        header += struct.pack(">Q", length)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return bytes(header) + mask + masked


class FrameBuffer:
    """把收到的数据流拆成完整的 WebSocket 消息（处理分片）。"""

    def __init__(self):
        self._buffer = bytearray()
        self._parts: list[bytes] = []

    def feed(self, data: bytes) -> list[bytes]:
        self._buffer += data
        messages = []
        while True:
            parsed = self._parse_one()
            if parsed is None:
                break
            fin, opcode, payload = parsed
            if opcode in (0x1, 0x2, 0x0):
                self._parts.append(payload)
                if fin:
                    messages.append(b"".join(self._parts))
                    self._parts = []
            # ping/close 等控制帧不组装成消息，ping 由调用方回 pong
        return messages

    def _parse_one(self):
        buf = self._buffer
        if len(buf) < 2:
            return None
        fin = bool(buf[0] & 0x80)
        opcode = buf[0] & 0x0F
        masked = bool(buf[1] & 0x80)
        length = buf[1] & 0x7F
        pos = 2
        if length == 126:
            if len(buf) < pos + 2:
                return None
            length = struct.unpack(">H", buf[pos : pos + 2])[0]
            pos += 2
        elif length == 127:
            if len(buf) < pos + 8:
                return None
            length = struct.unpack(">Q", buf[pos : pos + 8])[0]
            pos += 8
        mask = b""
        if masked:
            if len(buf) < pos + 4:
                return None
            mask = bytes(buf[pos : pos + 4])
            pos += 4
        if len(buf) < pos + length:
            return None
        payload = bytes(buf[pos : pos + length])
        del buf[: pos + length]
        if masked:
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        return fin, opcode, payload


def _parse_ws_url(ws_url: str) -> tuple[str, int, str]:
    rest = ws_url.split("://", 1)[1]
    host_port, _, path = rest.partition("/")
    host, _, port = host_port.partition(":")
    return host, int(port or 80), "/" + path


def cdp_get_cookies(ws_url: str, timeout: float = 10.0) -> list[dict]:
    """连调试端口取全部 Cookie（同步调用，慢操作请放线程里）。"""
    host, port, path = _parse_ws_url(ws_url)
    key = base64.b64encode(os.urandom(16)).decode()
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "\r\n"
    )
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(request.encode())
        response = bytearray()
        while b"\r\n\r\n" not in response:
            chunk = sock.recv(4096)
            if not chunk:
                raise ConnectionError("调试端口没有回应")
            response += chunk
        head, _, rest = bytes(response).partition(b"\r\n\r\n")
        if b"101" not in head.split(b"\r\n", 1)[0]:
            raise ConnectionError("调试端口拒绝连接")
        sock.sendall(
            encode_frame(
                json.dumps(
                    {"id": 1, "method": "Storage.getCookies", "params": {}}
                ).encode()
            )
        )
        frames = FrameBuffer()
        if rest:
            messages = frames.feed(rest)
        else:
            messages = []
        deadline = time.time() + timeout
        while time.time() < deadline:
            for message in messages:
                try:
                    data = json.loads(message)
                except ValueError:
                    continue
                if data.get("id") == 1:
                    return list(
                        data.get("result", {}).get("cookies", [])
                    )
            chunk = sock.recv(65536)
            if not chunk:
                break
            messages = frames.feed(chunk)
    raise TimeoutError("读取 Cookie 超时")


def pick_douyin_cookie(cookies: list[dict]) -> str | None:
    """从全部 Cookie 里挑抖音的；登录成功（有 sessionid）才返回
    拼好的 Cookie 字符串，否则返回 None。"""
    chosen: dict[str, tuple[str, str]] = {}
    for cookie in cookies:
        domain = str(cookie.get("domain", ""))
        if _COOKIE_DOMAIN not in domain:
            continue
        name = str(cookie.get("name", ""))
        value = str(cookie.get("value", ""))
        if not name:
            continue
        old = chosen.get(name)
        # 同名的以 .douyin.com 这种大范围域名的为准
        if old is None or (domain.startswith(".") and not old[1].startswith(".")):
            chosen[name] = (value, domain)
    if not any(
        key in chosen and chosen[key][0] for key in _LOGIN_KEYS
    ):
        return None
    return "; ".join(f"{name}={value}" for name, (value, _) in chosen.items())


# ---------- 登录窗口 ----------


class BrowserLogin:
    """一个专属资料目录的浏览器登录窗口。"""

    def __init__(
        self,
        profile_dir,
        url: str = LOGIN_URL,
        browser_path: str | None = None,
    ):
        self.profile_dir = Path(profile_dir)
        self.url = url
        self.browser_path = browser_path or find_browser()
        self.process: subprocess.Popen | None = None
        self.ws_url: str | None = None

    def start(self) -> bool:
        """打开登录窗口；成功返回 True（窗口已开、调试端口已就绪）。"""
        if not self.browser_path:
            return False
        self.profile_dir.mkdir(parents=True, exist_ok=True)
        self.process = subprocess.Popen(
            [
                self.browser_path,
                "--remote-debugging-port=0",
                f"--user-data-dir={self.profile_dir}",
                "--no-first-run",
                "--no-default-browser-check",
                "--window-size=1150,820",
                self.url,
            ]
        )
        port_file = self.profile_dir / "DevToolsActivePort"
        deadline = time.time() + 20
        while time.time() < deadline:
            if self.process.poll() is not None:
                return False
            try:
                lines = port_file.read_text(encoding="utf-8").splitlines()
                if len(lines) >= 2 and lines[0].strip().isdigit():
                    self.ws_url = (
                        f"ws://127.0.0.1:{lines[0].strip()}{lines[1].strip()}"
                    )
                    return True
            except OSError:
                pass
            time.sleep(0.25)
        return False

    def poll_cookie(self) -> str | None:
        """问一次浏览器：登录成功了就返回 Cookie 字符串，否则 None。
        同步且可能慢几百毫秒，调用方应放在线程里执行。"""
        if not self.ws_url:
            return None
        try:
            cookies = cdp_get_cookies(self.ws_url)
        except Exception:
            return None
        return pick_douyin_cookie(cookies)

    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def close(self) -> None:
        if self.process is None:
            return
        try:
            if os.name == "nt":
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(self.process.pid)],
                    capture_output=True,
                    timeout=10,
                )
            else:
                self.process.terminate()
        except Exception:
            pass
        self.process = None
