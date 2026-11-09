# -*- coding: utf-8 -*-
"""Web UI 模式（收藏版）：本地网页界面，全程鼠标操作。

在主菜单选择「Web UI 模式」后，本模块在本机启动一个只监听
127.0.0.1 的网页服务并自动打开浏览器。网页上的按钮通过本模块
调用与终端模式完全相同的下载引擎（TikTok 类），下载规则、
下载记录共用同一套配置与数据库。

退出方式：在网页上点「关闭 Web UI」按钮，或回到程序窗口按回车。
"""

import asyncio
import socket
import threading
import webbrowser
from collections import deque
from time import localtime, strftime, time

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from ..custom import EDITION
from ..interface import API
from ..translation import _
from .main_terminal import TikTok

__all__ = ["WebUIServer"]

# 下载规则设置的全部键与默认值（与终端「下载规则设置」保持一致）
RULE_DEFAULTS = {
    "album_threshold": 2,
    "live_folder_music": True,
    "video_root_mode": True,
    "folder_show_date": True,
    "folder_show_type": True,
    "folder_show_title": False,
    "folder_show_id": False,
    "file_show_date": True,
    "file_show_type": True,
    "file_show_author": True,
    "file_show_title": True,
    "file_show_id": False,
}
RULE_BOOL_KEYS = [k for k, v in RULE_DEFAULTS.items() if isinstance(v, bool)]

# 记录来源的展示顺序（与终端「删除作者下载记录」一致）
SOURCE_ORDER = {
    "账号作品": 0,
    "收藏作品": 1,
    "合集作品": 2,
    "收藏夹": 3,
    "链接作品": 4,
    "早期记录": 5,
}


def _now() -> str:
    return strftime("%H:%M:%S", localtime())


class _LogProxy:
    """日志代理：把引擎写日志的同一份文本同步到网页日志区，
    其余行为（控制台打印、文件记录、属性读写）与原日志器一致。"""

    def __init__(self, wrapped, sink: deque):
        object.__setattr__(self, "_wrapped", wrapped)
        object.__setattr__(self, "_sink", sink)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_wrapped"), name)

    def __setattr__(self, name, value):
        if name in ("_wrapped", "_sink"):
            object.__setattr__(self, name, value)
        else:
            setattr(object.__getattribute__(self, "_wrapped"), name, value)

    def _push(self, text) -> None:
        try:
            object.__getattribute__(self, "_sink").append((_now(), str(text)))
        except Exception:
            pass

    def info(self, text, *args, **kwargs):
        self._push(text)
        return self._wrapped.info(text, *args, **kwargs)

    def warning(self, text, *args, **kwargs):
        self._push(text)
        return self._wrapped.warning(text, *args, **kwargs)

    def error(self, text, *args, **kwargs):
        self._push(text)
        return self._wrapped.error(text, *args, **kwargs)

    def print(self, text, *args, **kwargs):
        self._push(text)
        return self._wrapped.print(text, *args, **kwargs)


class WebUIServer:
    """收藏版 Web UI 服务：持有下载引擎，处理网页的全部请求。"""

    def __init__(self, app, engine_factory=None):
        self.app = app
        # 测试注入点：替换真实引擎构造方式，生产环境为 None
        self._engine_factory = engine_factory
        self.engine = None
        self.logs: deque = deque(maxlen=300)
        self.job = {
            "running": False,
            "title": "",
            "step": "",
            "started_at": 0.0,
            "error": "",
        }
        self._job_task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._collects_cache: dict[str, str] = {}
        self.port: int | None = None

    # ---------- 引擎管理 ----------

    def _build_engine(self) -> None:
        logger = self.app.parameter.logger
        if not isinstance(logger, _LogProxy):
            self.app.parameter.logger = _LogProxy(logger, self.logs)
        if self._engine_factory is not None:
            self.engine = self._engine_factory()
        else:
            self.engine = TikTok(
                self.app.parameter,
                self.app.database,
                server_mode=True,
            )
        API._web_progress_hook = self._on_progress

    async def _refresh_engine(self) -> None:
        """配置变更后与终端一致：重读配置并重建引擎，立即生效。"""
        await self.app.check_settings()
        self._build_engine()

    def _on_progress(self, text, count, page) -> None:
        if self.job["running"]:
            self.job["step"] = _(
                "正在拉取{text}列表（已获取 {count} 条，第 {page} 页）…"
            ).format(text=text, count=count, page=page)

    # ---------- 下载任务 ----------

    async def _start_job(self, title: str, factory):
        if self.job["running"]:
            return JSONResponse(
                {"ok": False, "error": _("已有任务正在进行，等它完成或先停止它")},
                status_code=409,
            )
        if not self.app.parameter.cookie_state:
            return JSONResponse(
                {
                    "ok": False,
                    "error": _("Cookie 还没设置，先在页面上方把 Cookie 粘贴保存"),
                },
                status_code=400,
            )
        self.logs.clear()
        self.job = {
            "running": True,
            "title": title,
            "step": _("正在准备…"),
            "started_at": time(),
            "error": "",
        }
        self._job_task = asyncio.create_task(self._run_job(factory))
        return {"ok": True}

    async def _run_job(self, factory) -> None:
        try:
            await factory()
            self.job["step"] = _("已完成")
        except asyncio.CancelledError:
            self.job["step"] = _("已停止")
            self.logs.append(
                (
                    _now(),
                    _(
                        "任务已被停止。已经下载好的文件不会白下，"
                        "下次下载时会自动跳过它们。"
                    ),
                )
            )
        except Exception as error:  # noqa: BLE001 - 网页需要看到失败原因
            self.job["error"] = str(error)
            self.job["step"] = _("出错了")
            self.logs.append((_now(), _("出错：{error}").format(error=error)))
        finally:
            self.job["running"] = False

    async def _job_collection(self) -> None:
        sec_user_id = await self.engine.check_sec_user_id(self.engine.owner.url)
        if not sec_user_id:
            raise RuntimeError(
                _("主页链接取不到账号信息，请在页面上方重新粘贴你自己的抖音主页链接")
            )
        await self.engine._deal_collection_data(sec_user_id)

    async def _job_collects(self, ids: list[str]) -> None:
        items = [
            (self._collects_cache[i], i) for i in ids if i in self._collects_cache
        ]
        if not items:
            raise RuntimeError(
                _("没有可下载的收藏夹，请先点「查看收藏夹」重新拉取列表")
            )
        for name, collect_id in items:
            await self.engine._deal_collects_data(name, collect_id)

    async def _job_music(self) -> None:
        await self.engine.collection_music_interactive()

    def _job_state(self) -> dict:
        state = dict(self.job)
        state["elapsed"] = (
            int(time() - state["started_at"]) if state["started_at"] else 0
        )
        state["logs"] = [f"[{t}] {s}" for t, s in self.logs]
        return state

    # ---------- 网页接口 ----------

    def build_app(self) -> FastAPI:
        app = FastAPI(title="DouK-Downloader Web UI")

        @app.get("/", response_class=HTMLResponse)
        async def index():
            return PAGE_HTML

        @app.get("/api/state")
        async def state():
            return {
                "cookie_set": bool(self.app.parameter.cookie_state),
                "owner_set": bool(getattr(self.engine.owner, "url", "")),
                "edition": EDITION,
            }

        @app.post("/api/cookie")
        async def save_cookie(request: Request):
            body = await request.json()
            cookie = (body.get("cookie") or "").strip()
            if not cookie:
                return JSONResponse(
                    {"ok": False, "error": _("内容是空的")}, status_code=400
                )
            if not self.app.cookie.run(cookie, tiktok=False):
                return JSONResponse(
                    {
                        "ok": False,
                        "error": _("这段内容不像有效的 Cookie，请重新复制"),
                    },
                    status_code=400,
                )
            await self._refresh_engine()
            return {"ok": True}

        @app.post("/api/owner_url")
        async def save_owner_url(request: Request):
            body = await request.json()
            url = (body.get("url") or "").strip()
            if not url:
                return JSONResponse(
                    {"ok": False, "error": _("内容是空的")}, status_code=400
                )
            if not await self.engine.check_sec_user_id(url):
                return JSONResponse(
                    {
                        "ok": False,
                        "error": _(
                            "这个链接取不到账号信息，请确认是自己的抖音主页链接"
                        ),
                    },
                    status_code=400,
                )
            data = self.app.settings.read()
            owner = dict(data.get("owner_url") or {})
            owner["url"] = url
            data["owner_url"] = owner
            self.app.settings.update(data)
            self.engine.owner.url = url
            return {"ok": True}

        @app.get("/api/collects")
        async def list_collects():
            if not self.app.parameter.cookie_state:
                return JSONResponse(
                    {"ok": False, "error": _("Cookie 还没设置")}, status_code=400
                )
            items = await self.engine.web_list_collects()
            self._collects_cache = {
                i["id"]: i["name"] for i in items if i.get("id")
            }
            return {"items": items}

        @app.post("/api/job/collection")
        async def job_collection():
            return await self._start_job(_("下载收藏作品"), self._job_collection)

        @app.post("/api/job/collects")
        async def job_collects(request: Request):
            body = await request.json()
            ids = [str(i) for i in body.get("ids") or []]
            return await self._start_job(
                _("下载收藏夹"), lambda: self._job_collects(ids)
            )

        @app.post("/api/job/music")
        async def job_music():
            return await self._start_job(_("下载收藏音乐"), self._job_music)

        @app.get("/api/job")
        async def job_state():
            return self._job_state()

        @app.post("/api/job/stop")
        async def job_stop():
            if self._job_task and not self._job_task.done():
                self._job_task.cancel()
            return {"ok": True}

        @app.get("/api/rules")
        async def get_rules():
            data = self.app.settings.read()
            return {k: data.get(k, v) for k, v in RULE_DEFAULTS.items()}

        @app.post("/api/rules")
        async def save_rules(request: Request):
            body = await request.json()
            try:
                threshold = int(body.get("album_threshold", 2))
                if not 1 <= threshold <= 999:
                    raise ValueError
            except (TypeError, ValueError):
                return JSONResponse(
                    {"ok": False, "error": _("图片张数不正确")}, status_code=400
                )
            data = self.app.settings.read()
            data["album_threshold"] = threshold
            for key in RULE_BOOL_KEYS:
                data[key] = bool(body.get(key, RULE_DEFAULTS[key]))
            self.app.settings.update(data)
            await self._refresh_engine()
            return {"ok": True}

        @app.get("/api/authors")
        async def list_authors(keyword: str = ""):
            rows = await self.app.recorder.list_authors()
            if keyword:
                rows = [r for r in rows if keyword in (r.get("name") or "")]
            rows.sort(
                key=lambda r: (SOURCE_ORDER.get(r.get("source"), 9), -r["count"])
            )
            return {"items": rows}

        @app.post("/api/authors/delete")
        async def delete_author(request: Request):
            body = await request.json()
            deleted = await self.app.recorder.delete_by_author(
                str(body.get("uid") or ""), str(body.get("source") or "")
            )
            return {"ok": True, "deleted": deleted}

        @app.post("/api/records/clear")
        async def clear_records():
            await self.app.recorder.delete_ids("ALL")
            return {"ok": True}

        @app.post("/api/shutdown")
        async def shutdown():
            self._stop.set()
            return {"ok": True}

        return app

    # ---------- 服务启动 ----------

    @staticmethod
    def _pick_port() -> int:
        for port in range(8765, 8775):
            with socket.socket() as sock:
                try:
                    sock.bind(("127.0.0.1", port))
                    return port
                except OSError:
                    continue
        return 8765

    async def serve(self) -> None:
        self._build_engine()
        self.port = self._pick_port()
        server = uvicorn.Server(
            uvicorn.Config(
                self.build_app(),
                host="127.0.0.1",
                port=self.port,
                log_level="warning",
            )
        )
        task = asyncio.create_task(server.serve())
        url = f"http://127.0.0.1:{self.port}"
        console = self.app.console
        console.print()
        console.print(_("Web UI 已启动，正在打开浏览器：{url}").format(url=url))
        console.print(
            _(
                "在网页上点按钮就能下载，不用再敲键盘。"
                "退出 Web UI：点网页右上角的「关闭 Web UI」，或回到这个窗口按回车。"
            )
        )

        def _wait_enter() -> None:
            try:
                input()
            except (EOFError, KeyboardInterrupt):
                pass
            self._stop.set()

        threading.Thread(target=_wait_enter, daemon=True).start()
        try:
            webbrowser.open(url)
        except Exception:
            pass
        try:
            await self._stop.wait()
        finally:
            if self._job_task and not self._job_task.done():
                self._job_task.cancel()
            server.should_exit = True
            await task
            API._web_progress_hook = None
            console.print(_("Web UI 已关闭。"))

PAGE_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN" data-theme="light">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>DouK-Downloader 收藏版</title>
<script>
  (function () {
    var saved = localStorage.getItem("dy-theme");
    var theme = saved || (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
    document.documentElement.setAttribute("data-theme", theme);
  })();
</script>
<style>
  :root {
    --bg: #f2f4f7; --card: #ffffff; --text: #1c2333; --muted: #6b7280;
    --line: #eef1f5; --accent: #fe2c55; --accent-dark: #e6264c;
    --ghost: #eef1f5; --ok-bg: #e6f4ea; --ok-text: #2e7d32;
    --warn-bg: #fff7e0; --warn-text: #8d6e00; --log-bg: #111827; --log-text: #a7f3d0;
    --input-border: #d1d5db; --off: #d1d5db;
  }
  [data-theme="dark"] {
    --bg: #12151c; --card: #1d212b; --text: #e8eaf0; --muted: #9aa1af;
    --line: #2b303c; --ghost: #2b303c; --ok-bg: #17351f; --ok-text: #7dd87d;
    --warn-bg: #3a2f10; --warn-text: #ffd75e; --log-bg: #0a0d13; --log-text: #7de8b5;
    --input-border: #3a4050; --off: #4a5062;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: "Microsoft YaHei", sans-serif; background: var(--bg); color: var(--text); padding: 18px; transition: background .2s; }
  .wrap { max-width: 860px; margin: 0 auto; }
  .topbar { display: flex; justify-content: space-between; align-items: center; gap: 10px; flex-wrap: wrap; background: var(--card); border-radius: 14px; padding: 16px 20px; box-shadow: 0 1px 4px rgba(0,0,0,.08); }
  .logo { font-size: 20px; font-weight: 700; }
  .logo span { color: var(--accent); }
  .top-actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
  .status { font-size: 14px; color: var(--ok-text); background: var(--ok-bg); padding: 6px 12px; border-radius: 999px; }
  .status.bad { color: var(--warn-text); background: var(--warn-bg); }
  .btn { border: none; cursor: pointer; background: var(--accent); color: #fff; font-size: 15px; padding: 10px 22px; border-radius: 999px; }
  .btn:hover { background: var(--accent-dark); }
  .btn.ghost { background: var(--ghost); color: var(--text); }
  .btn.small { font-size: 13px; padding: 7px 16px; }
  .btn:disabled { opacity: .5; cursor: default; }
  .alert { background: var(--warn-bg); color: var(--warn-text); border-radius: 14px; padding: 16px 20px; margin-top: 16px; font-size: 14.5px; }
  .alert textarea { width: 100%; margin-top: 10px; border: 1px solid var(--input-border); border-radius: 10px; padding: 10px 12px; font-size: 13.5px; background: var(--card); color: var(--text); resize: vertical; min-height: 52px; }
  .alert .row-end { margin-top: 10px; text-align: right; display: flex; gap: 8px; justify-content: flex-end; }
  .cards { display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; margin-top: 16px; }
  .card { background: var(--card); border-radius: 14px; padding: 22px 18px; text-align: center; box-shadow: 0 1px 4px rgba(0,0,0,.08); }
  .card .icon { font-size: 40px; }
  .card h3 { margin: 10px 0 6px; font-size: 17px; }
  .card p { font-size: 13px; color: var(--muted); min-height: 34px; }
  .card .btn { margin-top: 10px; }
  .panel { background: var(--card); border-radius: 14px; padding: 18px 20px; margin-top: 16px; box-shadow: 0 1px 4px rgba(0,0,0,.08); }
  .panel h3 { font-size: 16px; margin-bottom: 10px; }
  .progress-line { display: flex; justify-content: space-between; font-size: 14px; color: var(--muted); gap: 10px; flex-wrap: wrap; }
  .log { background: var(--log-bg); color: var(--log-text); font-family: Consolas, monospace; font-size: 12.5px; border-radius: 10px; padding: 12px 14px; height: 150px; overflow-y: auto; line-height: 1.7; margin-top: 10px; white-space: pre-wrap; word-break: break-all; }
  .folder-row { display: flex; align-items: center; gap: 10px; padding: 10px 6px; border-bottom: 1px solid var(--line); font-size: 15px; cursor: pointer; }
  .folder-row:last-of-type { border-bottom: none; }
  .folder-row input { width: 18px; height: 18px; accent-color: var(--accent); }
  .links { display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; margin-top: 16px; }
  .link-card { background: var(--card); border-radius: 14px; padding: 16px; text-align: center; font-size: 15px; cursor: pointer; box-shadow: 0 1px 4px rgba(0,0,0,.08); }
  .link-card:hover { outline: 2px solid var(--accent); }
  .row { display: flex; justify-content: space-between; align-items: center; gap: 10px; padding: 11px 2px; border-bottom: 1px solid var(--line); font-size: 15px; }
  .row:last-child { border-bottom: none; }
  .row small { display: block; color: var(--muted); font-size: 12.5px; margin-top: 2px; }
  .switch { position: relative; width: 52px; height: 28px; flex: none; cursor: pointer; }
  .switch i { position: absolute; inset: 0; background: var(--off); border-radius: 999px; transition: .2s; }
  .switch i:after { content: ""; position: absolute; width: 22px; height: 22px; background: #fff; border-radius: 50%; top: 3px; left: 3px; transition: .2s; box-shadow: 0 1px 3px rgba(0,0,0,.25); }
  .switch.on i { background: var(--accent); }
  .switch.on i:after { left: 27px; }
  .stepper { display: flex; align-items: center; gap: 12px; }
  .stepper button { width: 34px; height: 34px; border-radius: 50%; border: none; background: var(--ghost); color: var(--text); font-size: 18px; cursor: pointer; }
  .stepper b { font-size: 16px; min-width: 52px; text-align: center; }
  .searchbar { display: flex; gap: 8px; margin-bottom: 6px; }
  .searchbar input { flex: 1; border: 1px solid var(--input-border); border-radius: 10px; padding: 9px 12px; font-size: 14px; background: var(--card); color: var(--text); }
  .author-row { display: flex; justify-content: space-between; align-items: center; gap: 10px; padding: 10px 2px; border-bottom: 1px solid var(--line); font-size: 15px; }
  .author-row:last-child { border-bottom: none; }
  .author-row .meta { color: var(--muted); font-size: 13px; }
  .row-end { margin-top: 12px; text-align: right; display: flex; gap: 8px; justify-content: flex-end; flex-wrap: wrap; }
  .hint { margin-top: 14px; font-size: 13px; color: var(--muted); text-align: center; }
  .toast { position: fixed; left: 50%; bottom: 28px; transform: translateX(-50%); background: var(--text); color: var(--bg); padding: 10px 20px; border-radius: 999px; font-size: 14px; opacity: 0; pointer-events: none; transition: opacity .25s; max-width: 90vw; text-align: center; }
  .toast.show { opacity: .95; }
  @media (max-width: 640px) { .cards, .links { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<div class="wrap">
  <div class="topbar">
    <div class="logo">DouK-Downloader <span>收藏版</span></div>
    <div class="top-actions">
      <div class="status" id="cookieStatus">正在检查…</div>
      <button class="btn ghost small" id="themeBtn" onclick="toggleTheme()">🌙 深色</button>
      <button class="btn ghost small" onclick="shutdown()">关闭 Web UI</button>
    </div>
  </div>

  <div class="alert" id="cookieBox" style="display:none">
    <b>先设置 Cookie 才能下载。</b>在电脑浏览器里打开抖音网页版、登录后按 F12 复制 Cookie，粘贴到下面（也可以点「从剪贴板粘贴」自动填）：
    <textarea id="cookieText" placeholder="把 Cookie 粘贴到这里"></textarea>
    <div class="row-end">
      <button class="btn ghost small" onclick="pasteInto('cookieText')">从剪贴板粘贴</button>
      <button class="btn small" onclick="saveCookie()">保存 Cookie</button>
    </div>
  </div>

  <div class="alert" id="ownerBox" style="display:none">
    <b>下载收藏作品需要你自己的抖音主页链接（只需填一次）。</b>打开抖音 App → 我 → 右上角分享 → 复制链接，粘贴到下面：
    <textarea id="ownerText" placeholder="把你的抖音主页链接粘贴到这里"></textarea>
    <div class="row-end">
      <button class="btn ghost small" onclick="pasteInto('ownerText')">从剪贴板粘贴</button>
      <button class="btn small" onclick="saveOwner()">保存主页链接</button>
    </div>
  </div>

  <div class="cards">
    <div class="card">
      <div class="icon">⭐</div>
      <h3>下载收藏作品</h3>
      <p>把你收藏的全部作品批量下载下来</p>
      <button class="btn" onclick="startJob('/api/job/collection')">开始下载</button>
    </div>
    <div class="card">
      <div class="icon">📁</div>
      <h3>下载收藏夹</h3>
      <p>先看你有哪些收藏夹，勾选了再下</p>
      <button class="btn" onclick="loadCollects()">查看收藏夹</button>
    </div>
    <div class="card">
      <div class="icon">🎵</div>
      <h3>下载收藏音乐</h3>
      <p>把你收藏的音乐批量下载下来</p>
      <button class="btn" onclick="startJob('/api/job/music')">开始下载</button>
    </div>
  </div>

  <div class="panel" id="folders" style="display:none">
    <h3>选择要下载的收藏夹（打勾后点开始）</h3>
    <div id="folderList"></div>
    <div class="row-end">
      <button class="btn ghost small" onclick="checkAll(true)">全选</button>
      <button class="btn ghost small" onclick="checkAll(false)">全不选</button>
      <button class="btn ghost small" onclick="document.getElementById('folders').style.display='none'">收起</button>
      <button class="btn small" onclick="startCollects()">下载勾选的收藏夹</button>
    </div>
  </div>

  <div class="panel" id="job" style="display:none">
    <h3 id="jobTitle">正在下载</h3>
    <div class="progress-line"><span id="jobStep">…</span><span id="jobTime"></span></div>
    <div class="log" id="logBox"></div>
    <div class="row-end">
      <button class="btn ghost small" id="stopBtn" onclick="stopJob()">停止下载</button>
    </div>
  </div>

  <div class="links">
    <div class="link-card" onclick="togglePanel('settings')">⚙️ 下载规则设置</div>
    <div class="link-card" onclick="openAuthors()">🗑️ 删除下载记录</div>
    <div class="link-card" onclick="toggleCookieBox()">🔑 Cookie 设置</div>
  </div>

  <div class="panel" id="settings" style="display:none">
    <h3>下载规则设置（点开关即可，不用打字）</h3>
    <div class="row"><div>图片达到几张起单独建文件夹<small>图片够这个数就单独建文件夹、带背景音乐</small></div>
      <div class="stepper"><button onclick="stepThreshold(-1)">−</button><b id="thresholdVal">2 张</b><button onclick="stepThreshold(1)">＋</button></div></div>
    <div class="row"><div>实况作品单独建文件夹并下载背景音乐</div><div class="switch" data-key="live_folder_music" onclick="this.classList.toggle('on')"><i></i></div></div>
    <div class="row"><div>单个视频直接放在下载根目录</div><div class="switch" data-key="video_root_mode" onclick="this.classList.toggle('on')"><i></i></div></div>
    <div class="row"><div>文件夹名包含日期</div><div class="switch" data-key="folder_show_date" onclick="this.classList.toggle('on')"><i></i></div></div>
    <div class="row"><div>文件夹名包含类型（视频/图集/实况）</div><div class="switch" data-key="folder_show_type" onclick="this.classList.toggle('on')"><i></i></div></div>
    <div class="row"><div>文件夹名包含标题<small>打开后文件夹路径可能过长</small></div><div class="switch" data-key="folder_show_title" onclick="this.classList.toggle('on')"><i></i></div></div>
    <div class="row"><div>文件夹名包含作品 ID</div><div class="switch" data-key="folder_show_id" onclick="this.classList.toggle('on')"><i></i></div></div>
    <div class="row"><div>文件名包含日期</div><div class="switch" data-key="file_show_date" onclick="this.classList.toggle('on')"><i></i></div></div>
    <div class="row"><div>文件名包含类型</div><div class="switch" data-key="file_show_type" onclick="this.classList.toggle('on')"><i></i></div></div>
    <div class="row"><div>文件名包含作者</div><div class="switch" data-key="file_show_author" onclick="this.classList.toggle('on')"><i></i></div></div>
    <div class="row"><div>文件名包含标题</div><div class="switch" data-key="file_show_title" onclick="this.classList.toggle('on')"><i></i></div></div>
    <div class="row"><div>文件名包含作品 ID</div><div class="switch" data-key="file_show_id" onclick="this.classList.toggle('on')"><i></i></div></div>
    <div class="row-end"><button class="btn small" onclick="saveRules()">保存设置</button></div>
  </div>

  <div class="panel" id="authors" style="display:none">
    <h3>删除下载记录（删了之后，那些作品可以重新下载）</h3>
    <div class="searchbar">
      <input id="authorKeyword" placeholder="输作者名字的几个字筛选（可不输）">
      <button class="btn ghost small" onclick="pasteInto('authorKeyword')">粘贴</button>
      <button class="btn small" onclick="loadAuthors()">搜索</button>
    </div>
    <div id="authorList"></div>
    <div class="row-end">
      <button class="btn ghost small" onclick="clearAllRecords()">清空全部记录</button>
      <button class="btn ghost small" onclick="document.getElementById('authors').style.display='none'">收起</button>
    </div>
  </div>

  <p class="hint">下载好的文件在程序文件夹的 Download 里面，和终端下载的是同一个地方。</p>
</div>
<div class="toast" id="toast"></div>
<script>
  var threshold = 2;
  var jobTimer = null;

  function toast(msg) {
    var t = document.getElementById("toast");
    t.textContent = msg;
    t.classList.add("show");
    setTimeout(function () { t.classList.remove("show"); }, 2600);
  }
  function esc(s) {
    return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }
  async function api(path, options) {
    var res = await fetch(path, options || {});
    var data = {};
    try { data = await res.json(); } catch (e) {}
    if (!res.ok) { throw new Error(data.error || ("请求失败 " + res.status)); }
    return data;
  }
  function post(path, body) {
    return api(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body || {}) });
  }

  function toggleTheme() {
    var cur = document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
    document.documentElement.setAttribute("data-theme", cur);
    localStorage.setItem("dy-theme", cur);
    syncThemeBtn();
  }
  function syncThemeBtn() {
    var dark = document.documentElement.getAttribute("data-theme") === "dark";
    document.getElementById("themeBtn").textContent = dark ? "☀️ 浅色" : "🌙 深色";
  }

  async function pasteInto(id) {
    try {
      var text = await navigator.clipboard.readText();
      if (text) { document.getElementById(id).value = text; }
      else { toast("剪贴板是空的"); }
    } catch (e) {
      toast("浏览器不让自动粘贴，请在框里右键选择粘贴");
      document.getElementById(id).focus();
    }
  }

  async function loadState() {
    try {
      var s = await api("/api/state");
      var pill = document.getElementById("cookieStatus");
      pill.textContent = s.cookie_set ? "● Cookie 已设置，可以下载" : "● Cookie 还没设置";
      pill.classList.toggle("bad", !s.cookie_set);
      document.getElementById("cookieBox").style.display = s.cookie_set ? "none" : "block";
      document.getElementById("ownerBox").style.display = (!s.owner_set && s.cookie_set) ? "block" : "none";
    } catch (e) {}
  }

  function toggleCookieBox() {
    var box = document.getElementById("cookieBox");
    box.style.display = box.style.display === "none" ? "block" : "none";
    if (box.style.display === "block") box.scrollIntoView({ behavior: "smooth" });
  }
  async function saveCookie() {
    try {
      await post("/api/cookie", { cookie: document.getElementById("cookieText").value });
      toast("Cookie 已保存");
      document.getElementById("cookieText").value = "";
      loadState();
    } catch (e) { toast(e.message); }
  }
  async function saveOwner() {
    try {
      await post("/api/owner_url", { url: document.getElementById("ownerText").value });
      toast("主页链接已保存，下次不用再填");
      loadState();
    } catch (e) { toast(e.message); }
  }

  async function startJob(path) {
    try {
      await post(path);
      showJob();
    } catch (e) { toast(e.message); }
  }
  async function loadCollects() {
    try {
      toast("正在拉取你的收藏夹列表…");
      var data = await api("/api/collects");
      var list = document.getElementById("folderList");
      if (!data.items.length) { list.innerHTML = "<p style='color:var(--muted)'>没有拉到收藏夹（可能还没有收藏夹，或 Cookie 已失效）</p>"; }
      else {
        list.innerHTML = data.items.map(function (i) {
          return "<label class='folder-row'><input type='checkbox' checked value='" + esc(i.id) + "'> " + esc(i.name) + "</label>";
        }).join("");
      }
      var f = document.getElementById("folders");
      f.style.display = "block";
      f.scrollIntoView({ behavior: "smooth" });
    } catch (e) { toast(e.message); }
  }
  function checkAll(on) {
    document.querySelectorAll("#folderList input[type=checkbox]").forEach(function (c) { c.checked = on; });
  }
  async function startCollects() {
    var ids = Array.from(document.querySelectorAll("#folderList input[type=checkbox]:checked")).map(function (c) { return c.value; });
    if (!ids.length) { toast("先勾选至少一个收藏夹"); return; }
    try {
      await post("/api/job/collects", { ids: ids });
      showJob();
    } catch (e) { toast(e.message); }
  }

  function showJob() {
    var j = document.getElementById("job");
    j.style.display = "block";
    j.scrollIntoView({ behavior: "smooth", block: "center" });
    if (jobTimer) clearInterval(jobTimer);
    pollJob();
    jobTimer = setInterval(pollJob, 1500);
  }
  async function pollJob() {
    try {
      var s = await api("/api/job");
      if (!s.title && !s.running) return;
      document.getElementById("jobTitle").textContent = (s.running ? "正在下载：" : "下载任务：") + s.title;
      document.getElementById("jobStep").textContent = s.error ? ("出错：" + s.error) : s.step;
      document.getElementById("jobTime").textContent = s.started_at ? ("已用 " + Math.floor(s.elapsed / 60) + " 分 " + (s.elapsed % 60) + " 秒") : "";
      var box = document.getElementById("logBox");
      box.textContent = s.logs.join("\n");
      box.scrollTop = box.scrollHeight;
      document.getElementById("stopBtn").disabled = !s.running;
      if (!s.running && jobTimer) { clearInterval(jobTimer); jobTimer = null; loadState(); }
    } catch (e) {}
  }
  async function stopJob() {
    try { await post("/api/job/stop"); toast("正在停止…"); } catch (e) { toast(e.message); }
  }

  function togglePanel(id) {
    var p = document.getElementById(id);
    var show = p.style.display === "none";
    p.style.display = show ? "block" : "none";
    if (show) { loadRules(); p.scrollIntoView({ behavior: "smooth" }); }
  }
  function stepThreshold(d) {
    threshold = Math.min(99, Math.max(1, threshold + d));
    document.getElementById("thresholdVal").textContent = threshold + " 张";
  }
  async function loadRules() {
    try {
      var r = await api("/api/rules");
      threshold = r.album_threshold || 2;
      document.getElementById("thresholdVal").textContent = threshold + " 张";
      document.querySelectorAll("#settings .switch").forEach(function (sw) {
        sw.classList.toggle("on", !!r[sw.getAttribute("data-key")]);
      });
    } catch (e) { toast(e.message); }
  }
  async function saveRules() {
    var body = { album_threshold: threshold };
    document.querySelectorAll("#settings .switch").forEach(function (sw) {
      body[sw.getAttribute("data-key")] = sw.classList.contains("on");
    });
    try { await post("/api/rules", body); toast("设置已保存，立即生效"); }
    catch (e) { toast(e.message); }
  }

  function openAuthors() {
    var p = document.getElementById("authors");
    p.style.display = "block";
    p.scrollIntoView({ behavior: "smooth" });
    loadAuthors();
  }
  async function loadAuthors() {
    try {
      var kw = document.getElementById("authorKeyword").value.trim();
      var data = await api("/api/authors?keyword=" + encodeURIComponent(kw));
      var list = document.getElementById("authorList");
      if (!data.items.length) { list.innerHTML = "<p style='color:var(--muted);padding:8px 2px'>没有符合的记录</p>"; return; }
      list.innerHTML = "";
      data.items.forEach(function (r) {
        var row = document.createElement("div");
        row.className = "author-row";
        var left = document.createElement("span");
        left.innerHTML = esc(r.name || "(无昵称)") + " <span class='meta'>" + esc(r.source) + " · " + r.count + " 部</span>";
        var btn = document.createElement("button");
        btn.className = "btn ghost small";
        btn.textContent = "删除记录";
        btn.onclick = async function () {
          if (!confirm("删除「" + (r.name || "") + "」在" + r.source + "的 " + r.count + " 条下载记录？删了之后这些作品可以重新下载。")) return;
          try {
            var res = await post("/api/authors/delete", { uid: r.uid, source: r.source });
            toast("已删除 " + res.deleted + " 条记录");
            loadAuthors();
          } catch (e) { toast(e.message); }
        };
        row.appendChild(left); row.appendChild(btn);
        list.appendChild(row);
      });
    } catch (e) { toast(e.message); }
  }
  async function clearAllRecords() {
    if (!confirm("把全部下载记录都清空？清空后所有作品都会重新下载。")) return;
    try { await post("/api/records/clear"); toast("已清空全部记录"); loadAuthors(); }
    catch (e) { toast(e.message); }
  }

  async function shutdown() {
    if (!confirm("关闭 Web UI？正在进行的下载会被停止。")) return;
    try { await post("/api/shutdown"); } catch (e) {}
    document.body.innerHTML = "<p style='text-align:center;margin-top:80px;font-size:18px'>Web UI 已关闭，可以关掉这个网页了。<br>回到程序主菜单还能再次打开。</p>";
  }

  syncThemeBtn();
  loadState();
</script>
</body>
</html>
"""
