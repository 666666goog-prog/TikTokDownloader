# -*- coding: utf-8 -*-
"""图形界面模式（收藏版）：程序自己的窗口，全程鼠标操作。

收藏版双击打开后直接进入本窗口（不再先过终端菜单，背后的
命令行窗口会被藏起来），窗口里的按钮调用与终端模式完全相同
的下载引擎（TikTok 类），下载规则、下载记录共用同一套配置
与数据库。关闭窗口即退出程序。

窗口事件与程序的主事件循环协同运行（定时处理窗口事件），因此
日志、拉取进度都能实时刷新。
"""

import asyncio
from collections import deque
from time import localtime, strftime, time

from ..interface import API, Collection
from ..translation import _
from . import browser_login
from .main_terminal import TikTok

__all__ = ["GuiCore", "run_gui"]

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
    """日志代理：把引擎写日志的同一份文本同步到窗口日志区，
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

    @staticmethod
    def _visible(args, kwargs) -> bool:
        """引擎日志的 output 参数：False 表示只写日志文件、不给用户看
        （网址、请求头这类调试内容都是这么标记的），窗口里同样不显示。"""
        if args:
            return bool(args[0])
        return bool(kwargs.get("output", True))

    def info(self, text, *args, **kwargs):
        if self._visible(args, kwargs):
            self._push(text)
        return self._wrapped.info(text, *args, **kwargs)

    def warning(self, text, *args, **kwargs):
        if self._visible(args, kwargs):
            self._push(text)
        return self._wrapped.warning(text, *args, **kwargs)

    def error(self, text, *args, **kwargs):
        if self._visible(args, kwargs):
            self._push(text)
        return self._wrapped.error(text, *args, **kwargs)

    def print(self, text, *args, **kwargs):
        self._push(text)
        return self._wrapped.print(text, *args, **kwargs)


class GuiCore:
    """图形界面的业务核心：持有下载引擎，处理窗口的全部操作。

    不涉及任何窗口控件，可独立测试。所有方法都在程序主事件
    循环里调用，与终端模式共用同一个循环，引擎、数据库、
    网络客户端的归属与终端模式完全一致。
    """

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
        # 扫码登录的状态
        self.login = {"state": "idle", "message": ""}
        self._login_window: browser_login.BrowserLogin | None = None

    # ---------- 引擎管理 ----------

    def build_engine(self) -> None:
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

    async def refresh_engine(self) -> None:
        """配置变更后与终端一致：重读配置并重建引擎，立即生效。"""
        await self.app.check_settings()
        self.build_engine()

    def release(self) -> None:
        """窗口关闭时收尾：停掉进行中的任务，摘除进度钩子。"""
        if self._job_task and not self._job_task.done():
            self._job_task.cancel()
        if self._login_window is not None:
            self._login_window.close()
            self._login_window = None
        API._web_progress_hook = None

    def _on_progress(self, text, count, page) -> None:
        if self.job["running"]:
            self.job["step"] = _(
                "正在拉取{text}列表（已获取 {count} 条，第 {page} 页）…"
            ).format(text=text, count=count, page=page)

    # ---------- 状态 ----------

    def status(self) -> dict:
        parameter = self.app.parameter
        return {
            "cookie_state": bool(parameter.cookie_state),
            "root": str(parameter.root),
            "owner_url": str(getattr(self.engine.owner, "url", "") or "")
            if self.engine is not None
            else "",
        }

    # ---------- 下载任务 ----------

    def _begin_job(self, title: str, factory) -> tuple[bool, str]:
        if self.job["running"]:
            return False, _("已有任务正在进行，等它完成或先停止它")
        self.logs.clear()
        self.job = {
            "running": True,
            "title": title,
            "step": _("正在准备…"),
            "started_at": time(),
            "error": "",
        }
        self._job_task = asyncio.ensure_future(self._run_job(factory))
        self._job_task.add_done_callback(self._on_job_done)
        return True, ""

    def start_job(self, kind: str) -> tuple[bool, str]:
        """kind: collection=批量下载收藏作品，music=批量下载收藏音乐。"""
        if self.job["running"]:
            return False, _("已有任务正在进行，等它完成或先停止它")
        if not self.app.parameter.cookie_state:
            return False, _("Cookie 还没设置，先到「设置」页把 Cookie 粘贴保存")
        factory = {
            "collection": self._job_collection,
            "music": self._job_music,
        }.get(kind)
        if factory is None:
            return False, _("不认识的任务类型")
        title = {
            "collection": _("批量下载收藏作品"),
            "music": _("批量下载收藏音乐"),
        }[kind]
        return self._begin_job(title, factory)

    def _on_job_done(self, task: asyncio.Task) -> None:
        # 兜底：任务在真正开始运行前就被取消时，_run_job 内部的
        # 收尾代码不会执行，这里把状态补齐，避免界面卡在“进行中”
        if self.job["running"]:
            self.job["running"] = False
            if task.cancelled():
                self.job["step"] = _("已停止")

    def stop_job(self) -> None:
        if self._job_task and not self._job_task.done():
            self._job_task.cancel()

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
        except Exception as error:  # noqa: BLE001 - 窗口需要看到失败原因
            self.job["error"] = str(error)
            self.job["step"] = _("出错了")
            self.logs.append((_now(), _("出错：{error}").format(error=error)))
        finally:
            self.job["running"] = False

    async def _job_collection(self) -> None:
        sec_user_id = await self.engine.check_sec_user_id(self.engine.owner.url)
        if not sec_user_id:
            raise RuntimeError(
                _("主页链接取不到账号信息，请到「设置」页重新粘贴你自己的抖音主页链接")
            )
        await self.engine._deal_collection_data(sec_user_id)

    async def _job_music(self) -> None:
        await self.engine.collection_music_interactive()

    def job_state(self) -> dict:
        state = dict(self.job)
        state["elapsed"] = (
            int(time() - state["started_at"]) if state["started_at"] else 0
        )
        state["logs"] = [f"[{t}] {s}" for t, s in self.logs]
        return state

    # ---------- 下载规则 ----------

    def get_rules(self) -> dict:
        data = self.app.settings.read()
        return {k: data.get(k, v) for k, v in RULE_DEFAULTS.items()}

    async def save_rules(self, values: dict) -> tuple[bool, str]:
        try:
            threshold = int(values.get("album_threshold", 2))
            if not 1 <= threshold <= 999:
                raise ValueError
        except (TypeError, ValueError):
            return False, _("图片张数不正确")
        data = self.app.settings.read()
        data["album_threshold"] = threshold
        for key in RULE_BOOL_KEYS:
            data[key] = bool(values.get(key, RULE_DEFAULTS[key]))
        self.app.settings.update(data)
        await self.refresh_engine()
        return True, ""

    # ---------- 下载记录 ----------

    async def list_authors(self, keyword: str = "") -> list[dict]:
        """作者名单统一整理成字典列表。

        数据库返回的是（UID，昵称，来源，作品数）元组行，不是字典；
        来源为空字符串的是早期记录，显示为“早期记录”，但删除时
        必须传回原始的空字符串，所以 raw 与 label 分开保存。
        """
        raw_rows = await self.app.recorder.list_authors()
        rows = []
        for r in raw_rows:
            if isinstance(r, dict):
                uid = r.get("uid")
                name = r.get("name")
                source = r.get("source") or ""
                count = r.get("count") or 0
            else:
                uid, name, source, count = r[0], r[1], r[2] or "", r[3]
            rows.append(
                {
                    "uid": str(uid or ""),
                    "name": str(name or ""),
                    "source": str(source),
                    "source_label": str(source) or _("早期记录"),
                    "count": int(count or 0),
                }
            )
        if keyword:
            rows = [r for r in rows if keyword in r["name"]]
        rows.sort(
            key=lambda r: (SOURCE_ORDER.get(r["source_label"], 9), -r["count"])
        )
        return rows

    async def delete_author(self, uid: str, source: str) -> int:
        return await self.app.recorder.delete_by_author(uid, source)

    async def clear_records(self) -> None:
        await self.app.recorder.delete_ids("ALL")

    # ---------- 设置 ----------

    async def save_cookie(self, cookie: str) -> tuple[bool, str]:
        """保存 Cookie 并校验：能读到自己的收藏作品，才算登录是好的。"""
        cookie = (cookie or "").strip()
        if not cookie:
            return False, _("内容是空的")
        if not self.app.cookie.run(cookie, tiktok=False):
            return False, _("这段内容不像有效的 Cookie，请重新复制")
        await self.refresh_engine()
        if await self._verify_cookie():
            return True, _("Cookie 已保存，校验通过（能读到你的收藏，登录是好的）")
        return True, _(
            "Cookie 已保存。提醒一句：校验时没读到你的收藏作品，"
            "要是你收藏里本来有东西，这个 Cookie 可能没登录或已过期"
        )

    async def _verify_cookie(self) -> bool:
        """用刚保存的 Cookie 试着读一页自己的收藏作品。

        收藏列表只有登录本人能读，所以读得到就证明登录有效；
        读不到也可能是收藏本来就空，调用方按“提醒”处理。
        """
        try:
            owner_url = str(getattr(self.engine.owner, "url", "") or "")
            if not owner_url:
                return False
            sec_user_id = await self.engine.check_sec_user_id(owner_url)
            if not sec_user_id:
                return False
            probe = Collection(
                self.app.parameter,
                sec_user_id=sec_user_id,
                pages=1,
            )
            await probe.run(single_page=True)
            return bool(probe.response)
        except Exception:  # noqa: BLE001 - 校验失败不挡保存
            return False

    # ---------- 扫码登录 ----------

    async def start_browser_login(self) -> None:
        """打开专属浏览器窗口等用户扫码登录，成功后自动保存 Cookie。"""
        if self.login["state"] in ("starting", "waiting"):
            return
        exe = browser_login.find_browser()
        if not exe:
            self.login = {
                "state": "error",
                "message": _("没在这台电脑上找到 Edge 或 Chrome 浏览器"),
            }
            return
        profile = self.app.settings.path.parent / "browser_profile"
        window = browser_login.BrowserLogin(profile, browser_path=exe)
        self._login_window = window
        self.login = {"state": "starting", "message": _("正在打开登录窗口…")}
        if not await asyncio.to_thread(window.start):
            self.login = {
                "state": "error",
                "message": _("登录窗口没能打开，请再试一次"),
            }
            return
        self.login = {
            "state": "waiting",
            "message": _(
                "登录窗口已打开：在里面扫码登录抖音，登录好这边会自动保存，不用你再管"
            ),
        }
        deadline = time() + 300
        while time() < deadline:
            if self.login["state"] != "waiting":
                return  # 被取消了，收尾在 cancel 里做
            if not window.is_running():
                self.login = {
                    "state": "error",
                    "message": _("登录窗口被关掉了，没拿到登录信息"),
                }
                return
            cookie = await asyncio.to_thread(window.poll_cookie)
            if self.login["state"] != "waiting":
                return
            if cookie:
                ok, message = await self.save_cookie(cookie)
                window.close()
                self.login = {
                    "state": "done" if ok else "error",
                    "message": _("登录成功，Cookie 已自动保存")
                    if ok
                    else _("读到了登录信息，但保存没成功：{error}").format(
                        error=message
                    ),
                }
                return
            await asyncio.sleep(2)
        window.close()
        self.login = {
            "state": "error",
            "message": _("等了 5 分钟还没等到登录，这次先算了"),
        }

    def cancel_browser_login(self) -> None:
        if self._login_window is not None:
            self._login_window.close()
            self._login_window = None
        if self.login["state"] in ("starting", "waiting"):
            self.login = {"state": "idle", "message": _("已取消登录")}

    async def save_owner_url(self, url: str) -> tuple[bool, str]:
        url = (url or "").strip()
        if not url:
            return False, _("内容是空的")
        if not await self.engine.check_sec_user_id(url):
            return False, _("这个链接取不到账号信息，请确认是自己的抖音主页链接")
        data = self.app.settings.read()
        owner = dict(data.get("owner_url") or {})
        owner["url"] = url
        data["owner_url"] = owner
        self.app.settings.update(data)
        self.engine.owner.url = url
        return True, ""

    # ---------- 界面主题 ----------

    def get_theme_pref(self) -> str:
        """返回 "dark" / "light" / ""（未设置，跟随系统）。"""
        try:
            return str(self.app.settings.read().get("gui_theme") or "")
        except Exception:
            return ""

    def set_theme_pref(self, value: str) -> None:
        try:
            data = self.app.settings.read()
            data["gui_theme"] = value
            self.app.settings.update(data)
        except Exception:
            pass


def _system_prefers_dark() -> bool:
    """Windows 系统是否处于深色模式（读不到时按浅色处理）。"""
    try:
        import winreg

        with winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            r"Software\Microsoft\Windows\CurrentVersion\Themes\Personalize",
        ) as key:
            value, _ = winreg.QueryValueEx(key, "AppsUseLightTheme")
            return value == 0
    except Exception:
        return False


def _set_console_visible(visible: bool) -> None:
    """Windows 下隐藏/恢复程序背后的命令行黑窗口（收藏版双击打开
    直接进窗口，黑窗口没必要露在后面）。非 Windows 或失败时什么
    都不做，不影响窗口本身。"""
    try:
        import ctypes

        hwnd = ctypes.windll.kernel32.GetConsoleWindow()
        if hwnd:
            ctypes.windll.user32.ShowWindow(hwnd, 5 if visible else 0)
    except Exception:
        pass


def _set_titlebar_dark(root, dark: bool) -> None:
    """深色模式时把 Windows 窗口标题栏也调成深色，别上面一条白。

    用系统 DWM 接口（沉浸式深色模式开关）；老版本 Windows 的
    属性号不同，两个都试一遍，失败就静悄悄算了。
    """
    try:
        import ctypes
        import sys

        if sys.platform != "win32":
            return
        hwnd = ctypes.windll.user32.GetParent(root.winfo_id()) or root.winfo_id()
        value = ctypes.c_int(1 if dark else 0)
        for attribute in (20, 19):
            result = ctypes.windll.dwmapi.DwmSetWindowAttribute(
                hwnd, attribute, ctypes.byref(value), ctypes.sizeof(value)
            )
            if result == 0:
                break
    except Exception:
        pass


# ---------- 窗口层（tkinter 在此惰性导入，导入本模块不需要显示器） ----------

_PALETTES = {
    "light": {
        "bg": "#F5F6F8",
        "panel": "#FFFFFF",
        "sidebar": "#EDF0F4",
        "fg": "#1F2329",
        "sub": "#8A919C",
        "entry": "#FFFFFF",
        "border": "#DFE3E8",
        "accent": "#2F7CF6",
        "accent_active": "#1F6AE0",
        "accent_soft": "#E3EEFE",
        "info_bg": "#EEF4FF",
        "danger": "#E54545",
    },
    "dark": {
        "bg": "#191919",
        "panel": "#232323",
        "sidebar": "#1E1E1E",
        "fg": "#EDEDED",
        "sub": "#9CA3AF",
        "entry": "#2C2C2C",
        "border": "#3A3A3A",
        "accent": "#2F7CF6",
        "accent_active": "#4A8DFF",
        "accent_soft": "#26364D",
        "info_bg": "#22314A",
        "danger": "#F0524F",
    },
}

# 下载规则开关的分组与显示文字（正常用词，开关即开/关）
_RULE_GROUPS = (
    (
        "存放方式",
        (
            ("live_folder_music", "实况照片：单独建文件夹，并下载背景音乐"),
            ("video_root_mode", "纯视频：直接放在下载目录里，不建文件夹"),
        ),
    ),
    (
        "文件夹名字里包含",
        (
            ("folder_show_date", "日期"),
            ("folder_show_type", "类型（视频/图集/实况）"),
            ("folder_show_title", "标题（标题太长时路径可能超限）"),
            ("folder_show_id", "作品ID"),
        ),
    ),
    (
        "文件名字里包含",
        (
            ("file_show_date", "日期"),
            ("file_show_type", "类型（视频/图集/实况）"),
            ("file_show_author", "作者"),
            ("file_show_title", "标题"),
            ("file_show_id", "作品ID"),
        ),
    ),
)


async def run_gui(app) -> None:
    """打开图形界面窗口，直到用户关闭窗口才返回。"""
    import tkinter as tk
    from tkinter import messagebox, ttk

    core = GuiCore(app)
    core.build_engine()
    console = app.console
    console.print()
    console.print(_("图形界面已打开，在弹出的程序窗口里用鼠标操作即可。"))
    console.print(_("关闭那个窗口，程序就会退出。"))

    def spawn(coro, on_error) -> None:
        """启动一个界面协程：出错时把原因写到对应的提示栏，
        不让它变成控制台里一条没人看的“未处理的异常”。"""

        async def _guarded():
            try:
                await coro
            except Exception as error:  # noqa: BLE001 - 提示给用户看
                on_error(error)

        asyncio.ensure_future(_guarded())

    pref = core.get_theme_pref()
    dark = pref == "dark" or (pref == "" and _system_prefers_dark())

    # 窗口才是正主：把背后的命令行黑窗口藏起来（建窗失败时恢复，
    # 让报错能显示出来）
    _set_console_visible(False)
    try:
        root = tk.Tk()
    except Exception:
        _set_console_visible(True)
        raise
    root.title(_("DouK-Downloader 收藏版"))
    root.geometry("980x720")
    root.minsize(860, 600)

    style = ttk.Style(root)
    raw_widgets: list = []  # 需要手工上色的 tk 控件（文本框、侧栏等）
    nav_buttons: dict[str, tk.Button] = {}
    pages: dict[str, ttk.Frame] = {}
    current_page = tk.StringVar(value="download")
    login_dlg: dict = {}  # 登录弹窗的控件引用（没打开时为空）
    # 设置页上的三行状态（登录/外观/下载目录都归设置页管）
    theme_text_var = tk.StringVar()
    set_login_var = tk.StringVar()
    set_root_var = tk.StringVar()

    def pal() -> dict:
        return _PALETTES["dark" if dark else "light"]

    def apply_theme() -> None:
        p = pal()
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        style.configure("TFrame", background=p["bg"])
        style.configure("Panel.TFrame", background=p["panel"])
        style.configure(
            "TLabel",
            background=p["bg"],
            foreground=p["fg"],
            font=("Microsoft YaHei UI", 10),
        )
        style.configure(
            "Panel.TLabel",
            background=p["panel"],
            foreground=p["fg"],
            font=("Microsoft YaHei UI", 10),
        )
        style.configure(
            "Sub.TLabel",
            background=p["bg"],
            foreground=p["sub"],
            font=("Microsoft YaHei UI", 9),
        )
        style.configure(
            "Title.TLabel",
            background=p["bg"],
            foreground=p["fg"],
            font=("Microsoft YaHei UI", 13, "bold"),
        )
        style.configure(
            "Section.TLabel",
            background=p["bg"],
            foreground=p["fg"],
            font=("Microsoft YaHei UI", 11, "bold"),
        )
        style.configure(
            "TButton",
            background=p["panel"],
            foreground=p["fg"],
            bordercolor=p["border"],
            font=("Microsoft YaHei UI", 10),
            padding=(14, 7),
        )
        style.map("TButton", background=[("active", p["bg"])])
        style.configure(
            "Accent.TButton",
            background=p["accent"],
            foreground="#FFFFFF",
            bordercolor=p["accent"],
            font=("Microsoft YaHei UI", 11, "bold"),
            padding=(18, 10),
        )
        style.map(
            "Accent.TButton",
            background=[
                ("active", p["accent_active"]),
                ("disabled", p["border"]),
            ],
        )
        style.configure(
            "Danger.TButton",
            foreground=p["danger"],
            font=("Microsoft YaHei UI", 10),
            padding=(14, 7),
        )
        style.configure(
            "TCheckbutton",
            background=p["bg"],
            foreground=p["fg"],
            font=("Microsoft YaHei UI", 10),
        )
        style.configure("TLabelframe", background=p["bg"], bordercolor=p["border"])
        style.configure(
            "TLabelframe.Label",
            background=p["bg"],
            foreground=p["fg"],
            font=("Microsoft YaHei UI", 10, "bold"),
        )
        style.configure("TEntry", fieldbackground=p["entry"], foreground=p["fg"])
        style.configure("TSpinbox", fieldbackground=p["entry"], foreground=p["fg"])
        style.configure(
            "Treeview",
            background=p["panel"],
            fieldbackground=p["panel"],
            foreground=p["fg"],
            rowheight=28,
            font=("Microsoft YaHei UI", 10),
        )
        style.configure(
            "Treeview.Heading",
            background=p["bg"],
            foreground=p["fg"],
            font=("Microsoft YaHei UI", 10, "bold"),
        )
        root.configure(background=p["bg"])
        sidebar.configure(background=p["sidebar"])
        for widget in raw_widgets:
            widget.configure(
                background=p["panel"],
                foreground=p["fg"],
                insertbackground=p["fg"],
                selectbackground=p["accent"],
                highlightbackground=p["border"],
            )
        paint_nav()
        for widget in side_frames:
            widget.configure(background=p["sidebar"])
        for widget, is_sub in side_labels:
            widget.configure(
                background=p["sidebar"],
                foreground=p["sub"] if is_sub else p["fg"],
            )
        theme_text_var.set(_("☀️ 切换到浅色") if dark else _("🌙 切换到深色"))
        _set_titlebar_dark(root, dark)
        if login_dlg:
            paint_login_dialog()

    def paint_nav(active: str | None = None) -> None:
        p = pal()
        active = active or current_page.get()
        for name, button in nav_buttons.items():
            if name == active:
                button.configure(
                    background=p["accent_soft"],
                    foreground=p["accent"],
                    font=("Microsoft YaHei UI", 11, "bold"),
                )
            else:
                button.configure(
                    background=p["sidebar"],
                    foreground=p["fg"],
                    font=("Microsoft YaHei UI", 11),
                )

    def show_page(name: str) -> None:
        current_page.set(name)
        for key, frame in pages.items():
            if key == name:
                frame.pack(fill="both", expand=True)
            else:
                frame.pack_forget()
        paint_nav(name)

    # ---------- 左侧导航栏 ----------
    side_frames: list = []
    side_labels: list = []
    sidebar = tk.Frame(root, width=176)
    side_frames.append(sidebar)
    sidebar.pack(side="left", fill="y")
    sidebar.pack_propagate(False)
    side_title = tk.Label(
        sidebar,
        text=_("DouK-Downloader"),
        font=("Microsoft YaHei UI", 12, "bold"),
        anchor="w",
    )
    side_title.pack(fill="x", padx=16, pady=(18, 0))
    side_labels.append((side_title, False))
    side_sub = tk.Label(sidebar, text=_("收藏版"), anchor="w")
    side_sub.pack(fill="x", padx=16, pady=(0, 14))
    side_labels.append((side_sub, True))

    for key, label in (
        ("download", _("⬇  下载")),
        ("rules", _("⚙  下载规则")),
        ("records", _("🗂  下载记录")),
        ("settings", _("🔧  设置")),
    ):
        button = tk.Button(
            sidebar,
            text=label,
            anchor="w",
            relief="flat",
            bd=0,
            padx=16,
            pady=9,
            cursor="hand2",
            activebackground=pal()["accent_soft"],
            command=lambda k=key: show_page(k),
        )
        button.pack(fill="x", padx=8, pady=2)
        nav_buttons[key] = button

    # ---------- 右侧内容区 ----------
    content = ttk.Frame(root)
    content.pack(side="left", fill="both", expand=True)

    def refresh_status() -> None:
        # 登录状态和下载目录都显示在「设置」页里
        info = core.status()
        set_login_var.set(
            _("Cookie：已设置") if info["cookie_state"] else _("Cookie：未设置")
        )
        set_root_var.set(str(info["root"]))

    def make_page(key: str) -> ttk.Frame:
        frame = ttk.Frame(content)
        pages[key] = frame
        return frame


    # ---------- 页 1：下载 ----------
    page_job = make_page("download")
    ttk.Label(page_job, text=_("下载我的收藏"), style="Title.TLabel").pack(
        anchor="w", padx=18, pady=(10, 2)
    )
    ttk.Label(
        page_job,
        text=_("下载你自己收藏的作品和收藏的音乐，点一下就开始，不用别的操作。"),
        style="Sub.TLabel",
    ).pack(anchor="w", padx=18)
    buttons = ttk.Frame(page_job)
    buttons.pack(fill="x", padx=18, pady=14)
    btn_collection = ttk.Button(
        buttons, text=_("批量下载收藏作品"), style="Accent.TButton"
    )
    btn_collection.pack(side="left", padx=(0, 10))
    btn_music = ttk.Button(buttons, text=_("批量下载收藏音乐"), style="Accent.TButton")
    btn_music.pack(side="left")
    step_var = tk.StringVar(value=_("点上面的按钮开始下载"))
    ttk.Label(page_job, textvariable=step_var).pack(anchor="w", padx=18)
    log_text = tk.Text(
        page_job,
        height=16,
        wrap="word",
        font=("Microsoft YaHei UI", 10),
        spacing1=3,
        spacing3=3,
        padx=12,
        pady=10,
        relief="flat",
        bd=0,
        highlightthickness=1,
    )
    raw_widgets.append(log_text)
    log_text.pack(fill="both", expand=True, padx=18, pady=10)
    log_text.configure(state="disabled")
    job_row = ttk.Frame(page_job)
    job_row.pack(fill="x", padx=18, pady=(0, 12))
    btn_stop = ttk.Button(job_row, text=_("停止"), style="Danger.TButton")
    btn_stop.pack(side="right")
    ttk.Label(
        job_row,
        text=_("停止不会白下：下好的文件下次自动跳过"),
        style="Sub.TLabel",
    ).pack(side="left")

    def start(kind: str) -> None:
        ok, error = core.start_job(kind)
        if not ok:
            messagebox.showwarning(_("暂时不能开始"), error, parent=root)

    btn_collection.configure(command=lambda: start("collection"))
    btn_music.configure(command=lambda: start("music"))
    btn_stop.configure(command=core.stop_job)

    # ---------- 页 2：下载规则 ----------
    page_rules = make_page("rules")
    ttk.Label(page_rules, text=_("下载规则"), style="Title.TLabel").pack(
        anchor="w", padx=18, pady=(10, 6)
    )
    rules = core.get_rules()
    threshold_var = tk.IntVar(value=int(rules["album_threshold"]))
    row_top = ttk.Frame(page_rules)
    row_top.pack(fill="x", padx=18, pady=8)
    ttk.Label(row_top, text=_("图集有几张图片时，单独建一个文件夹（并下载背景音乐）：")).pack(side="left")
    ttk.Spinbox(
        row_top, from_=1, to=999, width=6, textvariable=threshold_var
    ).pack(side="left", padx=6)
    rule_vars: dict[str, tk.BooleanVar] = {}
    for group_title, items in _RULE_GROUPS:
        frame = ttk.Labelframe(page_rules, text=_(group_title))
        frame.pack(fill="x", padx=18, pady=6)
        for key, label in items:
            var = tk.BooleanVar(value=bool(rules[key]))
            rule_vars[key] = var
            ttk.Checkbutton(frame, text=_(label), variable=var).pack(
                anchor="w", padx=10, pady=3
            )
    rules_feedback = tk.StringVar()
    rules_row = ttk.Frame(page_rules)
    rules_row.pack(fill="x", padx=18, pady=10)
    ttk.Label(rules_row, textvariable=rules_feedback, style="Sub.TLabel").pack(side="left")

    def save_rules() -> None:
        values = {k: v.get() for k, v in rule_vars.items()}
        values["album_threshold"] = threshold_var.get()

        async def _save():
            ok, error = await core.save_rules(values)
            rules_feedback.set(_("已保存，马上生效") if ok else error)

        spawn(_save(), lambda e: rules_feedback.set(_("保存失败：{error}").format(error=e)))

    ttk.Button(rules_row, text=_("保存规则"), command=save_rules).pack(side="right")

    # ---------- 页 3：下载记录 ----------
    page_records = make_page("records")
    ttk.Label(page_records, text=_("下载记录"), style="Title.TLabel").pack(
        anchor="w", padx=18, pady=(10, 6)
    )
    search_row = ttk.Frame(page_records)
    search_row.pack(fill="x", padx=18, pady=8)
    ttk.Label(search_row, text=_("按作者名查找：")).pack(side="left")
    search_var = tk.StringVar()
    ttk.Entry(search_row, textvariable=search_var, width=24).pack(side="left", padx=6)
    tree = ttk.Treeview(
        page_records, columns=("name", "source", "count"), show="headings", height=13
    )
    tree.heading("name", text=_("作者"))
    tree.heading("source", text=_("下载来源"))
    tree.heading("count", text=_("作品数"))
    tree.column("name", width=340)
    tree.column("source", width=140, anchor="center")
    tree.column("count", width=90, anchor="center")
    tree.pack(fill="both", expand=True, padx=18)
    author_rows: list[dict] = []
    records_feedback = tk.StringVar()
    rec_row = ttk.Frame(page_records)
    rec_row.pack(fill="x", padx=18, pady=10)
    ttk.Label(rec_row, textvariable=records_feedback, style="Sub.TLabel").pack(side="left")
    btn_clear = ttk.Button(rec_row, text=_("清空全部记录"), style="Danger.TButton")
    btn_clear.pack(side="right", padx=(6, 0))
    btn_delete = ttk.Button(rec_row, text=_("删除选中作者的记录"))
    btn_delete.pack(side="right")

    def fill_authors(rows: list[dict]) -> None:
        author_rows.clear()
        author_rows.extend(rows)
        tree.delete(*tree.get_children())
        for index, row in enumerate(rows):
            tree.insert(
                "",
                "end",
                iid=str(index),
                values=(row.get("name") or "", row.get("source_label") or "", row.get("count") or 0),
            )
        records_feedback.set(_("共 {count} 条").format(count=len(rows)))

    def refresh_authors() -> None:
        async def _load():
            fill_authors(await core.list_authors(search_var.get().strip()))

        spawn(_load(), lambda e: records_feedback.set(_("读取记录失败：{error}").format(error=e)))

    ttk.Button(search_row, text=_("查找"), command=refresh_authors).pack(side="left")
    ttk.Button(search_row, text=_("显示全部"), command=lambda: (search_var.set(""), refresh_authors())).pack(
        side="left", padx=6
    )

    def delete_selected() -> None:
        selection = tree.selection()
        if not selection:
            records_feedback.set(_("先在列表里点一下要删除的作者"))
            return
        row = author_rows[int(selection[0])]
        if not messagebox.askyesno(
            _("删除下载记录"),
            _(
                "确定删除「{name}」（{source}）的 {count} 条下载记录吗？\n"
                "删掉后，这些作品下次会被重新下载。"
            ).format(name=row.get("name") or "", source=row.get("source_label") or "", count=row.get("count") or 0),
            parent=root,
        ):
            return

        async def _delete():
            await core.delete_author(str(row.get("uid") or ""), str(row.get("source") or ""))
            fill_authors(await core.list_authors(search_var.get().strip()))

        spawn(_delete(), lambda e: records_feedback.set(_("删除失败：{error}").format(error=e)))

    def clear_all() -> None:
        if not messagebox.askyesno(
            _("清空全部记录"),
            _("确定清空全部下载记录吗？\n清空后，所有下过的作品下次都会被重新下载。"),
            parent=root,
        ):
            return

        async def _clear():
            await core.clear_records()
            fill_authors(await core.list_authors(search_var.get().strip()))

        spawn(_clear(), lambda e: records_feedback.set(_("清空失败：{error}").format(error=e)))

    btn_delete.configure(command=delete_selected)
    btn_clear.configure(command=clear_all)

    def toggle_theme() -> None:
        nonlocal dark
        dark = not dark
        core.set_theme_pref("dark" if dark else "light")
        apply_theme()

    # ---------- 页 4：设置 ----------
    page_settings = make_page("settings")
    ttk.Label(page_settings, text=_("设置"), style="Title.TLabel").pack(
        anchor="w", padx=20, pady=(14, 4)
    )
    # 抖音登录：入口和状态都在设置页
    ttk.Label(page_settings, text=_("抖音登录"), style="Section.TLabel").pack(
        anchor="w", padx=20, pady=(12, 2)
    )
    ttk.Label(page_settings, textvariable=set_login_var).pack(anchor="w", padx=20)
    login_row = ttk.Frame(page_settings)
    login_row.pack(fill="x", padx=20, pady=8)
    btn_settings_login = ttk.Button(
        login_row, text=_("登录抖音"), style="Accent.TButton"
    )
    btn_settings_login.pack(side="left")
    ttk.Label(
        login_row,
        text=_("点开弹窗：扫码登录，或者粘贴 Cookie 保存并校验。"),
        style="Sub.TLabel",
    ).pack(side="left", padx=10)
    # 外观：深色/浅色也归设置页
    ttk.Label(page_settings, text=_("外观"), style="Section.TLabel").pack(
        anchor="w", padx=20, pady=(16, 2)
    )
    ttk.Button(
        page_settings, textvariable=theme_text_var, command=toggle_theme
    ).pack(anchor="w", padx=20, pady=6)
    # 下载目录（只看不动）
    ttk.Label(page_settings, text=_("下载目录"), style="Section.TLabel").pack(
        anchor="w", padx=20, pady=(16, 2)
    )
    ttk.Label(page_settings, textvariable=set_root_var, style="Sub.TLabel").pack(
        anchor="w", padx=20
    )
    # 我的抖音主页链接
    ttk.Label(page_settings, text=_("我的抖音主页链接"), style="Section.TLabel").pack(
        anchor="w", padx=20, pady=(16, 2)
    )
    ttk.Label(
        page_settings,
        text=_("下载「收藏作品」要用：打开抖音网页版你的主页，复制地址栏链接粘贴到这里。"),
        style="Sub.TLabel",
    ).pack(anchor="w", padx=20)
    owner_var = tk.StringVar(value=core.status()["owner_url"])
    ttk.Entry(page_settings, textvariable=owner_var).pack(fill="x", padx=20, pady=8)
    owner_feedback = tk.StringVar()
    owner_row = ttk.Frame(page_settings)
    owner_row.pack(fill="x", padx=20)
    ttk.Label(owner_row, textvariable=owner_feedback, style="Sub.TLabel").pack(side="left")

    def paste_owner() -> None:
        try:
            owner_var.set(root.clipboard_get())
        except tk.TclError:
            pass

    def save_owner() -> None:
        async def _save():
            ok, error = await core.save_owner_url(owner_var.get())
            owner_feedback.set(_("主页链接已保存") if ok else error)

        spawn(_save(), lambda e: owner_feedback.set(_("保存失败：{error}").format(error=e)))

    ttk.Button(owner_row, text=_("保存主页链接"), command=save_owner).pack(side="right")
    ttk.Button(owner_row, text=_("从剪贴板粘贴"), command=paste_owner).pack(
        side="right", padx=6
    )

    closed = False

    def on_close() -> None:
        nonlocal closed
        closed = True

    root.protocol("WM_DELETE_WINDOW", on_close)
    ttk.Button(page_settings, text=_("关闭图形界面"), command=on_close).pack(
        anchor="e", padx=20, pady=18
    )

    # ---------- 登录弹窗 ----------
    def paint_login_dialog() -> None:
        if not login_dlg:
            return
        p = pal()
        login_dlg["top"].configure(background=p["bg"])
        login_dlg["info"].configure(background=p["info_bg"])
        for widget in login_dlg["labels"]:
            widget.configure(background=p["bg"], foreground=p["fg"])
        login_dlg["info_text"].configure(
            background=p["info_bg"], foreground=p["fg"]
        )
        for widget in login_dlg["subs"]:
            widget.configure(background=p["bg"], foreground=p["sub"])
        login_dlg["cookie_text"].configure(
            background=p["panel"],
            foreground=p["fg"],
            insertbackground=p["fg"],
            highlightbackground=p["border"],
        )
        login_dlg["divider"].configure(background=p["bg"])
        for line in login_dlg["lines"]:
            line.configure(background=p["border"])

    def close_login_dialog() -> None:
        if core.login["state"] in ("starting", "waiting"):
            core.cancel_browser_login()
        if login_dlg:
            try:
                login_dlg["top"].destroy()
            except Exception:
                pass
            login_dlg.clear()

    def open_login_dialog() -> None:
        if login_dlg:
            login_dlg["top"].lift()
            return
        p = pal()
        top = tk.Toplevel(root)
        top.title(_("抖音登录"))
        top.geometry("540x510")
        top.resizable(False, False)
        top.transient(root)
        top.configure(background=p["bg"])
        labels: list = []
        subs: list = []

        def make_label(parent, **kwargs):
            widget = tk.Label(parent, **kwargs)
            labels.append(widget)
            return widget

        head = tk.Frame(top, background=p["bg"])
        head.pack(fill="x", padx=18, pady=(14, 4))
        make_label(
            head, text=_("抖音登录"), font=("Microsoft YaHei UI", 13, "bold")
        ).pack(side="left")
        tk.Button(
            head,
            text="✕",
            relief="flat",
            bd=0,
            background=p["bg"],
            foreground=p["fg"],
            command=close_login_dialog,
        ).pack(side="right")
        make_label(
            top, text=_("扫码登录（推荐）"), font=("Microsoft YaHei UI", 11, "bold")
        ).pack(anchor="w", padx=18, pady=(8, 6))
        info = tk.Frame(top, background=p["info_bg"])
        info.pack(fill="x", padx=18)
        info_text = tk.Label(
            info,
            text=_(
                "ⓘ  点击下方按钮打开登录窗口，扫码并完成二次验证后，"
                "程序会自动读取 Cookie 并关闭窗口。"
            ),
            wraplength=470,
            justify="left",
            background=p["info_bg"],
            foreground=p["fg"],
            font=("Microsoft YaHei UI", 10),
        )
        info_text.pack(anchor="w", padx=12, pady=10)
        btn_dlg_login = tk.Button(
            top,
            text=_("打开登录窗口"),
            relief="flat",
            bd=0,
            pady=9,
            cursor="hand2",
            background=p["accent"],
            foreground="#FFFFFF",
            activebackground=p["accent_active"],
            font=("Microsoft YaHei UI", 11, "bold"),
        )
        btn_dlg_login.pack(fill="x", padx=18, pady=(12, 4))
        login_row = tk.Frame(top, background=p["bg"])
        login_row.pack(fill="x", padx=18)
        dlg_login_var = tk.StringVar()
        dlg_login_label = tk.Label(
            login_row, textvariable=dlg_login_var, font=("Microsoft YaHei UI", 9)
        )
        subs.append(dlg_login_label)
        dlg_login_label.pack(side="left")
        btn_dlg_cancel = ttk.Button(login_row, text=_("取消登录"))
        btn_dlg_cancel.pack(side="right")
        divider = tk.Frame(top, background=p["bg"])
        divider.pack(fill="x", padx=18, pady=(12, 4))
        line1 = tk.Frame(divider, height=1, background=p["border"])
        line1.pack(side="left", fill="x", expand=True)
        divider_label = tk.Label(
            divider, text=_("  或者手动粘贴 Cookie  "), font=("Microsoft YaHei UI", 9)
        )
        subs.append(divider_label)
        divider_label.pack(side="left")
        line2 = tk.Frame(divider, height=1, background=p["border"])
        line2.pack(side="left", fill="x", expand=True)
        hint = tk.Label(
            top,
            text=_("从浏览器复制完整 Cookie 粘贴到下面（需包含 sessionid）："),
            font=("Microsoft YaHei UI", 9),
        )
        subs.append(hint)
        hint.pack(anchor="w", padx=18, pady=(4, 4))
        cookie_text = tk.Text(
            top,
            height=5,
            wrap="word",
            font=("Consolas", 9),
            relief="flat",
            bd=0,
            highlightthickness=1,
        )
        cookie_text.pack(fill="x", padx=18)
        bottom = tk.Frame(top, background=p["bg"])
        bottom.pack(fill="x", padx=18, pady=12)
        dlg_cookie_var = tk.StringVar()
        dlg_cookie_label = tk.Label(
            bottom, textvariable=dlg_cookie_var, font=("Microsoft YaHei UI", 9)
        )
        subs.append(dlg_cookie_label)
        dlg_cookie_label.pack(side="left")
        btn_dlg_save = ttk.Button(bottom, text=_("保存并校验"), style="Accent.TButton")
        btn_dlg_save.pack(side="right")
        ttk.Button(bottom, text=_("从剪贴板粘贴")).pack(side="right", padx=6)

        def paste_cookie() -> None:
            try:
                text = root.clipboard_get()
            except tk.TclError:
                text = ""
            cookie_text.delete("1.0", "end")
            if text:
                cookie_text.insert("1.0", text)

        bottom.winfo_children()[-1].configure(command=paste_cookie)

        def start_login() -> None:
            async def _login():
                await core.start_browser_login()
                refresh_status()

            spawn(
                _login(),
                lambda e: dlg_login_var.set(_("登录失败：{error}").format(error=e)),
            )

        btn_dlg_login.configure(command=start_login)
        btn_dlg_cancel.configure(command=core.cancel_browser_login)

        def save_cookie() -> None:
            async def _save():
                ok, message = await core.save_cookie(cookie_text.get("1.0", "end"))
                dlg_cookie_var.set(message)
                if ok:
                    refresh_status()

            spawn(
                _save(),
                lambda e: dlg_cookie_var.set(_("保存失败：{error}").format(error=e)),
            )

        btn_dlg_save.configure(command=save_cookie)
        top.protocol("WM_DELETE_WINDOW", close_login_dialog)
        login_dlg.update(
            {
                "top": top,
                "info": info,
                "info_text": info_text,
                "labels": labels,
                "subs": subs,
                "cookie_text": cookie_text,
                "divider": divider,
                "lines": [line1, line2],
                "login_var": dlg_login_var,
                "btn_login": btn_dlg_login,
                "btn_cancel": btn_dlg_cancel,
            }
        )

    btn_settings_login.configure(command=open_login_dialog)

    # ---------- 状态同步与主循环 ----------
    show_page("download")
    apply_theme()
    refresh_status()
    refresh_authors()

    log_seen = 0
    last_login_state = None

    def sync_ui() -> None:
        nonlocal log_seen, last_login_state
        state = core.job_state()
        if state["running"]:
            step_var.set(
                _("{title} · {step}（已用时 {elapsed} 秒）").format(
                    title=state["title"], step=state["step"], elapsed=state["elapsed"]
                )
            )
        elif state["title"]:
            step_var.set(
                _("{title} · {step}").format(title=state["title"], step=state["step"])
            )
        lines = state["logs"]
        if len(lines) < log_seen:
            log_text.configure(state="normal")
            log_text.delete("1.0", "end")
            log_text.configure(state="disabled")
            log_seen = 0
        if len(lines) > log_seen:
            at_bottom = log_text.yview()[1] > 0.999
            log_text.configure(state="normal")
            for line in lines[log_seen:]:
                log_text.insert("end", line + "\n")
            log_text.configure(state="disabled")
            if at_bottom:
                log_text.see("end")
            log_seen = len(lines)
        running = state["running"]
        btn_collection.configure(state="disabled" if running else "normal")
        btn_music.configure(state="disabled" if running else "normal")
        btn_stop.configure(state="normal" if running else "disabled")
        # 登录状态：侧栏与弹窗同步
        login = core.login
        if login["state"] != last_login_state:
            last_login_state = login["state"]
            if login["state"] in ("done", "error"):
                refresh_status()
        if login_dlg:
            if login["message"]:
                login_dlg["login_var"].set(login["message"])
            busy = login["state"] in ("starting", "waiting")
            login_dlg["btn_login"].configure(
                state="disabled" if busy else "normal",
                text=_("等待登录中…") if busy else _("打开登录窗口"),
            )
            login_dlg["btn_cancel"].configure(
                state="normal" if busy else "disabled"
            )

    try:
        while not closed:
            try:
                if not root.winfo_exists():
                    break  # 窗口已被销毁
                root.update()
            except tk.TclError:
                break  # 窗口被外部销毁时直接退出
            sync_ui()
            await asyncio.sleep(0.03)
    finally:
        core.release()
        try:
            root.destroy()
        except Exception:
            pass
        console.print(_("图形界面已关闭。"))
