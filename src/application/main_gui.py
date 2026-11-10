# -*- coding: utf-8 -*-
"""图形界面模式（收藏版）：程序自己的窗口，全程鼠标操作。

在主菜单选择「图形界面模式」后，本模块打开一个独立的桌面窗口
（不借助浏览器），窗口里的按钮调用与终端模式完全相同的下载
引擎（TikTok 类），下载规则、下载记录共用同一套配置与数据库。

窗口事件与程序的主事件循环协同运行（定时处理窗口事件），因此
日志、拉取进度都能实时刷新；关闭窗口即回到主菜单。
"""

import asyncio
from collections import deque
from time import localtime, strftime, time

from ..interface import API
from ..translation import _
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
        rows = await self.app.recorder.list_authors()
        if keyword:
            rows = [r for r in rows if keyword in (r.get("name") or "")]
        rows.sort(key=lambda r: (SOURCE_ORDER.get(r.get("source"), 9), -r["count"]))
        return rows

    async def delete_author(self, uid: str, source: str) -> int:
        return await self.app.recorder.delete_by_author(uid, source)

    async def clear_records(self) -> None:
        await self.app.recorder.delete_ids("ALL")

    # ---------- 设置 ----------

    async def save_cookie(self, cookie: str) -> tuple[bool, str]:
        cookie = (cookie or "").strip()
        if not cookie:
            return False, _("内容是空的")
        if not self.app.cookie.run(cookie, tiktok=False):
            return False, _("这段内容不像有效的 Cookie，请重新复制")
        await self.refresh_engine()
        return True, ""

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
# ---------- 窗口层（tkinter 在此惰性导入，导入本模块不需要显示器） ----------

_PALETTES = {
    "light": {
        "bg": "#F5F5F5",
        "panel": "#FFFFFF",
        "fg": "#1F2329",
        "sub": "#6B7280",
        "entry": "#FFFFFF",
        "border": "#D9D9D9",
        "accent": "#07C160",
        "accent_active": "#06AD56",
        "danger": "#E54545",
        "tab_selected": "#FFFFFF",
    },
    "dark": {
        "bg": "#191919",
        "panel": "#232323",
        "fg": "#EDEDED",
        "sub": "#9CA3AF",
        "entry": "#2C2C2C",
        "border": "#3A3A3A",
        "accent": "#07C160",
        "accent_active": "#06AD56",
        "danger": "#F0524F",
        "tab_selected": "#2C2C2C",
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
    """打开图形界面窗口，直到用户关闭窗口才返回（回到主菜单）。"""
    import tkinter as tk
    from tkinter import messagebox, ttk

    core = GuiCore(app)
    core.build_engine()
    console = app.console
    console.print()
    console.print(_("图形界面已打开，在弹出的程序窗口里用鼠标操作即可。"))
    console.print(_("关闭那个窗口，就会回到这个主菜单。"))

    pref = core.get_theme_pref()
    dark = pref == "dark" or (pref == "" and _system_prefers_dark())

    root = tk.Tk()
    root.title(_("DouK-Downloader 收藏版 · 图形界面"))
    root.geometry("900x740")
    root.minsize(780, 620)

    style = ttk.Style(root)
    raw_widgets: list = []  # 需要手工上色的非 ttk 控件（文本框等）

    def apply_theme() -> None:
        pal = _PALETTES["dark" if dark else "light"]
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        style.configure("TFrame", background=pal["bg"])
        style.configure(
            "TLabel", background=pal["bg"], foreground=pal["fg"], font=("Microsoft YaHei UI", 10)
        )
        style.configure(
            "Sub.TLabel", background=pal["bg"], foreground=pal["sub"], font=("Microsoft YaHei UI", 9)
        )
        style.configure(
            "Title.TLabel",
            background=pal["bg"],
            foreground=pal["fg"],
            font=("Microsoft YaHei UI", 13, "bold"),
        )
        style.configure(
            "TButton",
            background=pal["panel"],
            foreground=pal["fg"],
            bordercolor=pal["border"],
            font=("Microsoft YaHei UI", 10),
            padding=(12, 6),
        )
        style.map("TButton", background=[("active", pal["entry"])])
        style.configure(
            "Accent.TButton",
            background=pal["accent"],
            foreground="#FFFFFF",
            font=("Microsoft YaHei UI", 12, "bold"),
            padding=(18, 12),
        )
        style.map(
            "Accent.TButton",
            background=[("active", pal["accent_active"]), ("disabled", pal["border"])],
        )
        style.configure(
            "Danger.TButton", foreground=pal["danger"], font=("Microsoft YaHei UI", 10), padding=(12, 6)
        )
        style.configure(
            "TCheckbutton",
            background=pal["bg"],
            foreground=pal["fg"],
            font=("Microsoft YaHei UI", 10),
        )
        style.configure(
            "TLabelframe", background=pal["bg"], bordercolor=pal["border"]
        )
        style.configure(
            "TLabelframe.Label",
            background=pal["bg"],
            foreground=pal["fg"],
            font=("Microsoft YaHei UI", 10, "bold"),
        )
        style.configure("TNotebook", background=pal["bg"], borderwidth=0)
        style.configure(
            "TNotebook.Tab",
            background=pal["panel"],
            foreground=pal["fg"],
            font=("Microsoft YaHei UI", 10),
            padding=(16, 7),
        )
        style.map("TNotebook.Tab", background=[("selected", pal["tab_selected"])])
        style.configure(
            "TEntry", fieldbackground=pal["entry"], foreground=pal["fg"]
        )
        style.configure(
            "TSpinbox", fieldbackground=pal["entry"], foreground=pal["fg"]
        )
        style.configure(
            "Treeview",
            background=pal["panel"],
            fieldbackground=pal["panel"],
            foreground=pal["fg"],
            rowheight=26,
            font=("Microsoft YaHei UI", 10),
        )
        style.configure(
            "Treeview.Heading",
            background=pal["bg"],
            foreground=pal["fg"],
            font=("Microsoft YaHei UI", 10, "bold"),
        )
        root.configure(background=pal["bg"])
        for widget in raw_widgets:
            widget.configure(
                background=pal["panel"],
                foreground=pal["fg"],
                insertbackground=pal["fg"],
                selectbackground=pal["accent"],
            )
        theme_button.configure(text=_("☀️ 浅色") if dark else _("🌙 深色"))

    # ---------- 顶部状态栏 ----------
    header = ttk.Frame(root)
    header.pack(fill="x", padx=14, pady=(12, 4))
    ttk.Label(header, text=_("DouK-Downloader 收藏版"), style="Title.TLabel").pack(side="left")
    theme_button = ttk.Button(header)
    theme_button.pack(side="right")
    status_var = tk.StringVar()
    ttk.Label(root, textvariable=status_var, style="Sub.TLabel").pack(
        anchor="w", padx=16
    )

    def refresh_status() -> None:
        info = core.status()
        status_var.set(
            _("Cookie：{cookie} · 下载目录：{root}").format(
                cookie=_("已设置") if info["cookie_state"] else _("未设置"),
                root=info["root"],
            )
        )

    def toggle_theme() -> None:
        nonlocal dark
        dark = not dark
        core.set_theme_pref("dark" if dark else "light")
        apply_theme()

    theme_button.configure(command=toggle_theme)

    notebook = ttk.Notebook(root)
    notebook.pack(fill="both", expand=True, padx=12, pady=10)

    # ---------- 页 1：下载 ----------
    page_job = ttk.Frame(notebook)
    notebook.add(page_job, text=_("下载"))

    buttons = ttk.Frame(page_job)
    buttons.pack(fill="x", padx=10, pady=12)
    btn_collection = ttk.Button(
        buttons, text=_("批量下载收藏作品"), style="Accent.TButton"
    )
    btn_collection.pack(side="left", expand=True, fill="x", padx=(0, 6))
    btn_music = ttk.Button(buttons, text=_("批量下载收藏音乐"), style="Accent.TButton")
    btn_music.pack(side="left", expand=True, fill="x", padx=(6, 0))

    step_var = tk.StringVar(value=_("点上面的按钮开始下载"))
    ttk.Label(page_job, textvariable=step_var).pack(anchor="w", padx=12)
    log_text = tk.Text(page_job, height=16, wrap="word", font=("Consolas", 9))
    raw_widgets.append(log_text)
    log_text.pack(fill="both", expand=True, padx=10, pady=8)
    log_text.configure(state="disabled")
    job_row = ttk.Frame(page_job)
    job_row.pack(fill="x", padx=10, pady=(0, 10))
    btn_stop = ttk.Button(job_row, text=_("停止"), style="Danger.TButton")
    btn_stop.pack(side="right")
    job_hint = ttk.Label(
        job_row,
        text=_("停止不会白下：下好的文件下次自动跳过"),
        style="Sub.TLabel",
    )
    job_hint.pack(side="left")

    def start(kind: str) -> None:
        ok, error = core.start_job(kind)
        if not ok:
            messagebox.showwarning(_("暂时不能开始"), error, parent=root)

    btn_collection.configure(command=lambda: start("collection"))
    btn_music.configure(command=lambda: start("music"))
    btn_stop.configure(command=core.stop_job)

    # ---------- 页 2：下载规则 ----------
    page_rules = ttk.Frame(notebook)
    notebook.add(page_rules, text=_("下载规则"))
    rules = core.get_rules()
    threshold_var = tk.IntVar(value=int(rules["album_threshold"]))
    row_top = ttk.Frame(page_rules)
    row_top.pack(fill="x", padx=12, pady=10)
    ttk.Label(row_top, text=_("图集有几张图片时，单独建一个文件夹（并下载背景音乐）：")).pack(side="left")
    ttk.Spinbox(
        row_top, from_=1, to=999, width=6, textvariable=threshold_var
    ).pack(side="left", padx=6)
    rule_vars: dict[str, tk.BooleanVar] = {}
    for group_title, items in _RULE_GROUPS:
        frame = ttk.Labelframe(page_rules, text=_(group_title))
        frame.pack(fill="x", padx=12, pady=6)
        for key, label in items:
            var = tk.BooleanVar(value=bool(rules[key]))
            rule_vars[key] = var
            ttk.Checkbutton(frame, text=_(label), variable=var).pack(
                anchor="w", padx=10, pady=3
            )
    rules_feedback = tk.StringVar()
    rules_row = ttk.Frame(page_rules)
    rules_row.pack(fill="x", padx=12, pady=10)
    ttk.Label(rules_row, textvariable=rules_feedback, style="Sub.TLabel").pack(side="left")

    def save_rules() -> None:
        values = {k: v.get() for k, v in rule_vars.items()}
        values["album_threshold"] = threshold_var.get()

        async def _save():
            ok, error = await core.save_rules(values)
            rules_feedback.set(_("已保存，马上生效") if ok else error)

        asyncio.ensure_future(_save())

    ttk.Button(rules_row, text=_("保存规则"), command=save_rules).pack(side="right")
    # ---------- 页 3：下载记录 ----------
    page_records = ttk.Frame(notebook)
    notebook.add(page_records, text=_("下载记录"))
    search_row = ttk.Frame(page_records)
    search_row.pack(fill="x", padx=12, pady=10)
    ttk.Label(search_row, text=_("按作者名查找：")).pack(side="left")
    search_var = tk.StringVar()
    ttk.Entry(search_row, textvariable=search_var, width=24).pack(side="left", padx=6)
    tree = ttk.Treeview(
        page_records, columns=("name", "source", "count"), show="headings", height=12
    )
    tree.heading("name", text=_("作者"))
    tree.heading("source", text=_("下载来源"))
    tree.heading("count", text=_("作品数"))
    tree.column("name", width=340)
    tree.column("source", width=140, anchor="center")
    tree.column("count", width=90, anchor="center")
    tree.pack(fill="both", expand=True, padx=12)
    author_rows: list[dict] = []
    records_feedback = tk.StringVar()
    rec_row = ttk.Frame(page_records)
    rec_row.pack(fill="x", padx=12, pady=10)
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
                values=(row.get("name") or "", row.get("source") or "", row.get("count") or 0),
            )
        records_feedback.set(_("共 {count} 条").format(count=len(rows)))

    def refresh_authors() -> None:
        async def _load():
            fill_authors(await core.list_authors(search_var.get().strip()))

        asyncio.ensure_future(_load())

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
            ).format(name=row.get("name") or "", source=row.get("source") or "", count=row.get("count") or 0),
            parent=root,
        ):
            return

        async def _delete():
            await core.delete_author(str(row.get("uid") or ""), str(row.get("source") or ""))
            fill_authors(await core.list_authors(search_var.get().strip()))

        asyncio.ensure_future(_delete())

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

        asyncio.ensure_future(_clear())

    btn_delete.configure(command=delete_selected)
    btn_clear.configure(command=clear_all)

    # ---------- 页 4：设置 ----------
    page_settings = ttk.Frame(notebook)
    notebook.add(page_settings, text=_("设置"))
    ttk.Label(page_settings, text=_("Cookie（登录凭证）"), style="Title.TLabel").pack(
        anchor="w", padx=12, pady=(12, 2)
    )
    ttk.Label(
        page_settings,
        text=_("从浏览器复制 Cookie 后，点「从剪贴板粘贴」，再点「保存 Cookie」。"),
        style="Sub.TLabel",
    ).pack(anchor="w", padx=12)
    cookie_text = tk.Text(page_settings, height=4, wrap="word", font=("Consolas", 9))
    raw_widgets.append(cookie_text)
    cookie_text.pack(fill="x", padx=12, pady=6)
    cookie_feedback = tk.StringVar()
    cookie_row = ttk.Frame(page_settings)
    cookie_row.pack(fill="x", padx=12)
    ttk.Label(cookie_row, textvariable=cookie_feedback, style="Sub.TLabel").pack(side="left")

    def paste_cookie() -> None:
        try:
            text = root.clipboard_get()
        except tk.TclError:
            text = ""
        cookie_text.delete("1.0", "end")
        if text:
            cookie_text.insert("1.0", text)

    def save_cookie() -> None:
        async def _save():
            ok, error = await core.save_cookie(cookie_text.get("1.0", "end"))
            cookie_feedback.set(_("Cookie 已保存") if ok else error)
            refresh_status()

        asyncio.ensure_future(_save())

    ttk.Button(cookie_row, text=_("保存 Cookie"), command=save_cookie).pack(side="right")
    ttk.Button(cookie_row, text=_("从剪贴板粘贴"), command=paste_cookie).pack(
        side="right", padx=6
    )

    ttk.Label(page_settings, text=_("我的抖音主页链接"), style="Title.TLabel").pack(
        anchor="w", padx=12, pady=(16, 2)
    )
    ttk.Label(
        page_settings,
        text=_("下载「收藏作品」要用：打开抖音网页版你的主页，复制地址栏链接粘贴到这里。"),
        style="Sub.TLabel",
    ).pack(anchor="w", padx=12)
    owner_var = tk.StringVar(value=core.status()["owner_url"])
    ttk.Entry(page_settings, textvariable=owner_var).pack(fill="x", padx=12, pady=6)
    owner_feedback = tk.StringVar()
    owner_row = ttk.Frame(page_settings)
    owner_row.pack(fill="x", padx=12)
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

        asyncio.ensure_future(_save())

    ttk.Button(owner_row, text=_("保存主页链接"), command=save_owner).pack(side="right")
    ttk.Button(owner_row, text=_("从剪贴板粘贴"), command=paste_owner).pack(
        side="right", padx=6
    )

    # ---------- 窗口关闭与主循环 ----------
    closed = False

    def on_close() -> None:
        nonlocal closed
        closed = True

    root.protocol("WM_DELETE_WINDOW", on_close)
    ttk.Button(page_settings, text=_("关闭图形界面"), command=on_close).pack(
        anchor="e", padx=12, pady=16
    )

    apply_theme()
    refresh_status()
    refresh_authors()

    log_seen = 0
    last_running = None

    def sync_ui() -> None:
        nonlocal log_seen, last_running
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
        if state["running"] != last_running:
            last_running = state["running"]
            widget_state = "disabled" if state["running"] else "normal"
            btn_collection.configure(state=widget_state)
            btn_music.configure(state=widget_state)
            btn_stop.configure(state="normal" if state["running"] else "disabled")

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



