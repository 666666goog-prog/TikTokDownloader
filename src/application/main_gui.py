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


def _work_row(raw: dict) -> dict:
    """把一条作品原始数据整理成列表里显示的一行。

    类型的判断口径与下载引擎完全一致：有图片且图片带 video
    字段的是实况，只有图片的是图集，其余是视频。
    """
    images = raw.get("images") or []
    if images:
        type_ = (
            _("实况")
            if any(isinstance(i, dict) and i.get("video") for i in images)
            else _("图片")
        )
    else:
        type_ = _("视频")
    timestamp = raw.get("create_time") or 0
    try:
        date = strftime("%Y-%m-%d", localtime(int(timestamp)))
    except (TypeError, ValueError, OSError):
        date = ""
    mix = ""
    mix_info = raw.get("mix_info")
    if isinstance(mix_info, dict):
        mix = str(mix_info.get("mix_name") or "")
    return {
        "id": str(raw.get("aweme_id") or ""),
        "date": date,
        "desc": str(raw.get("desc") or "").replace("\n", " ").strip(),
        "type": type_,
        "mix": mix,
    }


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
        # 主页作品：先拉列表再勾选下载的状态
        self.account = {
            "state": "idle",  # idle/loading/done/error
            "message": "",
            "rows": [],
            "author": "",
        }
        self._account_raw: dict[str, dict] = {}
        self._account_ctx: dict = {}
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
        message = _(
            "正在拉取{text}列表（已获取 {count} 条，第 {page} 页）…"
        ).format(text=text, count=count, page=page)
        if self.job["running"]:
            self.job["step"] = message
        if self.account["state"] == "loading":
            self.account["message"] = message

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

    def start_account_job(self, selected_ids: list[str]) -> tuple[bool, str]:
        """下载主页作品列表里勾选的那部分作品。"""
        if self.job["running"]:
            return False, _("已有任务正在进行，等它完成或先停止它")
        if not self.app.parameter.cookie_state:
            return False, _("Cookie 还没设置，先到「设置」页登录或粘贴 Cookie")
        if self.account["state"] != "done":
            return False, _("先点「获取作品」，把作品列表拉出来")
        raw = [self._account_raw[i] for i in selected_ids if i in self._account_raw]
        if not raw:
            return False, _("先在列表里勾选要下载的作品")
        ctx = self._account_ctx

        async def factory():
            result = await self.engine._batch_process_detail(
                raw,
                user_id=ctx.get("sec_user_id", ""),
                mark="",
                api=False,
                earliest=ctx.get("earliest"),
                latest=ctx.get("latest"),
                tiktok=False,
                mode="post",
                info=ctx.get("info"),
            )
            if result is False:
                raise RuntimeError(_("作品数据提取失败，看日志里的提示"))

        return self._begin_job(_("下载主页作品"), factory)

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

    # ---------- 主页作品（先拉列表，再勾选下载） ----------

    async def fetch_account(self, url: str) -> None:
        """把某个作者的全部作品拉成列表（只拉列表，不下载）。"""
        if self.account["state"] == "loading":
            return
        if self.job["running"]:
            self.account = {
                "state": "error",
                "message": _("有下载任务正在进行，等它完成再获取作品"),
                "rows": [],
                "author": "",
            }
            return
        if not self.app.parameter.cookie_state:
            self.account = {
                "state": "error",
                "message": _("Cookie 还没设置，先到「设置」页登录或粘贴 Cookie"),
                "rows": [],
                "author": "",
            }
            return
        url = (url or "").strip()
        if not url:
            self.account = {
                "state": "error",
                "message": _("先粘贴作者的抖音主页链接"),
                "rows": [],
                "author": "",
            }
            return
        self.account = {
            "state": "loading",
            "message": _("正在获取作品列表…"),
            "rows": [],
            "author": "",
        }
        try:
            sec_user_id = await self.engine.check_sec_user_id(url)
            if not sec_user_id:
                raise RuntimeError(
                    _("这个链接取不到账号信息，请确认是作者的抖音主页链接")
                )
            try:
                info = await self.engine.get_user_info_data(
                    sec_user_id=sec_user_id
                )
            except Exception:  # noqa: BLE001 - 账号信息拿不到不挡拉列表
                info = None
            data, earliest, latest = await self.engine._get_account_data(
                sec_user_id=sec_user_id,
                tab="post",
            )
            if not data:
                raise RuntimeError(
                    _(
                        "没有获取到作品（账号可能没有公开作品，"
                        "或 Cookie 已过期）"
                    )
                )
            rows = []
            self._account_raw = {}
            for raw in data:
                row = _work_row(raw)
                if not row["id"]:
                    continue
                self._account_raw[row["id"]] = raw
                rows.append(row)
            self._account_ctx = {
                "sec_user_id": sec_user_id,
                "info": info,
                "earliest": earliest,
                "latest": latest,
            }
            author = ""
            if isinstance(info, dict):
                author = str(info.get("nickname") or "")
            if not author and data:
                first_author = data[0].get("author") or {}
                if isinstance(first_author, dict):
                    author = str(first_author.get("nickname") or "")
            self.account = {
                "state": "done",
                "message": _("获取完成：共 {count} 个作品，勾选后点「开始下载」").format(
                    count=len(rows)
                ),
                "rows": rows,
                "author": author,
            }
        except Exception as error:  # noqa: BLE001 - 失败原因要显示在界面上
            self.account = {
                "state": "error",
                "message": str(error),
                "rows": [],
                "author": "",
            }

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

    # ---------- 页 2：主页作品（先拉列表，勾选后再下载） ----------
    page_account = ttk.Frame(notebook)
    notebook.add(page_account, text=_("主页作品"))
    acct_row = ttk.Frame(page_account)
    acct_row.pack(fill="x", padx=12, pady=10)
    ttk.Label(acct_row, text=_("作者主页链接：")).pack(side="left")
    account_url_var = tk.StringVar()
    ttk.Entry(acct_row, textvariable=account_url_var).pack(
        side="left", fill="x", expand=True, padx=6
    )

    def paste_account_url() -> None:
        try:
            account_url_var.set(root.clipboard_get())
        except tk.TclError:
            pass

    ttk.Button(acct_row, text=_("从剪贴板粘贴"), command=paste_account_url).pack(
        side="left"
    )
    btn_fetch = ttk.Button(acct_row, text=_("获取作品"))
    btn_fetch.pack(side="left", padx=(6, 0))
    account_feedback = tk.StringVar(
        value=_("粘贴作者的抖音主页链接，点「获取作品」，作品会先列出来，勾选后再下载")
    )
    ttk.Label(page_account, textvariable=account_feedback, style="Sub.TLabel").pack(
        anchor="w", padx=12
    )
    acct_tree = ttk.Treeview(
        page_account,
        columns=("check", "date", "desc", "mix", "type"),
        show="headings",
        height=14,
    )
    acct_tree.heading("check", text=_("选"))
    acct_tree.heading("date", text=_("发布日期"))
    acct_tree.heading("desc", text=_("描述"))
    acct_tree.heading("mix", text=_("合集"))
    acct_tree.heading("type", text=_("类型"))
    acct_tree.column("check", width=40, anchor="center", stretch=False)
    acct_tree.column("date", width=95, anchor="center", stretch=False)
    acct_tree.column("desc", width=430)
    acct_tree.column("mix", width=150)
    acct_tree.column("type", width=60, anchor="center", stretch=False)
    acct_tree.pack(fill="both", expand=True, padx=12, pady=6)
    checked_ids: set[str] = set()

    def fill_account_tree(rows: list[dict]) -> None:
        acct_tree.delete(*acct_tree.get_children())
        checked_ids.clear()
        for row in rows:
            checked_ids.add(row["id"])
            acct_tree.insert(
                "",
                "end",
                iid=row["id"],
                values=("☑", row["date"], row["desc"], row["mix"], row["type"]),
            )
        update_account_button()

    def update_account_button() -> None:
        count = len(checked_ids)
        btn_account_download.configure(
            text=_("开始下载（{count}）").format(count=count)
        )

    def toggle_row(event) -> None:
        row_id = acct_tree.identify_row(event.y)
        if not row_id:
            return
        if row_id in checked_ids:
            checked_ids.discard(row_id)
            mark = "☐"
        else:
            checked_ids.add(row_id)
            mark = "☑"
        values = list(acct_tree.item(row_id, "values"))
        values[0] = mark
        acct_tree.item(row_id, values=values)
        update_account_button()

    acct_tree.bind("<ButtonRelease-1>", toggle_row)

    acct_btn_row = ttk.Frame(page_account)
    acct_btn_row.pack(fill="x", padx=12, pady=(0, 10))

    def select_all() -> None:
        for row_id in acct_tree.get_children():
            checked_ids.add(row_id)
            values = list(acct_tree.item(row_id, "values"))
            values[0] = "☑"
            acct_tree.item(row_id, values=values)
        update_account_button()

    def invert_selection() -> None:
        for row_id in acct_tree.get_children():
            if row_id in checked_ids:
                checked_ids.discard(row_id)
                mark = "☐"
            else:
                checked_ids.add(row_id)
                mark = "☑"
            values = list(acct_tree.item(row_id, "values"))
            values[0] = mark
            acct_tree.item(row_id, values=values)
        update_account_button()

    def clear_account_list() -> None:
        acct_tree.delete(*acct_tree.get_children())
        checked_ids.clear()
        update_account_button()

    ttk.Button(acct_btn_row, text=_("全选"), command=select_all).pack(side="left")
    ttk.Button(acct_btn_row, text=_("反选"), command=invert_selection).pack(
        side="left", padx=6
    )
    ttk.Button(acct_btn_row, text=_("清空列表"), command=clear_account_list).pack(
        side="left"
    )
    btn_account_download = ttk.Button(
        acct_btn_row, text=_("开始下载（0）"), style="Accent.TButton"
    )
    btn_account_download.pack(side="right")

    def fetch_account() -> None:
        async def _fetch():
            await core.fetch_account(account_url_var.get())

        spawn(
            _fetch(),
            lambda e: account_feedback.set(
                _("获取失败：{error}").format(error=e)
            ),
        )

    btn_fetch.configure(command=fetch_account)

    def start_account_download() -> None:
        selected = [r for r in acct_tree.get_children() if r in checked_ids]
        ok, error = core.start_account_job(selected)
        if not ok:
            messagebox.showwarning(_("暂时不能开始"), error, parent=root)

    btn_account_download.configure(command=start_account_download)

    # ---------- 页 3：下载规则 ----------
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

        spawn(_save(), lambda e: rules_feedback.set(_("保存失败：{error}").format(error=e)))

    ttk.Button(rules_row, text=_("保存规则"), command=save_rules).pack(side="right")

    # ---------- 页 4：下载记录 ----------
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

    # ---------- 页 5：设置 ----------
    page_settings = ttk.Frame(notebook)
    notebook.add(page_settings, text=_("设置"))
    ttk.Label(page_settings, text=_("抖音登录"), style="Title.TLabel").pack(
        anchor="w", padx=12, pady=(12, 2)
    )
    ttk.Label(
        page_settings,
        text=_(
            "点下面的按钮会打开一个登录窗口，在里面扫码登录抖音，"
            "登录好程序会自动读取并保存 Cookie，窗口自动关掉，全程不用复制粘贴。"
        ),
        style="Sub.TLabel",
    ).pack(anchor="w", padx=12)
    login_row = ttk.Frame(page_settings)
    login_row.pack(fill="x", padx=12, pady=6)
    btn_login = ttk.Button(login_row, text=_("打开登录窗口（扫码登录）"))
    btn_login.pack(side="left")
    btn_login_cancel = ttk.Button(login_row, text=_("取消登录"))
    btn_login_cancel.pack(side="left", padx=6)
    login_feedback = tk.StringVar()
    ttk.Label(login_row, textvariable=login_feedback, style="Sub.TLabel").pack(
        side="left", padx=6
    )

    def start_login() -> None:
        async def _login():
            await core.start_browser_login()
            refresh_status()

        spawn(
            _login(),
            lambda e: login_feedback.set(_("登录失败：{error}").format(error=e)),
        )

    btn_login.configure(command=start_login)
    btn_login_cancel.configure(command=core.cancel_browser_login)

    ttk.Label(page_settings, text=_("Cookie（登录凭证）"), style="Title.TLabel").pack(
        anchor="w", padx=12, pady=(12, 2)
    )
    ttk.Label(
        page_settings,
        text=_("不想扫码也可以用老办法：从浏览器复制 Cookie 后，点「从剪贴板粘贴」，再点「保存并校验」。"),
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
            ok, message = await core.save_cookie(cookie_text.get("1.0", "end"))
            cookie_feedback.set(message)
            if ok:
                refresh_status()

        spawn(_save(), lambda e: cookie_feedback.set(_("保存失败：{error}").format(error=e)))

    ttk.Button(cookie_row, text=_("保存并校验"), command=save_cookie).pack(side="right")
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

        spawn(_save(), lambda e: owner_feedback.set(_("保存失败：{error}").format(error=e)))

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
    last_account_state = None

    def sync_ui() -> None:
        nonlocal log_seen, last_account_state
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
        acct = core.account
        acct_loading = acct["state"] == "loading"
        btn_collection.configure(
            state="disabled" if (running or acct_loading) else "normal"
        )
        btn_music.configure(
            state="disabled" if (running or acct_loading) else "normal"
        )
        btn_stop.configure(state="normal" if running else "disabled")
        # 主页作品：状态文字与列表刷新
        if acct["message"]:
            if acct["state"] == "done" and acct["author"]:
                account_feedback.set(
                    "{author} · {message}".format(
                        author=acct["author"], message=acct["message"]
                    )
                )
            else:
                account_feedback.set(acct["message"])
        if acct["state"] != last_account_state:
            last_account_state = acct["state"]
            if acct["state"] == "done":
                fill_account_tree(acct["rows"])
            elif acct["state"] == "loading":
                acct_tree.delete(*acct_tree.get_children())
                checked_ids.clear()
                update_account_button()
        btn_fetch.configure(
            state="disabled" if (running or acct_loading) else "normal"
        )
        btn_account_download.configure(
            state="disabled"
            if (running or acct["state"] != "done" or not checked_ids)
            else "normal"
        )
        # 扫码登录：状态文字与按钮
        login = core.login
        if login["message"]:
            login_feedback.set(login["message"])
        login_busy = login["state"] in ("starting", "waiting")
        btn_login.configure(state="disabled" if login_busy else "normal")
        btn_login_cancel.configure(state="normal" if login_busy else "disabled")

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
