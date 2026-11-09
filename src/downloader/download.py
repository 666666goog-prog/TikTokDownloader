from asyncio import Semaphore, gather
from datetime import datetime
from pathlib import Path
from shutil import move
from time import time
from types import SimpleNamespace
from typing import TYPE_CHECKING, Callable, Union

from aiofiles import open
from curl_cffi.requests import Response
from curl_cffi.requests.exceptions import HTTPError, RequestException
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from ..custom import (
    MAX_WORKERS,
    PROGRESS,
)
from ..tools import (
    CacheError,
    DownloaderError,
    FakeProgress,
    Retry,
    beautify_string,
    format_size,
)
from ..translation import _

if TYPE_CHECKING:
    from curl_cffi.requests import AsyncSession

    from ..config import Parameter
    from ..manager import DownloadRecorder
    from ..module import FFMPEG
    from ..record import BaseLogger
    from ..tools import ColorfulConsole

__all__ = ["Downloader"]


class Downloader:
    semaphore = Semaphore(MAX_WORKERS)
    CONTENT_TYPE_MAP = {
        "image/png": "png",
        "image/jpeg": "jpeg",
        "image/webp": "webp",
        "video/mp4": "mp4",
        "video/quicktime": "mov",
        "audio/mp4": "m4a",
        "audio/mpeg": "mp3",
        "application/octet-stream": "mp4",
    }
    WRITE_BUFFER_SIZE = 1024 * 1024 * 100

    def __init__(
        self,
        params: "Parameter",
        server_mode: bool = False,
    ):
        self.cleaner = params.CLEANER
        self.client: "AsyncSession" = params.client
        self.client_tiktok: "AsyncSession" = params.client_tiktok
        self.headers: dict = params.headers_download
        self.headers_tiktok: dict = params.headers_download_tiktok
        self.user_agent: str = params.user_agent
        self.user_agent_tiktok: str = params.user_agent_tiktok
        self.log: "BaseLogger" = params.logger
        self.console: "ColorfulConsole" = params.console
        self.root: Path = params.root
        self.folder_name: str = params.folder_name
        self.name_format: list[str] = params.name_format
        self.desc_length: int = params.desc_length
        self.name_length: int = params.name_length
        self.split: str = params.split
        self.folder_mode: bool = params.folder_mode
        self.music: bool = params.music
        # 自定义下载规则（可在主菜单“下载规则设置”中修改）
        self.album_threshold: int = getattr(params, "album_threshold", 2)
        self.live_folder_music: bool = getattr(
            params, "live_folder_music", True
        )
        self.video_root_mode: bool = getattr(params, "video_root_mode", True)
        # 作品文件夹命名开关（日期/类型/标题/ID 各自可开关）
        self.folder_show_date: bool = getattr(params, "folder_show_date", True)
        self.folder_show_type: bool = getattr(params, "folder_show_type", True)
        self.folder_show_title: bool = getattr(
            params, "folder_show_title", False
        )
        self.folder_show_id: bool = getattr(params, "folder_show_id", False)
        # 作品文件命名开关（与文件夹开关分开，默认不带作品 ID）
        self.file_show_date: bool = getattr(params, "file_show_date", True)
        self.file_show_type: bool = getattr(params, "file_show_type", True)
        self.file_show_author: bool = getattr(params, "file_show_author", True)
        self.file_show_title: bool = getattr(params, "file_show_title", True)
        self.file_show_id: bool = getattr(params, "file_show_id", False)
        self.dynamic_cover: bool = params.dynamic_cover
        self.static_cover: bool = params.static_cover
        # self.cookie = params.cookie
        # self.cookie_tiktok = params.cookie_tiktok
        self.proxy = params.proxy
        self.proxy_tiktok = params.proxy_tiktok
        self.download: bool = params.download
        self.max_size: int = params.max_size
        self.chunk: int = params.chunk
        self.max_retry: int = params.max_retry
        self.recorder: "DownloadRecorder" = params.recorder
        self.timeout: int | float = params.timeout
        self.ffmpeg: "FFMPEG" = params.ffmpeg
        self.cache: Path = params.cache
        self.truncate: int = params.truncate
        self.general_progress_object: Callable[..., Progress | FakeProgress] = (
            self.init_general_progress(
                server_mode,
            )
        )

    def init_general_progress(
        self,
        server_mode: bool = False,
    ) -> Callable[..., Progress | FakeProgress]:
        if server_mode:
            return self.__fake_progress_object
        return self.__general_progress_object

    @staticmethod
    def __fake_progress_object(
        *args,
        **kwargs,
    ) -> FakeProgress:
        return FakeProgress()

    def __general_progress_object(self) -> Progress:
        """文件下载进度条"""
        return Progress(
            TextColumn(
                "[progress.description]{task.description}",
                style=PROGRESS,
                justify="left",
            ),
            SpinnerColumn(),
            BarColumn(bar_width=20),
            "[progress.percentage]{task.percentage:>3.1f}%",
            "•",
            DownloadColumn(binary_units=True),
            "•",
            TimeRemainingColumn(),
            console=self.console,
            transient=True,
            expand=True,
        )

    def __live_progress_object(self) -> Progress:
        """直播下载进度条"""
        return Progress(
            TextColumn(
                "[progress.description]{task.description}",
                style=PROGRESS,
                justify="left",
            ),
            SpinnerColumn(),
            BarColumn(bar_width=20),
            "•",
            TransferSpeedColumn(),
            "•",
            TimeElapsedColumn(),
            console=self.console,
            transient=True,
            expand=True,
        )

    async def run(
        self,
        data: Union[list[dict], list[tuple]],
        type_: str,
        tiktok: bool = False,
        **kwargs,
    ) -> None:
        if not self.download or not data:
            return
        self.log.info(_("开始下载作品文件"))
        match type_:
            case "batch":
                await self.run_batch(data, tiktok, **kwargs)
            case "detail":
                await self.run_general(data, tiktok, **kwargs)
            case "music":
                await self.run_music(data, **kwargs)
            case "live":
                await self.run_live(data, tiktok, **kwargs)
            case _:
                raise ValueError

    async def run_batch(
        self,
        data: list[dict],
        tiktok: bool,
        mode: str = "",
        mark: str = "",
        user_id: str = "",
        user_name: str = "",
        mix_id: str = "",
        mix_title: str = "",
        collect_id: str = "",
        collect_name: str = "",
    ) -> None:
        root = self.storage_folder(
            mode,
            *self.data_classification(
                mode,
                mark,
                user_id,
                user_name,
                mix_id,
                mix_title,
                collect_id,
                collect_name,
            ),
        )
        await self.batch_processing(
            data,
            root,
            tiktok=tiktok,
        )

    async def run_general(
        self,
        data: list[dict],
        tiktok: bool,
        **kwargs,
    ) -> None:
        root = self.storage_folder(mode="detail")
        await self.batch_processing(
            data,
            root,
            tiktok=tiktok,
        )

    async def run_music(
        self,
        data: list[dict],
        **kwargs,
    ) -> None:
        root = self.root.joinpath("Music")
        tasks = []
        for i in data:
            name = self.generate_music_name(i)
            temp_root, actual_root = self.deal_folder_path(
                root,
                name,
                False,
            )
            self.download_music(
                tasks,
                name,
                i["id"],
                i,
                temp_root,
                actual_root,
                "download",
                True,
                type_=_("音乐"),
            )
        await self.downloader_chart(
            tasks, SimpleNamespace(), self.general_progress_object(), **kwargs
        )

    async def run_live(
        self,
        data: list[tuple],
        tiktok: bool = False,
        **kwargs,
    ) -> None:
        if not data or not self.download:
            return
        download_command = []
        self.generate_live_commands(
            data,
            download_command,
        )
        self.console.info(
            _("程序将会调用 ffmpeg 下载直播，关闭 DouK-Downloader 不会中断下载！"),
        )
        self.__download_live(download_command, tiktok)

    def generate_live_commands(
        self,
        data: list[tuple],
        commands: list,
        suffix: str = "mp4",
    ) -> None:
        root = self.root.joinpath("Live")
        root.mkdir(exist_ok=True)
        for i, f, m in data:
            name = self.cleaner.filter_name(
                f"{i['title']}{self.split}{i['nickname']}{self.split}{datetime.now():%Y-%m-%d %H.%M.%S}.{suffix}",
                f"{int(time())}{self.split}{datetime.now():%Y-%m-%d %H.%M.%S}.{suffix}",
            )
            path = root.joinpath(name)
            commands.append(
                (
                    m,
                    str(path.resolve()),
                )
            )

    def __download_live(
        self,
        commands: list,
        tiktok: bool,
    ) -> None:
        self.ffmpeg.download(
            commands,
            self.proxy_tiktok if tiktok else self.proxy,
            self.user_agent_tiktok if tiktok else self.user_agent,
        )

    async def batch_processing(self, data: list[dict], root: Path, **kwargs):
        count = SimpleNamespace(
            downloaded_image=set(),
            skipped_image=set(),
            downloaded_video=set(),
            skipped_video=set(),
            downloaded_live=set(),
            skipped_live=set(),
        )
        tasks = []
        used_names = {}
        # 文件夹名 → 作品 ID：本批已分配出去的作品文件夹，
        # 供 __unique_folder_name 判断不同作品是否撞文件夹名
        used_folders = {}
        # 本批被平台以 403 拒绝的作品（多半是仅朋友可见等权限限制）：
        # 最后写记录时单独提示，和普通“没下完”区分开
        self.forbidden_works = set()
        # 作品 ID →（作者 UID，作者昵称）：下载完成后写记录时顺带记下，
        # 这样主菜单「删除作者下载记录」才能按作者汇总、按作者删除。
        # 旧版本下载的作品记录里没有作者信息，不会出现在作者列表里。
        work_authors = {
            item["id"]: (
                str(item.get("uid") or ""),
                str(item.get("nickname") or ""),
            )
            for item in data
            if item.get("id")
        }
        for item in data:
            item["desc"] = beautify_string(
                item["desc"],
                self.desc_length,
            )
            t = item["type"]
            is_gallery = t == _("图集")
            is_live = t == _("实况")
            is_video = t == _("视频")
            downloads = item.get("downloads") or []
            image_count = (
                len(downloads) if isinstance(downloads, (list, tuple)) else 0
            )
            # 以下载规则设置（主菜单“下载规则设置”）为准：
            # 图集作品的图片数达到 album_threshold（默认 2 张）时，
            # 单独建文件夹并下载背景音乐；门槛设为 1 时单张图片也算
            is_multi_gallery = (
                is_gallery and image_count >= self.album_threshold
            )
            # 实况属于图文类作品，文件本身没有声音，它的声音就是
            # 作品的背景音乐；开关开启时（默认）和多图图集一样单独
            # 建文件夹并把背景音乐一起下载，开关关闭则按原版设置走
            # 纯视频默认直接放根目录、不下载背景音乐；把该开关关掉
            # 时，视频也单独建文件夹并下载背景音乐
            needs_folder_music = (
                is_multi_gallery
                or (is_live and self.live_folder_music)
                or (is_video and not self.video_root_mode)
            )
            if is_gallery or is_video or is_live:
                name = self.generate_work_name(item)
            else:
                name = self.generate_detail_name(item)
            # 同批撞名保护：同一作者同一天发布多个无标题（或同标题）
            # 作品时，拼出的文件名会完全相同，后下的会被误当成
            # “文件已存在”跳过。先到先得保留原名，撞名的后续作品
            # 在名字后追加自己的作品 ID 区分（只影响真撞名的情况）
            if name in used_names and used_names[name] != item["id"]:
                name = f"{name}{self.split}{item['id']}"
            used_names[name] = item["id"]
            if needs_folder_music:
                folder_mode = True
            elif is_video and self.video_root_mode:
                # 纯单视频作品不建子文件夹，直接保存在根目录
                folder_mode = False
            else:
                folder_mode = self.folder_mode
            temp_root, actual_root = self.deal_folder_path(
                root,
                name,
                folder_mode,
                # 只要这个作品要建独立文件夹，文件夹名就按命名开关
                # 拼装（日期/类型/标题/ID），不直接套用完整文件名，
                # 避免 Windows 路径过长导致保存失败；不同作品拼出
                # 同一个文件夹名时自动加标题/作品 ID 区分，保证每个
                # 作品有自己的文件夹
                folder_name=(
                    self.__unique_folder_name(item, name, root, used_folders)
                    if folder_mode
                    else None
                ),
                # 先不创建文件夹：等这个作品确实有文件要下载时再建，
                # 避免作品全部跳过（比如已有下载记录）时留下空文件夹
                create=False,
            )
            tasks_before = len(tasks)
            params = {
                "tasks": tasks,
                "name": name,
                "id_": item["id"],
                "item": item,
                "temp_root": temp_root,
                "actual_root": actual_root,
            }
            if is_gallery:
                await self.download_image(
                    **params,
                    type_=_("图集"),
                    skipped=count.skipped_image,
                )
            elif is_video:
                await self.download_video(
                    **params,
                    type_=_("视频"),
                    skipped=count.skipped_video,
                )
            elif is_live:
                await self.download_image(
                    suffix="mp4",
                    type_=_("实况"),
                    **params,
                    skipped=count.skipped_live,
                )
            else:
                raise DownloaderError
            # 背景音乐的跳过判断：作品有可信的下载记录（媒体文件
            # 齐全，记录未被上面的自愈逻辑删除）时才连音乐一起跳过；
            # 若记录还在、但音乐文件本身缺失（例如旧版本中途退出
            # 留下的状态），仍要把音乐补下。download_music 内部还会
            # 再按音乐文件是否存在判断一次，不会重复下载。
            recorded_now = await self.is_downloaded(item["id"])
            if is_video and self.video_root_mode:
                # 纯单视频作品不下载背景音乐
                pass
            elif needs_folder_music:
                # 图集/实况/视频（按设置）的背景音乐与作品文件保存在
                # 同一个作品文件夹，文件名沿用统一规则，序号排在
                # 全部文件之后（视频本体占序号 1，音乐为序号 2）
                seq = (image_count if not is_video else 1) + 1
                if recorded_now and self.file_exists_any_suffix(
                    actual_root.with_name(f"{name}_{seq}.mp3")
                ):
                    self.log.info(
                        _("【{type}】{name} 已有下载记录，跳过背景音乐").format(
                            type=_("音乐"), name=name
                        )
                    )
                else:
                    self.download_music(
                        **params,
                        type_=_("音乐"),
                        switch=True,
                        seq=seq,
                    )
            elif recorded_now:
                self.log.info(
                    _("【{type}】{name} 已有下载记录，跳过背景音乐").format(
                        type=_("音乐"), name=name
                    )
                )
            else:
                self.download_music(
                    **params,
                    type_=_("音乐"),
                )
            self.download_cover(**params)
            if folder_mode and len(tasks) > tasks_before:
                # 这个作品确实有文件要下载，现在才创建它的文件夹
                actual_root.parent.mkdir(parents=True, exist_ok=True)
        await self.downloader_chart(
            tasks, count, self.general_progress_object(), **kwargs
        )
        await self.__record_completed_works(tasks, work_authors)
        self.statistics_count(count)

    async def __record_completed_works(
        self, tasks: list, work_authors: dict = None
    ) -> None:
        """整批下载结束后统一处理下载记录

        一个作品的全部文件（封面、动图除外）都真实存在于目标
        位置时，才把作品 ID 写入下载记录；只要有文件没下载
        成功，就不写记录（并清掉可能残留的旧记录），下次运行
        时缺失的文件会自动补下。这样程序中途退出不会再造成
        “没下完却被记成已下载”的假记录。
        """
        by_work = {}
        for t in tasks:
            if str(t[3]).startswith(("【封面】", "【动图】")):
                continue
            by_work.setdefault(t[4], []).append(t[2])
        for work_id, paths in by_work.items():
            if all(self.file_exists_any_suffix(i) for i in paths):
                author_uid, author_name = (work_authors or {}).get(
                    work_id, ("", "")
                )
                await self.recorder.update_id(work_id, author_uid, author_name)
            else:
                await self.recorder.delete_id(work_id)
                if work_id in getattr(self, "forbidden_works", set()):
                    self.log.info(
                        _(
                            "作品 {id} 被平台拒绝下载（403），可能是仅朋友可见等权限限制，无法下载"
                        ).format(id=work_id)
                    )
                else:
                    self.log.info(
                        _(
                            "作品 {id} 未全部下载完成，不记入下载记录，下次将自动补下缺失文件"
                        ).format(id=work_id)
                    )

    async def downloader_chart(
        self,
        tasks: list[tuple],
        count: SimpleNamespace,
        progress: Progress,
        semaphore: Semaphore = None,
        **kwargs,
    ):
        with progress:
            tasks = [
                self.request_file(
                    *task,
                    count=count,
                    **kwargs,
                    progress=progress,
                    semaphore=semaphore,
                )
                for task in tasks
            ]
            await gather(*tasks)

    def deal_folder_path(
        self,
        root: Path,
        name: str,
        folder_mode: bool = False,
        folder_name: str = None,
        create: bool = True,
    ) -> tuple[Path, Path]:
        """生成文件的临时路径和目标路径

        create=False 时不在此处创建作品文件夹，由调用方在本作品
        确实产生下载任务后再创建，避免作品全部跳过时留下空文件夹。
        """
        root = self.create_detail_folder(
            root, folder_name or name, folder_mode
        )
        if create:
            root.mkdir(exist_ok=True)
        cache = self.cache.joinpath(name)
        actual = root.joinpath(name)
        return cache, actual

    def generate_folder_name(self, data: dict) -> str:
        """生成作品文件夹名称：按开关拼装日期/类型/标题/ID

        文件夹名不能直接使用完整的作品文件名：作品文件名本身较长
        （日期-类型-作者-标题-作品ID），文件夹里还要再放一层同样
        长的文件名，在 Windows 上整体路径很容易超过系统上限而导致
        保存文件时报“系统找不到指定的路径”。因此文件夹名放哪些
        内容由四个开关决定（folder_show_date/type/title/id，主菜单
        “下载规则设置”里可改），默认只放日期和类型；
        四项全部关闭时兜底使用作品 ID，保证文件夹名不会为空。
        """
        parts = []
        if self.folder_show_date:
            parts.append(self.__work_publish_date(data))
        if self.folder_show_type:
            parts.append(str(data.get("type") or ""))
        if self.folder_show_title:
            parts.append(self.__display_title(data))
        if self.folder_show_id:
            parts.append(str(data.get("id") or ""))
        parts = [i for i in parts if i]
        if not parts:
            parts = [str(data.get("id") or "")]
        return self.__clean_folder_name(self.split.join(parts), data)

    def __clean_folder_name(self, raw: str, data: dict) -> str:
        """文件夹名统一清洗：过滤非法字符并按命名长度上限截断"""
        return beautify_string(
            self.cleaner.filter_name(
                raw,
                str(data.get("id") or ""),
            ),
            length=self.name_length,
        )

    def __unique_folder_name(
        self,
        data: dict,
        name: str,
        root: Path,
        used_folders: dict,
    ) -> str:
        """保证每个作品有自己的文件夹，不同作品不共用一个文件夹

        文件夹名默认只有日期和类型，同一作者同一天的两个同类作品
        （如两条实况）会拼出同一个文件夹名，被塞进同一个文件夹。
        先到先得保留原名；撞名的后续作品先在文件夹名后加标题区
        分，标题也一样（或都是无标题）时再加作品 ID。除了本批已
        分配的文件夹，还要看磁盘上已有的同名文件夹：里面已经有
        别的作品的文件时同样视为撞名，避免分不同批次下载时再次
        合并；本作品自己的文件（补下载、已下载过）都以本作品的
        文件名开头，不会误判。
        """
        base = self.generate_folder_name(data)

        def taken(candidate: str) -> bool:
            owner = used_folders.get(candidate)
            if owner is not None:
                return owner != data["id"]
            folder = root.joinpath(candidate)
            if folder.is_dir():
                return any(
                    entry.is_file() and not entry.name.startswith(name)
                    for entry in folder.iterdir()
                )
            return False

        folder_name = base
        if taken(folder_name):
            candidate = self.__clean_folder_name(
                f"{base}{self.split}{self.__display_title(data)}",
                data,
            )
            if candidate == base or taken(candidate):
                candidate = self.__clean_folder_name(
                    f"{base}{self.split}{data['id']}",
                    data,
                )
            folder_name = candidate
        used_folders[folder_name] = data["id"]
        return folder_name

    async def is_downloaded(self, id_: str) -> bool:
        return await self.recorder.has_id(id_)

    @staticmethod
    def is_exists(path: Path) -> bool:
        return path.exists()

    @staticmethod
    def file_exists_any_suffix(path: Path) -> bool:
        """同名文件是否存在（不看后缀）

        文件真正保存时，后缀以服务器返回的类型为准（request_file
        里用 actual.with_suffix(真实后缀)），可能和计划的后缀不同
        （例如背景音乐计划 .mp3、实际存成 .m4a）。只按计划路径查
        在否，会把已下载成功的文件误判为缺失。
        """
        if path.exists():
            return True
        try:
            return any(path.parent.glob(f"{path.stem}.*"))
        except OSError:
            return False

    async def is_skip(self, id_: str, path: Path) -> bool:
        return await self.is_downloaded(id_) or self.is_exists(path)

    async def download_image(
        self,
        tasks: list,
        name: str,
        id_: str,
        item: SimpleNamespace,
        skipped: set,
        temp_root: Path,
        actual_root: Path,
        suffix: str = "jpeg",
        type_: str = _("图集"),
    ) -> None:
        if not item["downloads"]:
            self.log.error(
                _("【{type}】{name} 提取文件下载地址失败，跳过下载").format(
                    type=type_, name=name
                )
            )
            return
        recorded = await self.is_downloaded(id_)
        added = 0
        missing = 0
        for index, img in enumerate(
            item["downloads"],
            start=1,
        ):
            p = actual_root.with_name(f"{name}_{index}.{suffix}")
            if self.is_exists(p):
                self.log.info(
                    _("【{type}】{name}_{index} 文件已存在，跳过下载").format(
                        type=type_, name=name, index=index
                    )
                )
                self.log.info(f"文件路径: {p.resolve()}", False)
                continue
            missing += 1
            added += 1
            tasks.append(
                (
                    img,
                    temp_root.with_name(f"{name}_{index}.{suffix}"),
                    p,
                    f"【{type_}】{name}_{index}",
                    id_,
                    suffix,
                )
            )
        if recorded and not missing:
            # 有下载记录且文件齐全：整个作品跳过
            skipped.add(id_)
            self.log.info(
                _("【{type}】{name} 存在下载记录，跳过下载").format(
                    type=type_, name=name
                )
            )
        elif recorded and missing:
            # 有下载记录但文件不全：多半是上次下载中途退出留下的
            # 不可信记录。删掉它，只补下缺失的文件；全部补齐后
            # batch_processing 会重新写入记录
            await self.recorder.delete_id(id_)
            self.log.info(
                _("【{type}】{name} 上次没有下载完整，补下缺失的文件").format(
                    type=type_, name=name
                )
            )
        elif not added:
            skipped.add(id_)

    async def download_video(
        self,
        tasks: list[tuple],
        name: str,
        id_: str,
        item: SimpleNamespace,
        skipped: set,
        temp_root: Path,
        actual_root: Path,
        suffix: str = "mp4",
        type_: str = _("视频"),
    ) -> None:
        if not item["downloads"]:
            self.log.error(
                _("【{type}】{name} 提取文件下载地址失败，跳过下载").format(
                    type=type_, name=name
                )
            )
            return
        p = actual_root.with_name(
            f"{name}_1.{suffix}",
        )
        if self.is_exists(p):
            self.log.info(
                _("【{type}】{name} 存在下载记录或文件已存在，跳过下载").format(
                    type=type_, name=name
                )
            )
            self.log.info(f"文件路径: {p.resolve()}", False)
            skipped.add(id_)
            return
        if await self.is_downloaded(id_):
            # 有下载记录但文件不在：上次没有下载完整，删掉不可信
            # 的记录并重新下载
            await self.recorder.delete_id(id_)
            self.log.info(
                _("【{type}】{name} 上次没有下载完整，重新下载").format(
                    type=type_, name=name
                )
            )
        tasks.append(
            (
                item["downloads"],
                temp_root.with_name(f"{name}_1.{suffix}"),
                p,
                f"【{type_}】{name}",
                id_,
                suffix,
            )
        )

    def download_music(
        self,
        tasks: list,
        name: str,
        id_: str,
        item: dict,
        temp_root: Path,
        actual_root: Path,
        key: str = "music_url",
        switch: bool = False,
        suffix: str = "mp3",
        type_: str = _("音乐"),
        seq: int = None,
        **kwargs,
    ) -> None:
        # 传入序号时（图集背景音乐），文件名与其他作品文件保持同一命名规则
        file_name = f"{name}_{seq}" if seq is not None else name
        if switch and not item[key]:
            # 本应下载音乐（例如图集/实况的背景音乐），但平台没有为这个
            # 作品返回可下载的音乐地址；明确提示原因，避免被误认为
            # 程序漏下了音乐
            self.log.warning(
                _(
                    "【{type}】{name} 平台未提供可下载的背景音乐地址，跳过音乐下载"
                ).format(type=type_, name=name)
            )
            return
        if self.check_deal_music(
            url := item[key],
            p := actual_root.with_name(f"{file_name}.{suffix}"),
            switch,
        ):
            tasks.append(
                (
                    url,
                    temp_root.with_name(f"{file_name}.{suffix}"),
                    p,
                    _("【{type}】{name}").format(
                        type=type_,
                        name=name,
                    ),
                    id_,
                    suffix,
                )
            )

    def download_cover(
        self,
        tasks: list,
        name: str,
        id_: str,
        item: SimpleNamespace,
        temp_root: Path,
        actual_root: Path,
        static_suffix: str = "jpeg",
        dynamic_suffix: str = "webp",
        **kwargs,
    ) -> None:
        if all(
            (
                self.static_cover,
                url := item["static_cover"],
                not self.is_exists(
                    p := actual_root.with_name(f"{name}.{static_suffix}")
                ),
            )
        ):
            tasks.append(
                (
                    url,
                    temp_root.with_name(f"{name}.{static_suffix}"),
                    p,
                    f"【封面】{name}",
                    id_,
                    static_suffix,
                )
            )
        if all(
            (
                self.dynamic_cover,
                url := item["dynamic_cover"],
                not self.is_exists(
                    p := actual_root.with_name(f"{name}.{dynamic_suffix}")
                ),
            )
        ):
            tasks.append(
                (
                    url,
                    temp_root.with_name(f"{name}.{dynamic_suffix}"),
                    p,
                    f"【动图】{name}",
                    id_,
                    dynamic_suffix,
                )
            )

    def check_deal_music(
        self,
        url: str,
        path: Path,
        switch: bool = False,
    ) -> bool:
        """未传入 switch 参数则判断音乐下载开关设置"""
        return all(
            (switch or self.music, url, not self.file_exists_any_suffix(path))
        )

    @Retry.retry
    async def request_file(
        self,
        url: str,
        temp: Path,
        actual: Path,
        show: str,
        id_: str,
        suffix: str,
        count: SimpleNamespace,
        progress: Progress,
        headers: dict = None,
        tiktok=False,
        unknown_size=False,
        semaphore: Semaphore = None,
    ) -> bool | None:
        async with semaphore or self.semaphore:
            client = self.client_tiktok if tiktok else self.client
            headers = self.__adapter_headers(
                headers,
                tiktok,
            )
            self.__record_request_messages(
                show,
                url,
                headers,
            )
            try:
                # length, suffix = await self.__head_file(client, url, headers, suffix, )
                position = self.__update_headers_range(
                    headers,
                    temp,
                )
                response = await client.request(
                    "GET",
                    url,
                    headers=headers,
                    stream=True,
                )
                if response.status_code == 416:
                    raise CacheError(_("文件缓存异常，尝试重新下载"))
                response.raise_for_status()
                length, suffix = self._extract_content(
                    response.headers,
                    suffix,
                )
                length += position
                self._record_response(
                    response,
                    show,
                    length,
                )
                match self._download_initial_check(
                    length,
                    unknown_size,
                    show,
                ):
                    case 1:
                        return await self.download_file(
                            temp,
                            actual.with_suffix(
                                f".{suffix}",
                            ),
                            show,
                            id_,
                            response,
                            length,
                            position,
                            count,
                            progress,
                        )
                    case 0:
                        return True
                    case -1:
                        return False
                    case _:
                        raise DownloaderError
            except HTTPError as e:
                self.log.warning(
                    _("响应码异常: {error_repr}").format(error_repr=repr(e))
                )
                if tiktok:
                    self.console.warning(
                        _(
                            "如果 TikTok 平台作品下载功能异常，请检查配置文件中 browser_info_tiktok 的 device_id 参数！"
                        ),
                    )
                elif "403" in str(e):
                    # 本程序只下载抖音，抖音链接被服务器拒绝（403）时
                    # 与 TikTok 无关，不再显示原版那句会误导人的提示。
                    # 403 是服务器明确拒绝，多为链接过期、触发平台
                    # 限制或作品本身有权限限制（如仅朋友可见）：同一
                    # 地址短时间内反复重试不会成功，还可能把限制敲
                    # 得更严，置上 _no_more_retry 让重试器立刻停下，
                    # 并记下作品 ID，最后按权限限制单独提示
                    self._no_more_retry = True
                    if not hasattr(self, "forbidden_works"):
                        self.forbidden_works = set()
                    self.forbidden_works.add(id_)
                    self.console.warning(
                        _(
                            "平台拒绝了这次下载请求（403），可能是链接过期、触发平台限制，或作品有权限限制（如仅朋友可见）。"
                        ),
                    )
                return False
            except RequestException as e:
                self.log.warning(_("网络异常: {error_repr}").format(error_repr=repr(e)))
                return False
            except CacheError as e:
                self.delete(temp)
                self.log.error(str(e))
                return False
            except Exception as e:
                self.log.error(
                    _(
                        "下载文件时发生预期之外的错误，请向作者反馈，错误信息: {error}"
                    ).format(error=repr(e)),
                )
                self.log.error(f"URL: {url}", False)
                self.log.error(f"Headers: {headers}", False)
                return False

    async def download_file(
        self,
        cache: Path,
        actual: Path,
        show: str,
        id_: str,
        response: Response,
        content: int,
        position: int,
        count: SimpleNamespace,
        progress: Progress,
    ) -> bool:
        task_id = progress.add_task(
            beautify_string(show, self.truncate),
            total=content or None,
            completed=position,
        )
        try:
            buffer = bytearray()
            async with open(cache, "ab") as f:
                async for chunk in response.aiter_content(self.chunk):
                    buffer.extend(chunk)
                    if len(buffer) >= self.WRITE_BUFFER_SIZE:
                        await f.write(bytes(buffer))
                        buffer.clear()
                    progress.update(task_id, advance=len(chunk))
                if buffer:
                    await f.write(bytes(buffer))
                progress.remove_task(task_id)
        except RequestException as e:
            progress.remove_task(task_id)
            self.log.warning(
                _("{show} 下载中断，错误信息：{error}").format(show=show, error=e)
            )
            # self.delete_file(cache)
            return False
        self.save_file(cache, actual)
        self.log.info(_("{show} 文件下载成功").format(show=show))
        self.log.info(f"文件路径 {actual.resolve()}", False)
        # 注意：这里不再写入下载记录。以前单个文件下成功就把整个
        # 作品记为已下载，多文件作品只下好第一个文件、程序中途
        # 退出时，剩余文件会被误判为已下载而永远跳过。现在改由
        # batch_processing 在整批下载结束后统一处理：作品的全部
        # 文件都下载成功才写入记录。
        self.add_count(show, id_, count)
        return True

    def __record_request_messages(
        self,
        show: str,
        url: str,
        headers: dict,
    ):
        self.log.info(f"{show} URL: {url}", False)
        # 请求头脱敏处理，不记录 Cookie
        desensitize = {k: v for k, v in headers.items() if k != "Cookie"}
        self.log.info(f"{show} Headers: {desensitize}", False)

    def __adapter_headers(
        self,
        headers: dict,
        tiktok: bool,
        *args,
        **kwargs,
    ) -> dict:
        return (headers or self.headers_tiktok if tiktok else self.headers).copy()

    @staticmethod
    def add_count(show: str, id_: str, count: SimpleNamespace):
        if show.startswith(f"【{_('图集')}】"):
            count.downloaded_image.add(id_)
        elif show.startswith(f"【{_('视频')}】"):
            count.downloaded_video.add(id_)
        elif show.startswith(f"【{_('实况')}】"):
            count.downloaded_live.add(id_)

    @staticmethod
    def data_classification(
        mode: str = "",
        mark: str = "",
        user_id: str = "",
        user_name: str = "",
        mix_id: str = "",
        mix_title: str = "",
        collect_id: str = "",
        collect_name: str = "",
    ) -> tuple[str, str]:
        match mode:
            case "post" | "favorite" | "collection":
                return user_id, mark or user_name
            case "mix":
                return mix_id, mark or mix_title
            case "collects":
                return collect_id, mark or collect_name
            case _:
                raise DownloaderError

    def storage_folder(
        self,
        mode: str = "",
        id_: str = "",
        name: str = "",
    ) -> Path:
        match mode:
            case "post":
                folder_name = _("UID{id_}_{name}_发布作品").format(id_=id_, name=name)
            case "favorite":
                folder_name = _("UID{id_}_{name}_喜欢作品").format(id_=id_, name=name)
            case "mix":
                folder_name = _("MID{id_}_{name}_合集作品").format(id_=id_, name=name)
            case "collection":
                folder_name = _("UID{id_}_{name}_收藏作品").format(id_=id_, name=name)
            case "collects":
                folder_name = _("CID{id_}_{name}_收藏夹作品").format(id_=id_, name=name)
            case "detail":
                folder_name = self.folder_name
            case _:
                raise DownloaderError
        folder = self.root.joinpath(folder_name)
        folder.mkdir(exist_ok=True)
        return folder

    @staticmethod
    def __display_title(data: dict) -> str:
        """文件名/文件夹名里用的标题

        提取数据时，作者没写任何描述的作品，其 desc 被原版逻辑
        兜底成了作品 ID（一串长数字）。命名时把这种情况显示为
        “无标题”，不再把长串 ID 摆在标题的位置上；作品数据本身
        不动，其他地方（日志、统计）不受影响。
        """
        desc = str(data.get("desc") or "")
        if not desc or desc == str(data.get("id") or ""):
            return "无标题"
        return desc

    def generate_detail_name(self, data: dict) -> str:
        """生成作品文件名称"""
        return beautify_string(
            self.cleaner.filter_name(
                self.split.join(
                    self.__display_title(data) if i == "desc" else data[i]
                    for i in self.name_format
                ),
                data["id"],
            ),
            length=self.name_length,
        )

    def generate_work_name(self, data: dict) -> str:
        """生成图集/实况/视频作品文件名称：按开关拼装日期/类型/作者/标题/ID

        组成由五个开关决定（file_show_date/type/author/title/id，
        主菜单“下载规则设置”问题五里可改），与文件夹名的开关分开；
        默认日期、类型、作者、标题开，作品 ID 关（和原项目默认命名
        一致，不带长串数字）。序号由各下载方法在该名称后统一追加
        （图片按张序、视频为 1、图集背景音乐排在全部图片之后），
        始终保留。五项全部关闭时兜底使用作品 ID，保证不为空名。
        """
        parts = []
        if self.file_show_date:
            parts.append(self.__work_publish_date(data))
        if self.file_show_type:
            parts.append(str(data.get("type") or ""))
        if self.file_show_author:
            parts.append(str(data.get("nickname") or data.get("uid") or ""))
        if self.file_show_title:
            parts.append(self.__display_title(data))
        if self.file_show_id:
            parts.append(str(data.get("id") or ""))
        parts = [i for i in parts if i]
        if not parts:
            parts = [str(data.get("id") or "")]
        return beautify_string(
            self.cleaner.filter_name(
                self.split.join(parts),
                str(data.get("id") or ""),
            ),
            length=self.name_length,
        )

    @staticmethod
    def __work_publish_date(data: dict) -> str:
        """提取作品发布日期（年月日）"""
        if timestamp := data.get("create_timestamp"):
            try:
                return datetime.fromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
            except (TypeError, ValueError, OSError):
                pass
        create_time = str(data.get("create_time") or "")
        return create_time[:10]

    def generate_music_name(self, data: dict) -> str:
        """生成音乐文件名称"""
        return beautify_string(
            self.cleaner.filter_name(
                self.split.join(
                    data[i]
                    for i in (
                        "author",
                        "title",
                        "id",
                    )
                ),
                default=str(time())[:10],
            ),
            length=self.name_length,
        )

    @staticmethod
    def create_detail_folder(
        root: Path,
        name: str,
        folder_mode=False,
    ) -> Path:
        return root.joinpath(name) if folder_mode else root

    @staticmethod
    def delete(
        temp: "Path",
    ):
        if temp.is_file():
            temp.unlink()

    @staticmethod
    def save_file(cache: Path, actual: Path):
        move(cache.resolve(), actual.resolve())

    def delete_file(self, path: Path):
        path.unlink()
        self.log.info(_("{file_name} 文件已删除").format(file_name=path.name))

    def statistics_count(self, count: SimpleNamespace):
        self.log.info(
            _("下载视频作品 {downloaded_video_count} 个").format(
                downloaded_video_count=len(count.downloaded_video)
            ),
        )
        self.log.info(
            _("跳过视频作品 {skipped_count} 个").format(
                skipped_count=len(count.skipped_video)
            )
        )
        self.log.info(
            _("下载图集作品 {downloaded_image_count} 个").format(
                downloaded_image_count=len(count.downloaded_image)
            ),
        )
        self.log.info(
            _("跳过图集作品 {skipped_count} 个").format(
                skipped_count=len(count.skipped_image)
            )
        )
        self.log.info(
            _("下载实况作品 {downloaded_image_count} 个").format(
                downloaded_image_count=len(count.downloaded_live)
            ),
        )
        self.log.info(
            _("跳过实况作品 {skipped_count} 个").format(
                skipped_count=len(count.skipped_live)
            )
        )

    def _record_response(
        self,
        response: Response,
        show: str,
        length: int,
    ) -> None:
        self.log.info(f"{show} Response URL: {response.url}", False)
        self.log.info(f"{show} Response Code: {response.status_code}", False)
        self.log.info(f"{show} Response Headers: {response.headers}", False)
        self.log.info(
            f"{show} 文件大小 {format_size(length)}",
            False,
        )

    async def __head_file(
        self,
        client: "AsyncSession",
        url: str,
        headers: dict,
        suffix: str,
    ) -> tuple[int, str]:
        response = await client.head(
            url,
            headers=headers,
        )
        if response.status_code == 405:
            return 0, suffix
        response.raise_for_status()
        return self._extract_content(
            response.headers,
            suffix,
        )

    def _extract_content(
        self,
        headers: dict,
        suffix: str,
    ) -> tuple[int, str]:
        suffix = (
            self.__extract_type(
                headers.get("Content-Type"),
            )
            or suffix
        )
        length = headers.get(
            "Content-Length",
            0,
        )
        return int(length), suffix

    @staticmethod
    def __get_resume_byte_position(file: Path) -> int:
        return file.stat().st_size if file.is_file() else 0

    def __update_headers_range(
        self,
        headers: dict,
        file: Path,
        length: int = 0,
    ) -> int:
        position = self.__get_resume_byte_position(file)
        # if length and position >= length:
        #     self.delete(file)
        #     position = 0
        headers["Range"] = f"bytes={position}-"
        return position

    def __extract_type(self, content: str) -> str:
        if not (s := self.CONTENT_TYPE_MAP.get(content)):
            return self.__unknown_type(content)
        return s

    def __unknown_type(self, content: str) -> str:
        self.log.warning(_("未收录的文件类型：{content}").format(content=content))
        return ""

    def _download_initial_check(
        self,
        length: int,
        unknown_size: bool,
        show: str,
    ) -> int:
        if not length and not unknown_size:  # 响应内容大小判断
            self.log.warning(_("{show} 响应内容为空").format(show=show))
            return -1  # 执行重试
        if all(
            (
                self.max_size,
                length,
                length > self.max_size,
            )
        ):  # 文件下载跳过判断
            self.log.info(_("{show} 文件大小超出限制，跳过下载").format(show=show))
            return 0  # 跳过下载
        return 1  # 继续下载
