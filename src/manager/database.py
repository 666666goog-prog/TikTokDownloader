from asyncio import CancelledError
from contextlib import suppress
from shutil import move

from aiosqlite import Row, connect

from ..custom import VOLUME

__all__ = ["Database"]


class Database:
    __FILE = "DouK-Downloader.db"

    def __init__(
        self,
    ):
        self.file = VOLUME.joinpath(self.__FILE)
        self.database = None
        self.cursor = None

    async def __connect_database(self):
        self.database = await connect(self.file)
        self.database.row_factory = Row
        self.cursor = await self.database.cursor()
        await self.__create_table()
        await self.__write_default_config()
        await self.__write_default_option()
        await self.database.commit()

    async def __create_table(self):
        await self.database.execute(
            """CREATE TABLE IF NOT EXISTS config_data (
            NAME TEXT PRIMARY KEY,
            VALUE INTEGER NOT NULL CHECK(VALUE IN (0, 1))
            );"""
        )
        await self.database.execute(
            "CREATE TABLE IF NOT EXISTS download_data (ID TEXT PRIMARY KEY, AUTHOR_UID TEXT, AUTHOR_NAME TEXT, SOURCE TEXT);"
        )
        # 旧数据库缺少新列时自动补上，老记录不受影响
        # （已存在的列会报错，直接忽略即可）
        for _column in ("AUTHOR_UID TEXT", "AUTHOR_NAME TEXT", "SOURCE TEXT"):
            try:
                await self.database.execute(
                    f"ALTER TABLE download_data ADD COLUMN {_column};"
                )
            except Exception:
                pass
        await self.database.execute("""CREATE TABLE IF NOT EXISTS mapping_data (
        ID TEXT PRIMARY KEY,
        NAME TEXT NOT NULL,
        MARK TEXT NOT NULL
        );""")
        await self.database.execute("""CREATE TABLE IF NOT EXISTS option_data (
        NAME TEXT PRIMARY KEY,
        VALUE TEXT NOT NULL
        );""")

    async def __write_default_config(self):
        await self.database.execute("""INSERT OR IGNORE INTO config_data (NAME, VALUE)
                            VALUES ('Record', 1),
                            ('Logger', 0),
                            ('Disclaimer', 0);""")

    async def __write_default_option(self):
        await self.database.execute("""INSERT OR IGNORE INTO option_data (NAME, VALUE)
                            VALUES ('Language', 'zh_CN');""")

    async def read_config_data(self):
        await self.cursor.execute("SELECT * FROM config_data")
        return await self.cursor.fetchall()

    async def read_option_data(self):
        await self.cursor.execute("SELECT * FROM option_data")
        return await self.cursor.fetchall()

    async def update_config_data(
        self,
        name: str,
        value: int,
    ):
        await self.database.execute(
            "REPLACE INTO config_data (NAME, VALUE) VALUES (?,?)", (name, value)
        )
        await self.database.commit()

    async def update_option_data(
        self,
        name: str,
        value: str,
    ):
        await self.database.execute(
            "REPLACE INTO option_data (NAME, VALUE) VALUES (?,?)", (name, value)
        )
        await self.database.commit()

    async def update_mapping_data(self, id_: str, name: str, mark: str):
        await self.database.execute(
            "REPLACE INTO mapping_data (ID, NAME, MARK) VALUES (?,?,?)",
            (id_, name, mark),
        )
        await self.database.commit()

    async def read_mapping_data(self, id_: str):
        await self.cursor.execute(
            "SELECT NAME, MARK FROM mapping_data WHERE ID=?", (id_,)
        )
        return await self.cursor.fetchone()

    async def has_download_data(self, id_: str) -> bool:
        await self.cursor.execute("SELECT ID FROM download_data WHERE ID=?", (id_,))
        return bool(await self.cursor.fetchone())

    async def write_download_data(
        self,
        id_: str,
        author_uid: str = None,
        author_name: str = None,
        source: str = None,
    ):
        await self.database.execute(
            "INSERT OR IGNORE INTO download_data (ID) VALUES (?);", (id_,)
        )
        # 顺带记下作者：新版下载的作品才有作者信息，旧记录留空
        if author_uid or author_name:
            await self.database.execute(
                "UPDATE download_data SET AUTHOR_UID=?, AUTHOR_NAME=? WHERE ID=?;",
                (author_uid or "", author_name or "", id_),
            )
        # 顺带记下这部作品是从哪个功能下载的（如“账号作品”“收藏作品”），
        # 删除作者记录时据此分开处理，不同来源互不牵连
        if source:
            await self.database.execute(
                "UPDATE download_data SET SOURCE=? WHERE ID=?;",
                (source, id_),
            )
        await self.database.commit()

    async def list_download_authors(self) -> list:
        """按“作者 + 来源”汇总下载记录

        每行是（作者 UID，昵称，来源，作品数）。同一位作者如果既在
        账号作品里下过、又在收藏里下过，会分成两行，删除时各删各的。
        只有新版下载的作品才有作者信息，旧记录不参与汇总。
        """
        await self.cursor.execute(
            "SELECT AUTHOR_UID, MAX(AUTHOR_NAME) AS AUTHOR_NAME, "
            "IFNULL(SOURCE, '') AS SOURCE, COUNT(*) AS CNT "
            "FROM download_data WHERE AUTHOR_UID IS NOT NULL AND AUTHOR_UID != '' "
            "GROUP BY AUTHOR_UID, IFNULL(SOURCE, '') ORDER BY CNT DESC, AUTHOR_NAME;"
        )
        return await self.cursor.fetchall()

    async def delete_download_data_by_author(
        self, author_uid: str, source: str = None
    ) -> int:
        """删掉某一个作者的下载记录，返回删掉的作品数

        source 为 None 时删该作者的全部记录；给了来源时只删那个
        来源下的记录（如只删“账号作品”的，不动“收藏作品”的）。
        """
        if source is None:
            where, args = "AUTHOR_UID=?", (author_uid,)
        else:
            where = "AUTHOR_UID=? AND IFNULL(SOURCE, '')=?"
            args = (author_uid, source)
        await self.cursor.execute(
            f"SELECT COUNT(*) AS CNT FROM download_data WHERE {where};",
            args,
        )
        row = await self.cursor.fetchone()
        deleted = row["CNT"] if row else 0
        await self.database.execute(
            f"DELETE FROM download_data WHERE {where};", args
        )
        await self.database.commit()
        return deleted

    async def delete_download_data(self, ids: list | tuple | str):
        if not ids:
            return
        if isinstance(ids, str):
            ids = [ids]
        [await self.__delete_download_data(i) for i in ids]
        await self.database.commit()

    async def __delete_download_data(self, id_: str):
        await self.database.execute("DELETE FROM download_data WHERE ID=?", (id_,))

    async def delete_all_download_data(self):
        await self.database.execute("DELETE FROM download_data")
        await self.database.commit()

    async def __aenter__(self):
        self.compatible()
        await self.__connect_database()
        return self

    async def close(self):
        with suppress(CancelledError):
            await self.cursor.close()
        await self.database.close()

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    def compatible(self):
        if (
            old := VOLUME.parent.joinpath(self.__FILE)
        ).exists() and not self.file.exists():
            move(old, self.file)
