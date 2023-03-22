from collections import OrderedDict
from functools import cache
import json
from logging import getLogger
import math
import mmap
from pathlib import Path
from typing import Optional

import capnp

capnp.remove_import_hook()
logger = getLogger(__name__)


class PageReader(object):

    def __init__(self, page_path: Path):
        self.page_path = page_path
        self.f = open(self.page_path, "rb")
        self.mm = mmap.mmap(self.f.fileno(), length=0, access=mmap.ACCESS_READ)
        logger.debug("Assign mmap for '{}'.".format(self.page_path))

    def __del__(self):
        logger.debug("Release mmap for '{}'.".format(self.page_path))
        import pdb
        pdb.set_trace()
        if self.mm:
            self.mm.close()

        if self.f:
            self.f.close()


class CapnpManager(object):

    PAGE_SIZE = 500000
    modules = {}

    def __init__(self, dbdir=None):
        if dbdir is None:
            dbdir = Path(__file__).parent / 'capnp_db'
        elif isinstance(dbdir, str):
            dbdir = Path(dbdir)

        if not dbdir.exists():
            dbdir.mkdir(exist_ok=True)

        self.dbdir = dbdir

        self.page_cache = OrderedDict()

    @classmethod
    def load_file(cls, path: str, as_module: str = None):
        if as_module is None:
            as_module = Path(path).name.replace(".", "_")

        cls.modules[as_module] = capnp.load(f"{path}")
        return as_module

    def get_page_mmap(self, page_path: Path):
        if page_path in self.page_cache:
            logger.debug("{} is in the cache.".format(page_path))
            self.page_cache.move_to_end(page_path)
            return self.page_cache[page_path].mm

        new_page_reader = PageReader(page_path)
        self.page_cache[page_path] = new_page_reader
        self.page_cache.move_to_end(page_path)
        logger.debug("Added {} to the cache.".format(page_path))

        if len(self.page_cache) > 10:
            k, v = self.page_cache.popitem(0)
            del v
            logger.debug("{} had been deleted from the cache.".format(k))

        return self.page_cache[page_path].mm


class CapnpTable(CapnpManager):

    def __init__(
            self,
            tablename: str,
            dbdir=None):
        super().__init__(dbdir)
        self.tablename = tablename
        self.readers = {}

    @cache
    def _get_dir(self) -> Path:
        return self.dbdir / self.tablename

    def _get_config_path(self) -> Path:
        return self._get_dir() / "config.json"

    @cache
    def get_config(self) -> Path:
        with open(self._get_config_path(), "r") as f:
            config = json.load(f)

        return config

    def set_config(self, config: dict):
        with open(self._get_config_path(), "w") as f:
            json.dump(config, f)

    def load_capnp_file(self):
        config = self.get_config()
        module_name = config["module_name"]
        if module_name not in CapnpManager.modules:
            CapnpManager.load_file(
                self._get_dir() / config["capnp_file"],
                module_name)

    @cache
    def get_record_type(self):
        config = self.get_config()
        self.load_capnp_file()
        record_type = eval(
            "CapnpManager.modules['" + config["module_name"] + "']."
            + config["record_type"])
        return record_type

    @cache
    def get_list_type(self):
        config = self.get_config()
        self.load_capnp_file()
        list_type = eval("CapnpManager.modules['" +
                         config["module_name"] + "']." + config["list_type"])
        return list_type

    def count(self):
        config = self.get_config()
        return config["length"]

    def _get_page_path(self, pos: int) -> Path:
        """
        Get the path to the page file.

        Paramaters
        ----------
        pos: int
            Position number of the record contained in the page file.

        Returns
        -------
        Path
            Path to the page file.
        """
        page_number = math.floor(pos / self.PAGE_SIZE)
        table_dir = self._get_dir()
        return table_dir / f"page_{page_number:03d}.bin"

    def _write_page(
            self,
            page: int,
            nodes: list):
        target_nodes = nodes[0:self.PAGE_SIZE]
        list_obj = self.get_list_type().new_message()
        nodes_prop = list_obj.init('nodes', len(target_nodes))
        for i, node in enumerate(target_nodes):
            nodes_prop[i] = node

        page_path = self._get_page_path(page * self.PAGE_SIZE)
        with open(page_path, "wb") as f:
            list_obj.write(f)

    def create(
            self,
            capnp_file: str,
            module_name: str,
            record_type: str,
            list_type: str):
        table_dir = self._get_dir()
        if table_dir.exists():
            import shutil
            shutil.rmtree(table_dir)  # remove directory with its contents

        table_dir.mkdir()
        # Copy capnp file
        copied = table_dir / Path(capnp_file).name
        with open(capnp_file, "rb") as fin, open(copied, "wb") as fout:
            fout.write(fin.read())

        with open(self._get_config_path(), "w") as f:
            json.dump(obj={
                "capnp_file": copied.name,
                "module_name": module_name,
                "record_type": record_type,
                "list_type": list_type,
                "length": 0
            }, fp=f)

        return table_dir

    def get_record(
            self,
            pos: int):
        """
        Get a record from the table at pos.

        Parameters
        ----------
        pos: int
            Position of the target record.
        """
        page_path = self._get_page_path(pos=pos)
        mmap = self.get_page_mmap(page_path)
        with self.get_list_type().from_bytes(
                buf=mmap, traversal_limit_in_words=2**64-1) as list_obj:
            return list_obj.nodes[pos % self.PAGE_SIZE]

    def _get_page_reader(self, pos: int):
        return PageReader(table=self, pos=pos)

    def retrieve_records(
            self,
            limit: Optional[int] = None,
            offset: Optional[int] = None) -> list:
        """
        Get a generator that retrieves records from the table.

        Paramaters
        ----------
        limit: int, optional
            Max number of records to be retrieved.
            If omitted, all records are retrieved.
        offset: int, optional
            Specifies the number of records to be retrieved from.
            If omitted, the retrieval is performed from the beginning.

        Returns
        -------
        A record object of the table.
        """
        if limit is None:
            limit = self.count()

        offset = 0 if offset is None else offset

        f = None
        mm = None
        list_obj = None

        pos = offset
        while pos < offset + limit:
            page_path = self._get_page_path(pos)
            current_path = page_path
            f = open(current_path, "rb")
            mm = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ)
            with self.get_list_type().from_bytes(
                    buf=mm, traversal_limit_in_words=2**64-1) as list_obj:

                while pos < offset + limit:
                    page_path = self._get_page_path(pos)
                    if page_path != current_path:
                        break

                    yield list_obj.nodes[pos % self.PAGE_SIZE]
                    pos += 1

            mm.close()
            f.close()

    def append_records(
            self,
            nodes: list) -> bool:
        """
        Appends a record to the end of the table.

        Paramaters
        ----------
        nodes: list
            The list of nodes.
        """
        new_pos = self.count()

        page_path = self._get_page_path(new_pos)
        page = math.floor(new_pos / self.PAGE_SIZE)
        pos = page * self.PAGE_SIZE
        if new_pos - pos > 0:
            with open(page_path, "rb") as f:
                list_obj = self.get_list_type().read(f)

            self._write_page(
                page=page,
                nodes=(list_obj.nodes + nodes)[:self.PAGE_SIZE])

            new_nodes = nodes[self.PAGE_SIZE - len(list_obj.nodes):]
            page += 1
        else:
            new_nodes = nodes[:]

        while len(new_nodes) > 0:
            self._write_page(
                page=page,
                nodes=new_nodes[0:self.PAGE_SIZE])

            new_nodes = new_nodes[self.PAGE_SIZE:]
            page += 1

        config = self.get_config()
        config["length"] += len(nodes)
        self.set_config(config)

    def update_records(
            self,
            updates: dict) -> bool:
        """
        Updates records in a table that has already been output to a file.

        Paramaters
        ----------
        updates: dict
            A dict whose keys are the positions of records to be updated and
            whose values are the contents to be updated.

            The format of the valuse are a dict of field name/value pairs
            to be updated.

        Notes
        -----
        - This process is very slow and should not be called if possible.
        """
        current_page = None
        nodes = None
        updates = dict(sorted(updates.items()))

        for pos, new_value in updates.items():
            page = math.floor(pos / self.PAGE_SIZE)
            if page != current_page:
                if current_page is not None:
                    # Write the page to file
                    self._write_page(
                        page=current_page,
                        nodes=nodes
                    )

                # Read the page into memory
                page_path = self._get_page_path(pos)
                with open(page_path, "rb") as f:
                    current_page = page
                    list_obj = self.get_list_type().read(
                        f, traversal_limit_in_words=2**64 - 1)
                    nodes = [node.as_builder() for node in list_obj.nodes]
                    # nodes = list_obj.nodes

            for key, value in new_value.items():
                setattr(
                    nodes[pos % self.PAGE_SIZE],
                    key, value)

        if current_page is not None:
            # Write the page to file
            self._write_page(
                page=current_page,
                nodes=nodes
            )
