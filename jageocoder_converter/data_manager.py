import bz2
from contextlib import contextmanager
import csv
import glob
import heapq
import io
import json
from logging import getLogger
import os
import re
import tempfile
from typing import Union, Optional, List
import zipfile


from jageocoder.address import AddressLevel
from jageocoder.aza_master import AzaMaster
from jageocoder.dataset import Dataset
from jageocoder.itaiji import converter as itaiji_converter
from jageocoder.tree import AddressTree
from jageocoder.trie import AddressTrie, TrieNode
from jageocoder.node import AddressNode, AddressNodeTable

logger = getLogger(__name__)


class DataManager(object):
    """
    Manager class to register the converted formatted text data
    into the database.

    Attributes
    ----------
    db_dir: PathLike object
        The directory path where the database files will be located.
    text_dir: PathLike object
        The directory path where the text data is located.
    targets: list[str]
        List of prefecture codes (JISX0401) to be processed.
    """

    # Regular expression
    re_float = re.compile(r'^\-?\d+\.?\d*$')
    re_address = re.compile(r'^([^;]+);(\d+)$')
    re_name_level = re.compile(r'([^!]*?);(\d+),')

    def __init__(self,
                 db_dir: Union[str, bytes, os.PathLike],
                 text_dir: Union[str, bytes, os.PathLike],
                 targets: Optional[List[str]] = None) -> None:
        """
        Initialize the manager.

        Parameters
        ----------
        db_dir: PathLike object
            The directory path where the database files will be located.
        text_dir: PathLike object
            The directory path where the text data is located.
        targets: list[str]
            List of prefecture codes (JISX0401) to be processed.
            If omitted, all prefectures will be processed.
        """

        self.db_dir = db_dir
        self.text_dir = text_dir
        self.targets = targets
        self.targets = targets
        if self.targets is None:
            self.targets = ['{:02d}'.format(x) for x in range(1, 48)]

        os.makedirs(self.db_dir, mode=0o755, exist_ok=True)

        self.tmp_text = None
        self.tree = AddressTree(db_dir=self.db_dir, mode='w')
        self.aza_master = AzaMaster(db_dir=self.db_dir)
        # self.engine = self.tree.engine
        # self.session = self.tree.session

    def write_datasets(self, converters: list) -> None:
        """
        Write dataset metadata to 'dataset' table.
        """
        datasets = Dataset(db_dir=self.db_dir)
        datasets.create()
        records = [{
            "id": 0,
            "title": "住所変更履歴",
            "url": "https://github.com/t-sagara/analyze_city_history",
        }]
        for converter in converters:
            records.append({
                "id": converter.priority,
                "title": converter.dataset_name,
                "url": converter.dataset_url,
            })

        datasets.append_records(records)

    def register(self) -> None:
        """
        Process prefectures in the target list.

        - Open a new temp file,
        - Read records from text files, sort them,
          and output them to the temp file,
        - Then, write them to the database.
        """
        # Initialize AddressNode table
        self.address_nodes = AddressNodeTable(db_dir=self.db_dir)
        self.address_nodes.create()

        # Initialize variables over prefectures
        self.root_node = AddressNode.root()
        self.cur_id = self.root_node.id
        self.node_array = [self.root_node.to_record()]

        # Register from files
        for prefcode in self.targets:
            logger.info(f"Converting text files for {prefcode}")
            self.open_tmpfile()
            self.sort_data(prefcode=prefcode)
            self.write_database()

        if len(self.node_array) > 0:
            self.address_nodes.append_records(self.node_array)

    def create_index(self) -> None:
        """
        Create codes and TRIE index from the tree.
        """
        # Create other tables
        logger.info("Creating note index...")
        self.tree.create_note_index_table()
        logger.info("Creating TRIE index...")
        self.create_trie_index()

    def open_tmpfile(self) -> None:
        """
        Create a temporary file to store the sorted text.
        If it has already been created, delete it and create a new one.
        """
        if self.tmp_text:
            self.tmp_text.close()

        self.tmp_text = tempfile.TemporaryFile(mode='w+t')

    def sort_data(self, prefcode: str) -> None:
        """
        Read records from text files that matches the specified
        prefecture code, sort the records,
        and output them to the temp file.

        Parameters
        ----------
        prefcode: str
            The target prefecture code (JISX0401).
        """

        def sort_save_chunk(lines: List[str]) -> os.PathLike:
            lines.sort()
            tmpf = tempfile.NamedTemporaryFile(delete=False, mode='w')
            tmpf.writelines(lines)
            tmpf.close()
            return tmpf.name

        logger.info('Sorting text data in {}'.format(
            os.path.join(self.text_dir, prefcode + '_*.txt.bz2')))

        # Write chunked data to tempfiles
        temp_files = []
        lines = []
        size = 0
        for filename in glob.glob(
                os.path.join(self.text_dir, prefcode + '_*.txt.bz2')):
            logger.info("   ... reading '{}'".format(
                os.path.basename(filename)
            ))
            with bz2.open(filename, mode='rt') as fin:
                for line in fin:
                    if line[0] == '#':  # Skip as comment
                        continue

                    names = self.re_name_level.findall(line)
                    newline = " ".join([
                        itaiji_converter.standardize(x[0]) + f";{x[1]}"
                        for x in names
                    ]) + f"\t{line}"
                    chunk = newline.encode(encoding='utf-8')
                    lines.append(newline)
                    size += len(chunk)
                    if size >= 100 * 1024 * 1024:  # 100MB
                        temp_files.append(sort_save_chunk(lines))
                        lines.clear()
                        size = 0

        if lines:
            temp_files.append(sort_save_chunk(lines))

        # Merge sort
        logger.info("   ... merging {} chunks".format(len(temp_files)))
        fins = [open(fname, 'r') for fname in temp_files]
        self.tmp_text.writelines(heapq.merge(*fins))
        for f in fins:
            f.close()

        # Remove tempfiles
        for fname in temp_files:
            os.remove(fname)

    def write_database(self) -> None:
        """
        Generates records that can be output to a database
        from sorted and formatted text in the temporary file,
        and bulk inserts them to the database.
        """
        logger.info('Building nodes tables.')
        # Initialize variables valid in a prefecture
        self.tmp_text.seek(0)
        self.nodes = {}
        self.prev_key = ''
        # self.buffer = []
        self.update_array = {}

        # Read all texts for the prefecture
        reader = csv.reader(self.tmp_text)
        for args in reader:
            if "\t" not in args[0]:
                print(args)
                raise RuntimeError("Tab is not found in the sorted text!")

            keys, arg0 = args[0].split("\t")
            args[0] = arg0
            self.process_line(args, keys.split(" "))

        if len(self.nodes) > 0:
            for key, target_id in self.nodes.items():
                res = self._set_sibling(target_id, self.cur_id + 1)
                if res is False:
                    logger.debug(
                        "{}[{}] -> EOF[{}]".format(
                            key, target_id, self.cur_id + 1))

            self.nodes.clear()

        if len(self.update_array) > 0:
            logger.debug("Updating missed siblings.")
            self.address_nodes.update_records(self.update_array)

    def get_next_id(self):
        """
        Get the next serial id.
        """
        self.cur_id += 1
        return self.cur_id

    def process_line(
        self,
        args: List[str],
        keys: List[str],
    ) -> None:
        """
        Processes a single line of data.

        Parameters
        ----------
        args: List[str]
            Arguments in a line of formatted text data,
            including names of address elements, x and y values,
            and notes.
        keys: List[str]
            List of standardized address elements.
        """
        try:
            if self.re_float.match(args[-1]) and \
                    self.re_float.match(args[-2]):
                names = args[0:-2]
                x = float(args[-2])
                y = float(args[-1])
                note = None
            else:
                names = args[0:-3]
                x = float(args[-3])
                y = float(args[-2])
                note = str(args[-1])
        except ValueError as e:
            logger.debug(str(e) + "; args = '{}'".format(args))
            raise e

        if names[-1][0] == '!':
            priority = int(names[-1][1:])
            names = names[0:-1]

        self.add_elements(
            keys=keys,
            names=names,
            x=x, y=y,
            note=note,
            priority=priority)

    def add_elements(
            self,
            keys: List[str],
            names: List[str],
            x: float,
            y: float,
            note: Optional[str],
            priority: Optional[int]) -> None:
        """
        Format the address elements into a form that can be registered
        in the database. The parent_id is also calculated and assigned.

        Parameters
        ----------
        keys: [str]
            Standardized names of the address element.
        names: [str]
            Names of the address element.
        x: float
            The X value (longitude)
        y: float
            The Y value (latitude)
        note: str, optional
            Note
        priority: int, optional
            Source priority of this data.
        """

        def gen_key(names: List[str]) -> str:
            return ','.join(names)

        # Check duprecate addresses.
        key = gen_key(keys)
        if key in self.nodes:
            # logger.debug("Skip duprecate record: {}".format(key))
            return

        # Delete unnecessary cache.
        if not key.startswith(self.prev_key):
            for k, target_id in self.nodes.items():
                if not key.startswith(k) or \
                        (len(key) > len(k) and key[len(k)] != ','):
                    res = self._set_sibling(target_id, self.cur_id + 1)
                    if res is False:
                        logger.debug((
                            "Cant set siblingId {}[{}] to {}[{}]."
                            "(Update it by calling 'update_records' later)"
                        ).format(
                            key, self.cur_id + 1, k, target_id)
                        )

                    self.nodes[k] = None

            self.nodes = {
                k: v
                for k, v in self.nodes.items() if v is not None
            }

        # Add unregistered address elements to the buffer
        parent_id = self.root_node.id
        for i, name in enumerate(names):
            key = gen_key(keys[0:i + 1])
            if key in self.nodes:
                parent_id = self.nodes[key]
                continue

            m = self.re_address.match(name)
            name = m.group(1)
            level = m.group(2)
            new_id = self.get_next_id()
            # itaiji_converter.standardize(name)
            name_index = keys[i][0: keys[i].find(";")]

            node = AddressNode(
                id=new_id,
                name=name,
                name_index=name_index,
                x=x,
                y=y,
                level=level,
                priority=priority,
                note=note if i == len(names) - 1 else "",
                parent_id=parent_id
            )
            self.node_array.append(node.to_record())

            while len(self.node_array) >= self.address_nodes.PAGE_SIZE:
                self.address_nodes.append_records(self.node_array)
                self.node_array = self.node_array[
                    self.address_nodes.PAGE_SIZE:]

            self.nodes[key] = new_id
            self.prev_key = key
            parent_id = new_id

    def _set_sibling(self, target_id: int, sibling_id: int) -> bool:
        """
        Set the siblingId of the Capnp record afterwards.

        Parameters
        ----------
        target_id: int
            'id' of the record for which siblingId is to be set.
        sibling_id: int
            Value of siblingId to be set for the record.

        Returns
        -------
        bool
            Returns False if the target record has already been output
            to a file and cannot be changed, or True if it can be changed.
        """
        if len(self.node_array) == 0 or self.node_array[0]["id"] > target_id:
            if target_id not in self.update_array:
                self.update_array[target_id] = {}

            self.update_array[target_id]["siblingId"] = self.cur_id + 1
            return False
        else:
            pos = target_id - self.node_array[0]["id"]
            self.node_array[pos]["siblingId"] = sibling_id
            return True

    def prepare_aza_table(self, download_dir):
        """
        Read 'mt_town_all.csv.zip' and register to 'aza_master' table.
        """
        logger.debug("Creating aza_master table...")
        zipfilepath = os.path.join(download_dir, 'mt_town_all.csv.zip')
        if not os.path.exists(zipfilepath):
            raise RuntimeError(f"Can't open {zipfilepath}.")

        # Initialize Capnp table
        self.aza_master = AzaMaster(db_dir=self.db_dir)
        self.aza_master.create()

        records = {}
        with self.open_csv_in_zipfile(zipfilepath) as ft:
            reader = csv.DictReader(ft)
            n = 0
            aza_codes = {}
            for row in reader:
                if row["lg_code"][0:2] not in self.targets:
                    continue

                record = self.aza_master.from_csvrow(row)
                if record["code"] not in aza_codes:
                    aza_codes[record["code"]] = record

                n += 1
                if n % 10000 == 0:
                    logger.debug("  read {} records.".format(n))
                    # self.manager.session.commit()

            records = dict(sorted(aza_codes.items()))
            self.aza_master.append_records(records.values())

        # Create TRIE index
        self.aza_master.create_trie_on(attr="code")
        self.aza_master.create_trie_on(
            attr="names",
            key_func=lambda x: AzaMaster.standardize_aza_name(
                json.loads(x)
            )
        )

    @contextmanager
    def open_csv_in_zipfile(self, zipfilepath: Union[str, os.PathLike]):
        """
        Get file pointer to the first csv file in the zipfile.

        Parameters
        ----------
        zipfilepath: PathLike
            Path to the target zipfile.
        """
        if not os.path.exists(zipfilepath):
            yield None
            return

        with zipfile.ZipFile(zipfilepath) as z:
            for filename in z.namelist():
                if filename.lower().endswith('.csv'):
                    with z.open(filename, mode='r') as f:
                        ft = io.TextIOWrapper(
                            f, encoding='utf-8', newline='',
                            errors='backslashreplace')
                        logger.debug(
                            "Opening csvfile {} in zipfile {}.".format(
                                filename, zipfilepath))
                        yield ft

                elif filename.lower().endswith('.zip'):
                    with tempfile.NamedTemporaryFile("w+b") as nt:
                        with z.open(filename, mode='r') as f:
                            nt.write(f.read())

                        logger.debug(
                            "Copied zipfile {} to tmpfile {}.".format(
                                filename, nt.name))

                        with self.open_csv_in_zipfile(nt.name) as ft:
                            yield ft

    def create_trie_index(self) -> None:
        """
        Create the TRIE index from the tree.
        """
        self.index_table = {}
        logger.debug("Collecting labels for the trie index...")
        self._get_index_table()
        self._extend_index_table()

        logger.debug("Building Trie...")
        self.tree.trie = AddressTrie(self.tree.trie_path, self.index_table)
        self.tree.trie.save()

        records = self._set_index_table()
        self.index_table.clear()

        # Create and write TrieNode table
        self.tree.trie_nodes = TrieNode(db_dir=self.tree.db_dir)
        self.tree.trie_nodes.create()
        self.tree.trie_nodes.append_records(records)

    def _get_index_table(self) -> None:
        """
        Collect the names of all address elements
        to be registered in the TRIE index.
        The collected values are stored temporaly in `self.index_table`.

        Generates notations that describe everything from the name of
        the prefecture to the name of the oaza without abbreviation,
        notations that omit the name of the prefecture, or notations
        that omit the name of the prefecture and the city.
        """
        tree = self.tree

        # Build temporary lookup table
        logger.debug("Building temporary lookup table..")
        tmp_id_name_table = {}
        pos = AddressNode.ROOT_NODE_ID + 1
        while pos < tree.address_nodes.count_records():
            node = tree.address_nodes.get_record(pos=pos)
            if node.level <= AddressLevel.OAZA:
                tmp_id_name_table[node.id] = node
                if node.level < AddressLevel.OAZA:
                    pos += 1
                else:
                    pos = node.sibling_id

            else:
                parent = tree.address_nodes.get_record(pos=node.parent_id)
                if parent.level < AddressLevel.OAZA:
                    pos += 1
                else:
                    pos = parent.sibling_id

                continue

        logger.debug("  {} records found.".format(
            len(tmp_id_name_table)))

        # Create index_table
        self.index_table = {}
        for k, v in tmp_id_name_table.items():
            node_prefixes = []
            cur_node = v
            while True:
                node_prefixes.insert(0, cur_node.name)
                if cur_node.parent_id == AddressNode.ROOT_NODE_ID:
                    break

                if cur_node.parent_id not in tmp_id_name_table:
                    raise RuntimeError(
                        ('The parent_id:{} of node:{} is not'.format(
                            cur_node.parent_id, cur_node),
                         ' in the tmp_id_table'))

                cur_node = tmp_id_name_table[cur_node.parent_id]

            for i in range(len(node_prefixes)):
                label = ''.join(node_prefixes[i:])
                label_standardized = tree.converter.standardize(
                    label)
                if label_standardized in self.index_table:
                    self.index_table[label_standardized].append(v.id)
                else:
                    self.index_table[label_standardized] = [v.id]

            # Also register variant notations for node labels
            for candidate in tree.converter.standardized_candidates(
                    v.name_index):
                if candidate == v.name_index:
                    # The original notation has been already registered
                    continue

                if candidate in self.index_table:
                    self.index_table[candidate].append(v.id)
                else:
                    self.index_table[candidate] = [v.id]

    def _extend_index_table(self) -> None:
        """
        Expand the index, including support for omission of county names.
        """
        tree = self.tree

        # Build temporary lookup table
        logger.debug("Building temporary town and village table..")
        tmp_id_name_table = {}
        pos = AddressNode.ROOT_NODE_ID + 1
        while pos < tree.address_nodes.count_records():
            node = tree.address_nodes.get_record(pos=pos)
            if node.level <= AddressLevel.CITY:
                tmp_id_name_table[node.id] = node
                pos += 1
            else:
                parent = tree.address_nodes.get_record(pos=node.parent_id)
                pos = parent.sibling_id
                continue

        logger.debug("  {} records found.".format(
            len(tmp_id_name_table)))

        # Extend index_table
        aliases_json = os.path.join(
            os.path.dirname(__file__), "data/aliases.json"
        )
        with open(aliases_json) as f:
            aliases = json.load(f)

        for k, v in tmp_id_name_table.items():
            if v.parent_id == AddressNode.ROOT_NODE_ID:
                continue

            alternatives = []
            parent_node = tmp_id_name_table[v.parent_id]
            if parent_node.level == AddressLevel.PREF:
                parents = [parent_node.name]
            else:
                pref_node = tmp_id_name_table[parent_node.parent_id]
                parents = [pref_node.name, parent_node.name]

            if v.name in aliases:
                for candidate in aliases[v.name]:
                    for i in range(len(parents) + 1):
                        alternatives.append(parents[i:] + [candidate])

            if len(parents) > 1:
                alternatives.append([parents[0], v.name])
                if v.name in aliases:
                    for candidate in aliases[v.name]:
                        alternatives.append([parents[0], candidate])

            for alternative in alternatives:
                logger.debug("Extend index by adding '{}'".format(
                    '/'.join(alternative)))
                label = "".join(alternative)
                label_standardized = tree.converter.standardize(label)
                if label_standardized in self.index_table:
                    self.index_table[label_standardized].append(v.id)
                else:
                    self.index_table[label_standardized] = [v.id]

    def _set_index_table(self) -> list:
        """
        Map all the id of the TRIE index (TRIE id) to the node id.

        Collect notations recursively the names of all address elements
        which was registered in the TRIE index, retrieve
        the id of each notations in the TRIE index,
        then add the TrieNode to the database that maps
        the TRIE id to the node id.
        """
        tree = self.tree

        logger.debug("Creating mapping table from trie_id:node_id")
        trie_nodes = []
        for k, node_id_list in self.index_table.items():
            trie_id = tree.trie.get_id(k)
            if len(trie_nodes) <= trie_id:
                trie_nodes += [None for _ in range(
                    trie_id - len(trie_nodes) + 1)]

            trie_nodes[trie_id] = {
                "id": trie_id,
                "nodes": node_id_list,
            }

        return trie_nodes
