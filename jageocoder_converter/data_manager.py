import csv
import glob
import io
from logging import getLogger
import os
import re
import tempfile
from typing import Union, NoReturn, Optional, List

from jageocoder import AddressTree
from jageocoder.node import AddressNode
from jageocoder.itaiji import converter as itaiji_converter

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
    re_address = re.compile(r'^(\d+);(.*)$')

    def __init__(self,
                 db_dir: Union[str, bytes, os.PathLike],
                 text_dir: Union[str, bytes, os.PathLike],
                 targets: Optional[List[str]] = None) -> NoReturn:
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
        self.tree.create_db()
        self.engine = self.tree.engine
        self.root_node = self.tree.get_root()
        self.nodes = {}
        self.cur_id = self.root_node.id
        self.prev_names = []

        # Register the root node to the database
        self.engine.execute(
            AddressNode.__table__.insert(),
            [{'id': -1, 'name': '_root_', 'name_index': '_root_'}])

    def register(self) -> NoReturn:
        """
        Process prefectures in the target list.

        - Open a new temp file,
        - Read records from text files, sort them,
          and output them to the temp file,
        - Then, write them to the database.
        """
        for prefcode in self.targets:
            self.open_tmpfile()
            self.sort_data(prefcode=prefcode)
            self.write_database()

    def create_index(self) -> NoReturn:
        """
        Create relational index and trie index.
        """
        self.tree.create_tree_index()
        self.tree.create_trie_index()

    def open_tmpfile(self) -> NoReturn:
        """
        Create a temporary file to store the sorted text.
        If it has already been created, delete it and create a new one.
        """
        if self.tmp_text:
            self.tmp_text.close()

        self.tmp_text = tempfile.TemporaryFile(mode='w+b')

    def sort_data(self, prefcode: str) -> NoReturn:
        """
        Read records from text files that matches the specified
        prefecture code, sort the records,
        and output them to the temp file.

        Parameters
        ----------
        prefcode: str
            The target prefecture code (JISX0401).
        """
        logger.info('Sorting text data in {}'.format(
            os.path.join(self.text_dir, prefcode + '_*.txt')))
        records = []
        for filename in glob.glob(
                os.path.join(self.text_dir, prefcode + '_*.txt')):
            with open(filename, mode='rb') as fb_in:
                for line in fb_in:
                    records.append(line)

        records.sort()
        for record in records:
            self.tmp_text.write(record)

    def write_database(self) -> NoReturn:
        """
        Generates records that can be output to a database
        from sorted and formatted text in the temporary file,
        and bulk inserts them to the database.
        """
        self.tmp_text.seek(0)
        self.buffer = []
        fp = io.TextIOWrapper(self.tmp_text, encoding='utf-8', newline='')
        reader = csv.reader(fp)
        for args in reader:
            self.process_line(args)

        if len(self.buffer) > 0:
            self.engine.execute(
                AddressNode.__table__.insert(),
                self.buffer)

    def get_next_id(self):
        """
        Get the next serial id.
        """
        self.cur_id += 1
        return self.cur_id

    def process_line(self, args: List[str]) -> NoReturn:
        """
        Processes a single line of data.

        Parameters
        ----------
        args: [str]
            Arguments in a line of formatted text data,
            including names of address elements, x and y values,
            and notes.
        """
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

        if names[-1][0] == '!':
            names = names[0:-1]

        self.add_elements(names, x, y, note)

    def add_elements(self, names: List[str], x: float, y: float,
                     note: Union[str, None]) -> NoReturn:
        """
        Format the address elements into a form that can be registered
        in the database. The parent_id is also calculated and assigned.

        Parameters
        ----------
        names: [str]
            Names of the address element.
        x: float
            The X value (longitude)
        y: float
            The Y value (latitude)
        note: str, optional
            Note
        """

        # Delete unnecessary cache.
        if len(names) <= len(self.prev_names):
            for i in range(len(names) - 1, len(self.prev_names)):
                key = ','.join(self.prev_names[0:i+1])
                del self.nodes[key]

        # Add unregistered address elements to the buffer
        parent_id = self.root_node.id
        for i, name in enumerate(names):
            key = ','.join(names[0:i + 1])
            if key in self.nodes:
                parent_id = self.nodes[key]
                continue

            m = self.re_address.match(name)
            level = m.group(1)
            name = m.group(2)
            new_id = self.get_next_id()
            name_index = itaiji_converter.standardize(name)

            values = {
                'id': new_id,
                'name': name,
                'name_index': name_index,
                'x': x,
                'y': y,
                'level': level,
                'note': note,
                'parent_id': parent_id,
            }

            self.buffer.append(values)
            self.nodes[key] = new_id
            self.prev_names = names
            parent_id = new_id


if __name__ == '__main__':
    manager = DataManager(
        db_dir='dbtest',
        text_dir='output',
        targets=['13'])
    manager.register()
    manager.create_index()
