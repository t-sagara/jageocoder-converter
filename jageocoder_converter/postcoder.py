import csv
from functools import lru_cache
import io
from logging import getLogger
import os
import re
from typing import Union, NoReturn
import zipfile

import marisa_trie
from jageocoder.address import AddressLevel
from jageocoder.itaiji import converter
from jageocoder.strlib import strlib

from jageocoder_converter.base_converter import BaseConverter


logger = getLogger(__name__)


class PostCoder(BaseConverter):

    re_range = re.compile(r'(.*)（([０-９]+)～([０-９]+)(.*)）')
    postcoder = None

    def __init__(self,
                 input_dir: Union[str, bytes, os.PathLike]):
        self.input_dir = input_dir

        self.addresses = []
        self.codes = {}
        self.trie = None

    def load_file(self) -> NoReturn:
        """
        Read 'ken_all.csv'
        """
        self.prepare_jiscode_table()
        url = 'https://www.post.japanpost.jp/zipcode/dl/kogaki/zip/ken_all.zip'

        zipfilepath = os.path.join(
            self.input_dir, 'ken_all.zip')
        if not os.path.exists(zipfilepath):
            self.download(
                urls=[url],
                dirname=self.input_dir
            )

        with zipfile.ZipFile(zipfilepath) as z:
            # 0: citycode, 01101,
            # 1: postalcode 3digits, "060  ",
            # 2: postalcode 7digits, "0600000",
            # 3: prefecture yomi, "ホッカイドウ",
            # 4: city yomi, "サッポロシチュウオウク",
            # 5: oaza yomi, "イカニケイサイガナイバアイ",
            # 6: prefecture, "北海道",
            # 7: city, "札幌市中央区",
            # 8: oaza, "以下に掲載がない場合",
            # 0,0,0,0,0,0
            for filename in z.namelist():
                if filename.lower() != 'ken_all.csv':
                    continue

                with z.open(filename, mode='r') as f:
                    ft = io.TextIOWrapper(
                        f, encoding='CP932', newline='',
                        errors='backslashreplace')
                    reader = csv.reader(ft)
                    logger.debug('Processing {} in {}...'.format(
                        filename, zipfilepath))
                    pre_args = []
                    try:
                        for args in reader:
                            citycode, postalcode, oaza = \
                                args[0], args[2], args[8]
                            if '掲載がない' in oaza:
                                oaza = ''

                            cityname = ''.join([
                                x[1] for x in self.jiscodes[citycode]])

                            m = self.re_range.match(oaza)
                            if m:
                                head = m.group(1)
                                from_number = strlib.get_number(m.group(2))
                                to_number = strlib.get_number(m.group(3))
                                tail = m.group(4)

                                for number in range(
                                        from_number['n'], to_number['n'] + 1):
                                    oaza = head + str(number) + tail
                                    self._register_oaza(
                                        address=cityname + oaza,
                                        postalcode=postalcode)

                            else:
                                self._register_oaza(
                                    address=cityname + oaza,
                                    postalcode=postalcode)

                            pre_args = args

                    except UnicodeDecodeError:
                        raise RuntimeError((
                            "変換できない文字が見つかりました。"
                            "処理中のファイルは {}, "
                            "直前の行は次の通りです。\n{}").format(
                                filename, pre_args))

        self.trie = marisa_trie.Trie(self.addresses)

    def _register_oaza(self, address: str, postalcode: str) -> NoReturn:
        standardized = converter.standardize(address)
        self.addresses.append(standardized)
        self.codes[standardized] = postalcode
        # logger.debug('{} -> {}'.format(address, postalcode))

    @lru_cache
    def search(self, address: str) -> Union[str, None]:
        standardized = converter.standardize(address)
        prefixes = self.trie.prefixes(standardized)
        if len(prefixes) == 0:
            return None

        prefixes.sort(key=lambda prefix: len(prefix), reverse=True)
        longest_prefix = prefixes[0]

        return self.codes[longest_prefix]

    def search_by_list(self, nodes: list) -> Union[str, None]:
        address_str = ''
        city_str = ''
        for node in nodes:
            if node[0] <= AddressLevel.WARD:
                city_str += node[1]

            if node[0] == AddressLevel.OAZA and \
                    node[1].startswith('大字'):
                address_str += node[1][2:]
            elif node[0] == AddressLevel.AZA and \
                    node[1].startswith('字'):
                address_str += node[1][1:]
            else:
                address_str += node[1]

        code = self.search(address_str)
        if nodes[-1][0] <= AddressLevel.WARD:
            return code

        by_citylevel = self.search(city_str)
        if by_citylevel == code:
            logger.warning(
                'The code for {} is the same as city-level.'.format(
                    address_str))
            return None

        return code

    @classmethod
    def get_instance(cls, directory=None):
        if cls.postcoder is None:
            if directory is None:
                raise RuntimeError("No directory specified.")

            cls.postcoder = PostCoder(input_dir=directory)
            cls.postcoder.load_file()

        return cls.postcoder


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.WARNING)

    postcoder = PostCoder(input_dir=os.path.join(
        os.path.dirname(__file__), '../download/japanpost/'))
    postcoder.load_file()

    print(postcoder.search('東京都多摩市落合１－１５－２'))
    print(postcoder.search('東京都豊島区雑司ヶ谷２－１３－１５'))
