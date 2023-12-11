import csv
from functools import lru_cache
import io
from logging import getLogger
import os
import re
from typing import Union
import zipfile

import marisa_trie
from jageocoder.address import AddressLevel
from jageocoder.itaiji import converter

from jageocoder_converter.base_converter import BaseConverter


logger = getLogger(__name__)


class PostCoder(BaseConverter):

    re_range = re.compile(r'(.*)（([０-９～、]+)(.*)）')
    re_koaza = re.compile(r'(.*)（(.+)）')

    postcoder = None

    def __init__(self,
                 input_dir: Union[str, bytes, os.PathLike]):
        self.input_dir = input_dir

        self.addresses = set()
        self.codes = {}
        self.trie = None

    def load_file(self) -> None:
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

                            try:
                                cityname = ''.join(
                                    [x[1] for x in self.jiscodes[citycode]]
                                )
                            except KeyError:
                                logger.warning(
                                    f"Unknown citycode '{citycode}', skipped"
                                )
                                continue

                            m = self.re_koaza.match(oaza)
                            if m:
                                surface = m.group(2)
                                for aza in self._parse_koaza(surface):
                                    if aza in ('その他', '次のビルを除く'):
                                        aza = ''

                                    self._register_address(
                                        oaza=cityname + m.group(1),
                                        aza=aza,
                                        postalcode=postalcode)

                            else:
                                self._register_address(
                                    oaza=cityname + oaza,
                                    aza='',
                                    postalcode=postalcode)

                            pre_args = args

                    except UnicodeDecodeError:
                        raise RuntimeError((
                            "変換できない文字が見つかりました。"
                            "処理中のファイルは {}, "
                            "直前の行は次の通りです。\n{}").format(
                                filename, pre_args))

        self.trie = marisa_trie.Trie(self.addresses)

    def _parse_koaza(self, surface: str) -> list:
        """
        Split koaza representations by '、'.
        Ex. '３５～３８、４１、４２' -> ['３５～３８', '４１', '４２']
        """
        segments = surface.split('、')
        for segm in segments:
            yield segm

    def _search_pattern(self, pattern: str, target: str):
        """
        Search pattern including numbers from target.

        >>> _search_pattern('1.~19.丁目', '11.丁目')
        True
        """
        logger.debug('_search_pattern("{}","{}")'.format(
            pattern, target))
        ranges = []
        re_pattern = pattern
        for m in re.finditer(r'((\d+)\.)?(~((\d+)\.)?)?', pattern):
            if m.group(0) == '':
                continue

            span = m.group(0)
            args = m.groups()
            logger.debug('args:{}'.format(m.groups()))
            if args[1] is None and args[4] is None:
                continue

            while len(args) < 5:
                args.append(None)

            re_pattern = re_pattern.replace(span, r'(\d+)\.')
            if args[4]:
                if args[1] is None:
                    ranges.append([None, int(args[4])])
                else:
                    ranges.append([int(args[1]), int(args[4])])
            elif args[2]:
                ranges.append([int(args[1]), None])
            else:
                ranges.append([int(args[1]), int(args[1])])

        logger.debug('re_pattern: "{}"'.format(re_pattern))
        logger.debug(ranges)

        m = re.search(re_pattern, target)
        if m is None:
            logger.debug('not match at all.')
            return False

        for i, val in enumerate(m.groups()):
            v = int(val)
            r = ranges[i]
            logger.debug('Comparing value {} to range {}'.format(
                v, r))
            if r[0] is not None and v < r[0]:
                logger.debug('  {} is smaller than {} (FAIL)'.format(
                    v, r[0]))
                return False

            if r[1] is not None and v > r[1]:
                logger.debug('  {} is larger than {} (FAIL)'.format(
                    v, r[1]))
                return False

        logger.debug('Pass all check.')
        return True

    def _register_address(
            self, oaza: str, aza: str, postalcode: str) -> None:
        standardized = converter.standardize(oaza)
        if standardized not in self.addresses:
            self.addresses.add(standardized)
            self.codes[standardized] = {}

        aza_standardized = converter.standardize(aza)
        self.codes[standardized][aza_standardized] = postalcode

    @lru_cache
    def search(self, address: str) -> Union[str, None]:
        standardized = converter.standardize(address)

        while True:
            prefixes = self.trie.prefixes(standardized)
            if len(prefixes) == 0:
                return None

            prefixes.sort(key=lambda prefix: len(prefix), reverse=True)
            longest_prefix = prefixes[0]
            suffix = standardized[len(longest_prefix):]
            if suffix.startswith('字'):
                logger.debug("... remove '字' from {}".format(suffix))
                standardized = longest_prefix + suffix[1:]
                continue
            elif suffix.startswith('大字'):
                logger.debug("... remove '大字' from {}".format(suffix))
                standardized = longest_prefix + suffix[1:]
                continue

            break

        codes = self.codes[longest_prefix]
        code_list = list(codes.keys())
        code_list.sort(key=lambda aza: len(aza), reverse=True)

        for aza_pattern in code_list:
            if aza_pattern == '':
                return self.codes[longest_prefix][aza_pattern]

            if self._search_pattern(aza_pattern, suffix):
                return self.codes[longest_prefix][aza_pattern]

        return None

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
            if directory is None:  # Do not use PostCoder.
                return None

            cls.postcoder = PostCoder(input_dir=directory)
            cls.postcoder.load_file()

        return cls.postcoder
