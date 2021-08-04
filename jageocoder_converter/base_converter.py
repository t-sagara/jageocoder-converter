from jageocoder.address import AddressLevel
from jageocoder.itaiji import converter as itaiji_converter
from collections import OrderedDict
import json
import logging
import os
import re
import sys
from typing import TextIO, Union, Optional, NoReturn, List, Tuple
import urllib.request


Address = Tuple[int, str]  # Address element level and element name

logger = logging.getLogger(__name__)


class LRU(OrderedDict):
    'Limit size, evicting the least recently looked-up key when full'

    def __init__(self, maxsize=512, *args, **kwds):
        self.maxsize = maxsize
        super().__init__(*args, **kwds)

    def __getitem__(self, key):
        value = super().__getitem__(key)
        self.move_to_end(key)
        return value

    def __setitem__(self, key, value):
        if key in self:
            self.move_to_end(key)

        super().__setitem__(key, value)
        if len(self) > self.maxsize:
            oldest = next(iter(self))
            del self[oldest]


class BaseConverter(object):
    """
    Location Reference Information Converter Base Class

    Attributes
    ----------
    seirei: list
        List of cities designated by government ordinance
    jiscodes: dict[str, [str]]
        Dict with city code (JISX0402) as a key and a list of
        address element names as a value
    fp: TextIO, optional
        The text stream to output the result
    priority: int
        Priority to be included in output results,
        smaller is higher priority
    targets: list[str]
        List of prefecture codes (JISX0401) to be processed
    quiet: bool
        Quiet mode, if set to True, skip all input.
    """

    seirei = [
        "札幌市", "仙台市", "さいたま市", "千葉市", "横浜市",
        "川崎市", "相模原市", "新潟市", "静岡市", "浜松市",
        "名古屋市", "京都市", "大阪市", "堺市", "神戸市",
        "岡山市", "広島市", "北九州市", "福岡市", "熊本市"]

    kansuji = ['〇', '一', '二', '三', '四', '五', '六', '七', '八', '九']
    trans_kansuji_zarabic = str.maketrans('一二三四五六七八九', '１２３４５６７８９')

    def __init__(self, fp: Optional[TextIO] = None,
                 priority: Optional[int] = None,
                 targets: Optional[List[str]] = None,
                 quiet: Optional[bool] = False):
        """
        Initialize the converter.

        Parameters
        ----------
        fp: TextIO, optional
            The text stream to output the result
        priority: int, optional
            Priority to be included in output results,
            smaller is higher priority
        targets: list[str], optional
            List of prefecture codes (JISX0401) to be processed
        """
        self.set_fp(fp)
        self.priority = priority
        self.quiet_flag = quiet
        self.cache = LRU()
        self.targets = targets
        if self.targets is None:
            self.targets = ['{:02d}'.format(x) for x in range(1, 48)]

    def get_jiscode_json_path(self):
        """
        Return the file path to the 'jiscode.json'
        """
        return os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                'data/jiscode.jsonl'))

    def prepare_jiscode_table(self):
        """
        Create a city-level code table and a table for reverse lookup.
        The original data, data/jiscode.json, is created by
        'running city_converter.py:read_city_data()'.
        """
        self.jiscodes = {}
        with open(self.get_jiscode_json_path(), 'r') as f:
            for line in f:
                obj = json.loads(line)
                self.jiscodes.update(obj)

        # Create an index for reverse lookup of JIS codes from names.
        self.jiscode_from_name = {}
        for jiscode, elements in self.jiscodes.items():
            names = [x[1] for x in elements]
            name = itaiji_converter.standardize(''.join(names))
            self.jiscode_from_name[name] = jiscode
            if len(names) == 3:
                altname = itaiji_converter.standardize(names[0] + names[2])
                self.jiscode_from_name[altname] = jiscode

    def download(self, urls: List[str],
                 dirname: Union[str, bytes, os.PathLike],
                 notes: Optional[str] = None) -> NoReturn:
        """
        Download files from web specified by urls and save them under dirname.
        If notes is not None, display the message and a prompt for confirmation.
        """
        if not os.path.exists(dirname):
            os.makedirs(dirname, mode=0o755)

        while notes is not None and not self.quiet_flag:
            enter = input(notes + " (了承する場合は Y, 中止する場合は N を入力)")
            if enter == 'Y':
                break
            elif enter == 'N':
                print("中断します。")
                exit(0)

        for url in urls:
            basename = os.path.basename(url)
            filename = os.path.join(dirname, basename)
            if os.path.exists(filename):
                logger.info(
                    "File '{}' exists. (skip downloading)".format(filename))
                continue

            logger.debug(
                "Downloading '{}'->'{}'".format(url, filename))

            local_filename, headers = urllib.request.urlretrieve(url, filename)

    def set_fp(self, fp: Union[TextIO, None]) -> NoReturn:
        """
        Set (or change if already set) the output stream.

        Parameters
        ----------
        fp: TextIO, None
            The stream to output to, or sys.stdout if None.
        """
        if fp is None:
            self.fp = sys.stdout

        self.fp = fp

    def _get_jiscode(self, name: str) -> Union[str, None]:
        """
        Get the jiscode from the address element name.

        Parameters
        ----------
        name: str
            Address element name

        Return
        ------
        str, None
            jiscode, or None if not exists
        """
        st_name = itaiji_converter.standardize(name)
        if st_name in self.jiscode_from_name:
            return self.jiscode_from_name[st_name]

        return None

    def print_line(self, names: List[Address], x: float, y: float,
                   note: Optional[str] = None) -> NoReturn:
        """
        Outputs a single line of information.
        If the instance variable priority is set,
        add '!xx' next to the address element names.

        Parameters
        ----------
        names: [[str, int]]
            List of address elemenet names and levels
        x: float
            X value (Longitude)
        y: float
            Y value (Latitude)
        note: str, optional
            Notes (used to add codes, identifiers, etc.)
        """
        line = ''
        for name in names:
            line += '{:d};{:s},'.format(*name)

        if self.priority is not None:
            line += '!{:02d},'.format(self.priority)

        line += "{},{}".format(x, y)
        if note is not None:
            line += ',{}'.format(str(note))

        print(line, file=self.fp)

    def _arabicToNumber(self, arabic: str) -> int:
        """
        Converts Arabic numerals to int values.

        Parameters
        ----------
        arabic: str
            Numeric character string with Arabic numerals

        Return
        ------
        int
            Evaluated integer value, decimals are not recognized
        """
        total = 0
        for char in arabic:
            i = "０１２３４５６７８９0123456789".index(char)
            if i is None:
                break
            elif i < 10:
                total = total * 10 + i
            else:
                total = total * 10 + i - 10

        return total

    def _numberToKansuji(self, num: int) -> str:
        """
        Converts integer value to Chinese numeral.

        Parameters
        ----------
        num: int
            The integer value

        Return
        ------
        str
            A string of characters expressed in Chinese numerals
        """
        kanji = ''
        if num >= 1000:
            i = num / 1000
            if i > 1:
                kanji += self.kansuji[i]

            kanji += '千'
            num = num % 1000

        if num >= 100:
            i = num / 100
            if i > 1:
                kanji += self.kansuji[i]

            kanji += '百'
            num = num % 100

        if num >= 10:
            i = num / 10
            if i > 1:
                kanji += self.kansuji[i]

            kanji += '十'
            num = num % 10

        if num > 0:
            kanji += self.kansuji[num]

        return kanji

    def _guessAza_sub(self, name: str,
                      ignore_aza: bool = False) -> List[Address]:
        """
        Split Oaza and Aza in the string.

        Parameters
        ----------
        name: str
            The input string which may contain Oaza and Aza names
        ignore_aza: bool
            If set to True, ignore the Oaza which is starting with '字'

        Return
        ------
        list
            List of address elements composing the given string
        """
        if not ignore_aza:
            m = re.match(r'^([^字]+?[^文])(字.*)$', name)
            if m:
                return [[AddressLevel.OAZA, m.group(1)], [6, m.group(2)]]

        m = re.match(
            r'^(.*?[^０-９一二三四五六七八九〇十])([０-９一二三四五六七八九〇十]+線(東|西|南|北)?)$', name)
        if m:
            return [
                [AddressLevel.OAZA, m.group(1)],
                [AddressLevel.AZA, m.group(2)]]

        m = re.match(
            r'^(.*?[^０-９一二三四五六七八九〇十])([東西南北]?[０-９一二三四五六七八九〇十]+(丁目|線))$', name)
        if m:
            return [
                [AddressLevel.OAZA, m.group(1)],
                [AddressLevel.AZA, m.group(2)]]

        m = re.match(r'^(.*?[^０-９一二三四五六七八九〇十])([０-９一二三四五六七八九〇十]+番地)$', name)
        if m:
            return [
                [AddressLevel.OAZA, m.group(1)],
                [AddressLevel.BLOCK, m.group(2)]]

        # If it can' t be split, returned as is
        return [[AddressLevel.OAZA, name]]

    def guessAza(self, name: str, jcode: Optional[str] = None) -> str:
        """
        Analyze the Aza-name and return the split-formatted one.

        Parameters
        ----------
        name: str
            Aza-name
        jcode: str, optional
            The jiscode of the municipality containing the address
            being processed

        Return
        ------
        list
            List of address elements composing the given string

        Examples
        --------
        >>> from jageocoder_converter.base_converter import BaseConverter
        >>> base = BaseConverter()
        >>> base.guessAza('大通西十三丁目')
        [[5, '大通'], [6, '西十三丁目']]
        """
        name = re.sub(r'[　\s+]', '', name)

        if name in self.cache:
            return self.cache[name]

        if name[0] == '字':
            # Remove the leading '字'
            name = name[1:]

            result = self._guessAza_sub(name, ignore_aza=True)
            result[0][1] = '字' + result[0][1]
            self.cache[name] = result
            return result

        if name.startswith('大字'):
            name = name[2:]

            if jcode == '06201' and name.startswith('十文字'):
                # Exception: 山形県/山形市/大字十文字/大原 is not
                # converted to 大字十文/字大原
                result = [[AddressLevel.OAZA, '大字十文字'],
                          [AddressLevel.AZA, name[3:]]]
                self.cache[name] = result
                return result

            m = re.match(r'^([^字]+?[^文])(字.+)', name)
            if m:
                result = [[AddressLevel.OAZA, '大字' + m.group(1)],
                          [AddressLevel.AZA, m.group(2)]]
                self.cache[name] = result
                return result

            return self._guessAza_sub(name)

        # Exception handling when '大字・町丁目名' field value in
        # the '位置参照情報' is '（大字なし）'
        if '（大字なし）' == name:
            return []

        # The following addresses have Aza names after the Tyome
        #   福島県/郡山市/日和田町八丁目 (堰町)
        #   長野県/飯田市/知久町三丁目 (大横)
        #   長野県/飯田市/通り町三丁目 (大横)
        #   長野県/飯田市/本町三丁目（大横）
        #   岐阜県/岐阜市/西野町６丁目（北町)
        if jcode == '07203':
            m = re.match(r'^(日和田町)(八丁目.*)$', name)
        elif jcode == '20205':
            m = re.match(r'^(知久町)(三丁目.*)$', name)
            if not m:
                m = re.match(r'^(通り町)(三丁目.*)$', name)
            if not m:
                m = re.match(r'^(本町)([三四]丁目.*)$', name)
        elif jcode == '21201':
            m = re.match(r'^(西野町)([６７六七]丁目.*)$', name)
            if m:
                result = [[AddressLevel.OAZA, m.group(1)],
                          [AddressLevel.AZA, m.group(2)]]
                self.cache[name] = result
                return result

        # The following address is a maintenance error and should be corrected
        #   長野県/長野市/若里6丁目 -> 長野県/長野市/若里６丁目
        #   長野県/長野市/若里7丁目 -> 長野県/長野市/若里７丁目
        #   広島県/福山市/駅家町大字弥生ケ -> 広島県/福山市/駅家町大字弥生ヶ丘
        if jcode == '20201' and name == '若里6丁目':
            result = [[AddressLevel.OAZA, '若里'],
                      [AddressLevel.AZA, '６丁目']]
            self.cache[name] = result
            return result

        if jcode == '20201' and name == '若里7丁目':
            result = [[AddressLevel.OAZA, '若里'],
                      [AddressLevel.AZA, '７丁目']]
            self.cache[name] = result
            return result

        if jcode == '34207' and name == '駅家町大字弥生ケ':
            result = [[AddressLevel.OAZA, '駅家町大字弥生ヶ丘']]
            self.cache[name] = result
            return result

        result = self._guessAza_sub(name)
        self.cache[name] = result
        return result
