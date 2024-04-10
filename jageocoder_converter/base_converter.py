import csv
import datetime
from functools import lru_cache
import glob
import json
import logging
import os
import re
import sys
import time
from typing import TextIO, Union, Optional, List, Tuple
import zipfile

from jageocoder.address import AddressLevel
from jageocoder.aza_master import AzaMaster
from jageocoder.itaiji import converter as itaiji_converter
import urllib.request

from jageocoder_converter.data_manager import DataManager
import jageocoder_converter.config

Address = Tuple[int, str]  # Address element level and element name
logger = logging.getLogger(__name__)


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

    jiscodes = {}
    jiscode_from_name = {}
    azacodes = {}
    azacode_from_name = {}

    dataset_name = ""
    dataset_url = ""

    def __init__(
            self, fp: Optional[TextIO] = None,
            manager: Optional[DataManager] = None,
            priority: Optional[int] = None,
            targets: Optional[List[str]] = None,
            quiet: Optional[bool] = False,
            disable_postcoder: Optional[bool] = False):
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
        self.manager = manager
        self.priority = priority
        self.quiet_flag = quiet
        self.targets = targets
        if self.targets is None:
            self.targets = ['{:02d}'.format(x) for x in range(1, 48)]

        self.postcoder = None
        self.disable_postcoder = disable_postcoder

    def get_jiscode_json_path(self):
        """
        Return the file path to the 'jiscode.jsonl'
        """
        data_dir = os.path.abspath(
            os.path.join(
                jageocoder_converter.config.base_download_dir,
                'data'))
        os.makedirs(data_dir, 0o755, exist_ok=True)

        return os.path.join(data_dir, 'jiscode.jsonl')

    def prepare_jiscode_table(self):
        """
        Create a city-level code table and a table for reverse lookup.
        """
        if len(self.jiscodes) > 0:
            return

        jiscode_json_path = self.get_jiscode_json_path()

        if not os.path.exists(jiscode_json_path):
            self.create_jiscodes_from_city_file()

        with open(jiscode_json_path,
                  mode='r', encoding='utf-8') as f:
            for line in f:
                obj = json.loads(line)
                self.jiscodes.update(obj)

        # Create an index for reverse lookup of JIS codes from names.
        for jiscode, elements in self.jiscodes.items():
            names = [x[1] for x in elements]
            name = itaiji_converter.standardize(''.join(names))
            self.jiscode_from_name[name] = jiscode
            if len(names) == 3:
                altname = itaiji_converter.standardize(names[0] + names[2])
                self.jiscode_from_name[altname] = jiscode

    def create_jiscodes_from_city_file(self):
        """
        Read 'geoshape-city.csv' and write 'jiscode.jsonl'
        """
        input_filepath = os.path.join(
            jageocoder_converter.config.base_download_dir,
            'geoshape-city.csv')
        if not os.path.exists(input_filepath):
            self.download(
                urls=[
                    'http://agora.ex.nii.ac.jp/GeoNLP/dict/geoshape-city.csv'
                ],
                dirname=jageocoder_converter.config.base_download_dir
            )

        jiscodes = {}
        with open(input_filepath, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f)
            head = {}
            for rows in reader:
                if rows[0] in ('geonlp_id', 'entry_id'):
                    for i, row in enumerate(rows):
                        head[row] = i

                    continue

                for pref in rows[head['prefname']].split('/'):
                    for county in rows[head['countyname']].split('/'):

                        suffix = rows[head['suffix']].rstrip('/')
                        body = rows[head['body']] + suffix
                        # lon = rows[head['longitude']]
                        # lat = rows[head['latitude']]
                        jiscode = rows[head['code']]
                        valid_to = rows[head['valid_to']]

                        if len(jiscode) < 5:  # 境界未確定地域
                            continue

                        jiscode = jiscode[0:5]

                        level = AddressLevel.CITY
                        if suffix == '区' and pref != '東京都':
                            level = AddressLevel.WARD

                        names = [[AddressLevel.PREF, pref]]
                        if body != county and county != '':
                            if level == AddressLevel.WARD:
                                names.append([AddressLevel.CITY, county])
                            else:
                                names.append([AddressLevel.COUNTY, county])

                        names.append([level, body])

                        if jiscode not in jiscodes:
                            jiscodes[jiscode] = [names, valid_to]
                            continue

                        if jiscodes[jiscode][1] == '':
                            continue

                        if valid_to == '' or valid_to > jiscodes[jiscode][1]:
                            jiscodes[jiscode] = [names, valid_to]

        with open(self.get_jiscode_json_path(), 'w', encoding='utf-8') as f:
            for jiscode, args in jiscodes.items():
                print(json.dumps(
                    {jiscode: args[0]}, ensure_ascii=False), file=f)

    def get_address_all(self, download_dir) -> None:
        """
        Download "address_all.csv.zip" and extract
        to the specified directory.
        """
        target = os.path.join(download_dir, "address_all.csv.zip")
        if not os.path.exists(target):
            api_url = (
                'https://catalog.registries.digital.go.jp/rc/'
                'api/3/action/')
            dataset_id = 'ba000001'
            url = api_url + 'package_show?id={}'.format(dataset_id)
            logger.debug("Getting metadata of package '{}'".format(dataset_id))
            download_url = None
            with urllib.request.urlopen(url) as response:
                result = json.loads(response.read())
                metadata = result['result']
                download_url = self.dataurl_from_metadata(
                    metadata, download_dir)

            self.download(urls=[download_url], dirname=download_dir)

        # Extract "address_all_csv.zip"
        with zipfile.ZipFile(target) as z:
            for filename in z.namelist():
                if not filename.lower().endswith('.zip'):
                    continue

                target = os.path.join(
                    download_dir,
                    os.path.basename(filename))
                with open(target, mode="w+b") as fout:
                    with z.open(filename, mode='r') as fin:
                        fout.write(fin.read())

                    logger.debug("Extracted {}.".format(target))

    def dataurl_from_metadata(
            self,
            metadata: dict,
            data_dir: Optional[os.PathLike] = None) -> Union[str, None]:
        """
        Check CKAN metadata from address-base-registry,
        extract download file url from the metadata.

        Parameters
        ----------
        metadata: dict
            JSON decoded CKAN metadata.
        data_dir: PathLike, optional
            The directory where the datafile will be placed.

        Return
        ------
        str:
            The url where the data can be downloaded.
            If the file already exists and updated,
            return None.
        """
        download_url = None
        data_dir = data_dir or self.input_dir
        issued_at = datetime.datetime.fromisoformat(
            '2000-01-01T00:00')
        modified_at = datetime.datetime.fromisoformat(
            '2000-01-01T00:00')
        for resource in metadata['resources']:
            issued_at = resource["created"]
            modified_at = resource["metadata_modified"]
            url = resource["url"]
            if resource["format"].lower().startswith("csv"):
                basename = os.path.basename(url)
                filepath = os.path.join(data_dir, basename)
                if not os.path.exists(filepath) or \
                        os.path.getmtime(filepath) < max(
                            issued_at.timestamp(),
                            modified_at.timestamp()):
                    download_url = url
                    break

        return download_url

    def confirm(self, terms_of_use: Optional[str] = None) -> bool:
        """
        Show the terms of the license agreement and confirm acceptance.

        Parameters
        ----------
        terms_of_use: str, optional
            Text containing messages or links to license conditions, etc.
            If ommitted, return True without comfirmation.

        Return
        ------
        bool
            Return True if accept.
        """
        while terms_of_use is not None and not self.quiet_flag:
            enter = input("\n" + terms_of_use +
                          " (了承する場合は Y, 中止する場合は N を入力)")
            if enter in ('Y', 'y'):
                break
            elif enter == 'N':
                print("中断します。")
                exit(0)

        return True

    def download(self, urls: List[str],
                 dirname: Union[str, bytes, os.PathLike]) -> None:
        """
        Download files from web specified by urls and save them under dirname.

        Parameters
        ----------
        urls: List[str]
            A list of link urls to files to download.
        dirname: PathLike
            The directory where the downloaded files will be stored.
        """
        if not os.path.exists(dirname):
            os.makedirs(dirname, mode=0o755)

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
            time.sleep(5)

    def set_fp(self, fp: Union[TextIO, None]) -> None:
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

    def aza_from_names(
            self,
            elements: list) -> Union[object, None]:
        """
        Retrieve AzaMaster record from a list of address elements.

        Parameters
        ----------
        elements: list
            List of address elements ([level, name])

        Return
        ------
        object
            AzaMaster record or None.
        """
        key = AzaMaster.standardize_aza_name(elements)
        cands = self.manager.aza_master.search_records_on(
            "names", key)
        if len(cands) == 0:
            return None

        return cands[0]

    def code_from_names(
            self,
            elements: list) -> Union[str, None]:
        """
        Retrieve AzaMaster code from a list of address elements.

        Parameters
        ----------
        elements: list
            List of address elements ([level, name])

        Return
        ------
        str
            azacode or None.
        """
        aza = self.aza_from_names(elements)
        if aza is None:
            return None

        return aza.code

    def names_from_code(
            self,
            code: str) -> Union[List[str], None]:
        """
        Get a list of address elements from an azacode.

        Parameters
        ----------
        code: str
            Azacode.

        Return
        ------
        List[str], None
            List of address elements if the code exists, or None.
        """
        cands = self.manager.aza_master.search_records_on(
            "code", code)
        if len(cands) == 0:
            return None

        return json.loads(cands[0].names)

    def print_line(self, names: List[Address], x: float, y: float,
                   note: Optional[str] = None) -> None:
        """
        Outputs a single line of information.
        If the instance variable priority is set,
        add '!xx' next to the address element names.

        Parameters
        ----------
        names: [[int, str]]
            List of address element level and name
        x: float
            X value (Longitude)
        y: float
            Y value (Latitude)
        note: str, optional
            Notes (used to add codes, identifiers, etc.)
        """
        # keys = []
        # for name in names:
        #     if name[1] == '':
        #         continue

        #     keys.append(
        #         itaiji_converter.standardize(name[1]) + ";{}".format(name[0]))

        # line = " ".join(keys) + "\t"
        line = ""

        for name in names:
            if name[1] != '':
                line += '{:s};{:d},'.format(name[1], name[0])

        if self.priority is not None:
            line += '!{:02d},'.format(self.priority)

        line += "{},{}".format(x or 999, y or 999)
        if note is not None:
            line += ',{}'.format(str(note))

        print(line, file=self.fp)

    def print_line_with_postcode(
            self, names: List[Address], x: float, y: float,
            note: Optional[str] = None) -> None:
        """
        Outputs a single line of information with postcode.

        Parameters
        ----------
        names: [[int, str]]
            List of address element level and name
        x: float
            X value (Longitude)
        y: float
            Y value (Latitude)
        note: str, optional
            Notes (used to add codes, identifiers, etc.)
        """
        if self.disable_postcoder is True:
            self.print_line(names, x, y, note)
            return

        if self.postcoder is None:
            from jageocoder_converter.postcoder import PostCoder
            postcoder = PostCoder.get_instance()
            if postcoder is None:
                self.disable_postcoder = True
                self.print_line(names, x, y, note)
                return

            self.postcoder = postcoder

        if names[-1][0] <= AddressLevel.AZA:
            postcode = self.postcoder.search_by_list(names)
            if postcode:
                new_note = 'postcode:{}'.format(postcode)
                if note is not None and note != '':
                    note += '/' + new_note
                else:
                    note = new_note

        self.print_line(names, x, y, note)

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
            m = re.match(r'^(.+?[^文大])((字|小字).*)$', name)
            if m:
                return [
                    [AddressLevel.OAZA, m.group(1)],
                    [AddressLevel.AZA, m.group(2)]]

        m = re.match(
            r'^(.*?[^０-９一二三四五六七八九〇十])([０-９一二三四五六七八九〇十]+線(東|西|南|北)?)$', name)
        if m:
            return self._resplit_doubled_kansuji([
                [AddressLevel.OAZA, m.group(1)],
                [AddressLevel.AZA, m.group(2)]])

        m = re.match(
            r'^(.*?[０-９一二三四五六七八九〇十]+線)([東西南北]?[０-９一二三四五六七八九〇十]+号)$', name)
        if m:
            return self._resplit_doubled_kansuji([
                [AddressLevel.OAZA, m.group(1)],
                [AddressLevel.AZA, m.group(2)]])

        m = re.match(
            r'^(.*?[^０-９一二三四五六七八九〇十東西南北]{2,})'
            + r'([東西南北][０-９一二三四五六七八九〇十]+(丁目|線|丁))$', name)
        if m:
            return self._resplit_doubled_kansuji([
                [AddressLevel.OAZA, m.group(1)],
                [AddressLevel.AZA, m.group(2)]])

        m = re.match(
            r'^(.+?[^０-９一二三四五六七八九〇十東西南北]+)'
            + r'([東西南北]?[０-９一二三四五六七八九〇十]+(丁目|線|丁))$', name)
        if m:
            return self._resplit_doubled_kansuji([
                [AddressLevel.OAZA, m.group(1)],
                [AddressLevel.AZA, m.group(2)]])

        m = re.match(
            r'^(.*?[０-９一二三四五六七八九〇十]+丁目)([東西南北])$', name)
        if m:
            return self._resplit_doubled_kansuji([
                [AddressLevel.OAZA, m.group(1)],
                [AddressLevel.AZA, m.group(2)]])

        m = re.match(
            r'^(.*?[^０-９一二三四五六七八九〇十]+)'
            + r'([０-９一二三四五六七八九〇十]+(丁目|線|丁))$', name)
        if m:
            return self._resplit_doubled_kansuji([
                [AddressLevel.OAZA, m.group(1)],
                [AddressLevel.AZA, m.group(2)]])

        m = re.match(r'^(.*?[^０-９一二三四五六七八九〇十])([０-９一二三四五六七八九〇十]+番地)$', name)
        if m:
            return self._resplit_doubled_kansuji([
                [AddressLevel.OAZA, m.group(1)],
                [AddressLevel.BLOCK, m.group(2)]])

        # If it can' t be split, returned as is
        return [[AddressLevel.OAZA, name]]

    def _resplit_doubled_kansuji(self, values: list) -> list:
        """
        If the beginning of the split second notation
        contains two consecutive Kansuji from '一' to '九',
        move the first character to the end of the first notation.

        For example, if '与一二丁目' was split into '与' and '一二丁目',
        this method will be re-split into '与一' and '二丁目'.
        """
        m = re.match(r'^([一二三四五六七八九])([一二三四五六七八九].*)$',
                     values[1][1])
        if m:
            values[0][1] += m.group(1)
            values[1][1] = m.group(2)

        return values

    @ lru_cache
    def guessAza(self, name: str, jcode: str = '') -> str:
        """
        Analyze the Aza-name and return the split-formatted one.

        Parameters
        ----------
        name: str
            Aza-name
        jcode: str
            The jiscode of the municipality containing the address
            being processed

        Return
        ------
        list
            List of address elements composing the given string

        Examples
        --------
        >>> from jageocoder_converter.base_converter import BaseConverter
        >>> base = BaseConverter(disable_postcoder=True)
        >>> base.guessAza('大通西十三丁目')
        [[5, '大通'], [6, '西十三丁目']]
        >>> base.guessAza('与一一丁目')
        [[5, '与一'], [6, '一丁目']]
        >>> base.guessAza('神南二丁目')
        [[5, '神南'], [6, '二丁目']]
        >>> base.guessAza('北十一条西十三丁目')
        [[5, '北十一条'], [6, '西十三丁目']]
        """
        name = re.sub(r'[\u3000\s]+', '', name)

        if name[0] == '字':
            # Remove the leading '字'
            name = name[1:]

            result = self._guessAza_sub(name, ignore_aza=True)
            result[0][1] = '字' + result[0][1]
            return result

        pos = name.find('大字')
        if pos >= 0:
            area_name = name[:pos]
            sub_name = name[pos + 2:]

            if jcode == '06201' and sub_name.startswith('十文字'):
                # Exception: 山形県/山形市/大字十文字/大原 is not
                # converted to 大字十文/字大原
                result = [[AddressLevel.OAZA, '大字十文字'],
                          [AddressLevel.AZA, sub_name[3:]]]
            else:
                m = re.match(r'^([^字]+?[^文])(字.+)', sub_name)
                if m:
                    result = [[AddressLevel.OAZA, '大字' + m.group(1)],
                              [AddressLevel.AZA, m.group(2)]]
                else:
                    result = self._guessAza_sub(sub_name)
                    result[0][1] = '大字' + result[0][1]

            if area_name != '':
                result.insert(0, [AddressLevel.OAZA, area_name])

            return result

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
                return result

        # The following address is a maintenance error and should be corrected
        #   長野県/長野市/若里6丁目 -> 長野県/長野市/若里６丁目
        #   長野県/長野市/若里7丁目 -> 長野県/長野市/若里７丁目
        #   広島県/福山市/駅家町大字弥生ケ -> 広島県/福山市/駅家町大字弥生ヶ丘
        if jcode == '20201' and name == '若里6丁目':
            result = [[AddressLevel.OAZA, '若里'],
                      [AddressLevel.AZA, '６丁目']]
            return result

        if jcode == '20201' and name == '若里7丁目':
            result = [[AddressLevel.OAZA, '若里'],
                      [AddressLevel.AZA, '７丁目']]
            return result

        if jcode == '34207' and name == '駅家町大字弥生ケ':
            result = [[AddressLevel.OAZA, '駅家町'],
                      [AddressLevel.OAZA, '大字弥生ヶ丘']]
            return result

        result = self._guessAza_sub(name)
        return result

    def escape_texts(self, pattern: str):
        """
        Excape output text files generated by previous execution.

        Parameters
        ----------
        pattern: str
            Text file patter, such as 'city', 'oaza', etc.
        """
        for filepath in glob.glob(
                os.path.join(self.output_dir, f"*_{pattern}.txt")):
            if os.path.exists(filepath):
                os.rename(filepath, filepath + '.bak')

    def unescape_texts(self, pattern: str):
        """
        Recover xcaped output text files.

        Parameters
        ----------
        pattern: str
            Text file patter, such as 'city', 'oaza', etc.
        """
        for filepath in glob.glob(
                os.path.join(self.output_dir, f"*_{pattern}.txt.bak")):
            if os.path.exists(filepath):
                os.rename(filepath, filepath[0:-4])
