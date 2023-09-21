import csv
import glob
import io
from logging import getLogger
import os
import re
from typing import Union, Optional, List
import zipfile

import jaconv
from jageocoder.address import AddressLevel
from jageocoder_converter.base_converter import BaseConverter
from jageocoder_converter.data_manager import DataManager
from jageocoder.node import AddressNode

logger = getLogger(__name__)


class GaikuConverter(BaseConverter):
    """
    A converter that generates formatted text data at the street and
    block number level from '街区レベル位置参照情報'.

    Output 'output/xx_city.txt' for each prefecture.
    """
    dataset_name = "街区レベル位置参照情報"
    dataset_url = "https://nlftp.mlit.go.jp/cgi-bin/isj/dls/_choose_method.cgi"
    re_hugou = re.compile(r'^([^\d]*)(\d*[A-Z]?号?)([^\d]*)')

    def __init__(self,
                 output_dir: Union[str, bytes, os.PathLike],
                 input_dir: Union[str, bytes, os.PathLike],
                 manager: Optional[DataManager] = None,
                 priority: Optional[int] = None,
                 targets: Optional[List[str]] = None,
                 quiet: Optional[bool] = False) -> None:
        super().__init__(
            manager=manager, priority=priority, targets=targets, quiet=quiet)
        self.output_dir = output_dir
        self.input_dir = input_dir
        self.fp = None

    def confirm(self) -> bool:
        """
        Show the terms of the license agreement and confirm acceptance.
        """
        terms = (
            "「街区レベル位置参照情報」をダウンロードします。\n"
            "https://nlftp.mlit.go.jp/ksj/other/agreement.html の"
            "利用規約を必ず確認してください。\n"
        )
        return super().confirm(terms)

    def download_files(self):
        """
        Download zipped data files from
        '位置参照情報ダウンロードサービス'
        https://nlftp.mlit.go.jp/cgi-bin/isj/dls/_choose_method.cgi
        """
        urlbase = 'https://nlftp.mlit.go.jp/isj/dls/data'
        version = '21.0a'  # PY2022, 令和4年度
        urls = []
        for pref_code in self.targets:
            url = "{0}/{1}/{2}000-{1}.zip".format(
                urlbase, version, pref_code)
            urls.append(url)

        self.download(
            urls=urls,
            dirname=self.input_dir
        )

    def process_line(self, args, mode='latlon'):
        """
        Parse a line and add an address node.
        """
        if args[0] == '都道府県名' or args[2] == '' or args[11] == '0':
            # Skip line with blank Aza-names and non-representative points
            return

        if '.' in args[4]:
            # 街区符号・地番に小数点を含む場合はエラーとして無視
            return

        """
        if args[12] == '3' or args[13] == '3':
            # Add registered data with flag
            flag = True
        """

        pref = args[0]
        city = args[1]

        # Correct and register any data that contains errors.
        if pref == '大阪市':
            pref = "大阪府"
        elif pref == '岩手県' and city == '上開伊郡大槌町':
            city = '上閉伊郡大槌町'

        jcode = self._get_jiscode(pref + city)
        if jcode is None and city.find('ケ') >= 0:
            jcode = self._get_jiscode(pref + city.replace('ケ', 'ヶ'))

        if jcode is None and city.find('ヶ') >= 0:
            jcode = self._get_jiscode(pref + city.replace('ヶ', 'ケ'))

        if jcode is None:
            raise RuntimeError("Cannot get the jiscode of {} ({})".format(
                pref + city, args))

        if mode == 'latlon':
            y = args[8]  # latitude
            x = args[9]  # longitude
        elif mode == 'xy':
            x = args[6]
            y = args[7]

        uppers = self.jiscodes[jcode]
        names = []

        # The following addresses may be a branch numbers
        # 17206 石川県/加賀市/永井町五十六/12 => 石川県/加賀市/永井町/56番地/12
        if jcode == '17206':
            m = re.match(r'^永井町([一二三四五六七八九十１２３４５６７８９].*)$', args[2])
            if m:
                names.append([AddressLevel.OAZA, '永井町'])
                chiban = m.group(1).translate(self.trans_kansuji_zerabic)
                chiban = chiban.replace('十', '')
                names.append([AddressLevel.BLOCK, chiban + '番地'])
                hugou = jaconv.h2z(args[3], ascii=False, digit=False)
                names.append([AddressLevel.BLC, hugou])
                self.print_line(uppers + names, x, y)
                return

        if args[2] in ('', '（大字なし）'):
            names.append([AddressLevel.OAZA, AddressNode.NONAME])
        else:
            names += self.guessAza(args[2], jcode)

        if args[3] != '' and args[3] != ' ':
            names.append([AddressLevel.AZA, args[3]])

        hugou = jaconv.h2z(args[4], ascii=False, digit=False)
        if args[10] == '1':
            # 住居表示地域
            if hugou[-1] in '0123456789ABCabc':
                # 大阪市中央区上町の A番-C番 対応
                names.append([AddressLevel.BLOCK, hugou + '番'])
            else:
                # 「渡辺」対応
                logger.debug("Non-numeric hugou '{}' in {}".format(
                    hugou, ','.join(args)))
                names.append([AddressLevel.BLOCK, hugou])
        else:
            # 住居表示未実施地域
            m = self.re_hugou.match(hugou)
            aza, chiban, dropped = m.groups()
            if aza != '':
                if len(names) > 0 and names[-1][1] == aza:
                    # Error handling of data with duplicate Aza and Chiban
                    names = names[:-1]

                names.append([AddressLevel.AZA, aza])

            if chiban:
                if dropped == '':
                    names.append([AddressLevel.BLOCK, chiban + '番地'])
                else:
                    # 脱落地
                    # logger.debug("Datsurakuchi '{}' in {}".format(
                    #     hugou, ','.join(args)))
                    if chiban[-1] == '号':
                        names.append([AddressLevel.BLOCK, chiban])
                    else:
                        names.append([AddressLevel.BLOCK, chiban + '番'])

                    names.append([AddressLevel.BLOCK, dropped + '地'])

        self.print_line(uppers + names, x, y)

    def add_from_zipfile(self, zipfilepath):
        """
        Register address notations from 街区レベル位置参照情報
        """
        with zipfile.ZipFile(zipfilepath) as z:
            for filename in z.namelist():
                if not filename.lower().endswith('.csv'):
                    continue

                with z.open(filename, mode='r') as f:
                    ft = io.TextIOWrapper(
                        f, encoding='CP932', newline='',
                        errors='backslashreplace')
                    reader = csv.DictReader(ft)
                    pre_args = None
                    logger.debug('Processing {} in {}...'.format(
                        filename, zipfilepath))
                    try:
                        for row in reader:
                            args = [
                                row['都道府県名'],
                                row['市区町村名'],
                                row['大字・丁目名'],
                                row['小字・通称名'],
                                row['街区符号・地番'],
                                row['座標系番号'],
                                row['Ｘ座標'],
                                row['Ｙ座標'],
                                row['緯度'],
                                row['経度'],
                                row['住居表示フラグ'],
                                row['代表フラグ'],
                                row['更新前履歴フラグ'],
                                row['更新後履歴フラグ'],
                            ]
                            self.process_line(args)
                            pre_args = args

                    except UnicodeDecodeError:
                        raise RuntimeError((
                            "変換できない文字が見つかりました。"
                            "処理中のファイルは {}, "
                            "直前の行は次の通りです。\n{}").format(
                                filename, pre_args))

    def convert(self):
        """
        Read records from 'gaiku/xx000.zip' files, format them,
        then output to 'output/xx_gaiku.txt'.
        """
        self.prepare_jiscode_table()

        for pref_code in self.targets:
            output_filepath = os.path.join(
                self.output_dir, '{}_gaiku.txt'.format(pref_code))
            if os.path.exists(output_filepath):
                logger.info("SKIP: {}".format(output_filepath))
                continue

            input_filepath = None
            while input_filepath is None:
                zipfiles = glob.glob(
                    os.path.join(self.input_dir,
                                 '{}000*.zip'.format(pref_code)))
                if len(zipfiles) == 0:
                    self.download_files()
                else:
                    input_filepath = zipfiles[0]

            with open(output_filepath, 'w', encoding='utf-8') as fout:
                self.set_fp(fout)
                logger.debug("Reading from {}".format(input_filepath))
                self.add_from_zipfile(input_filepath)

    def update_oaza_index(self):
        """
        Create Oaza index from 'gaiku/xx000.zip' files.
        The index will be output to 'data/oazalist.txt'.
        """
        oaza_list = []
        for pref_code in range(1, 48):
            input_filepath = None
            while input_filepath is None:
                zipfiles = glob.glob(
                    os.path.join(self.input_dir,
                                 '{:02d}000*.zip'.format(pref_code)))
                if len(zipfiles) == 0:
                    self.download_files()
                else:
                    input_filepath = zipfiles[0]

            logger.debug("Extracting Oaza from {}".format(
                input_filepath))
            oaza_list += self.create_oaza_list(input_filepath)

        oaza_list = list(set(oaza_list))
        oaza_list.sort()
        with open(self.get_oaza_list_path(), 'w', encoding='utf-8') as f:
            for oaza in oaza_list:
                print(oaza, file=f)
