import csv
from logging import getLogger
import os
from typing import Union, Optional, List

from jageocoder.address import AddressLevel
from jageocoder.node import AddressNode

from jageocoder_converter.base_converter import BaseConverter
from jageocoder_converter.data_manager import DataManager

logger = getLogger(__name__)


class GeoloniaConverter(BaseConverter):
    """
    A converter that generates formatted text data at the
    area level from 'Geolonia 住所データ'.

    Output 'output/xx_geolonia.txt' for each prefecture.
    """
    dataset_name = "Geolonia 住所データ"
    dataset_url = "https://geolonia.github.io/japanese-addresses/"

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
            "「Geolonia 住所データ」をダウンロードします。\n"
            "https://geolonia.github.io/japanese-addresses/ の"
            "説明およびライセンスを必ず確認してください。\n"
        )
        return super().confirm(terms)

    def process_line(self, args):
        """
        Parse a line and add an address node.
        "都道府県コード",
        "都道府県名",
        "都道府県名カナ",
        "都道府県名ローマ字",
        "市区町村コード",
        "市区町村名",
        "市区町村名カナ",
        "市区町村名ローマ字",
        "大字町丁目名",
        "大字町丁目名カナ",
        "大字町丁目名ローマ字",
        "小字・通称名",
        "緯度",
        "経度"
        """
        ccode = args[4]
        if ccode == '':
            logger.debug("No ccode at '{}'".format(
                ','.join(args)))
            return

        note = None
        oaza = args[8]
        koaza = args[11]
        x, y = args[13], args[12]
        if oaza in ('', '（大字なし）'):
            names = self.jiscodes[ccode] + \
                [[AddressLevel.OAZA, AddressNode.NONAME]]
        else:
            names = self.jiscodes[ccode] + self.guessAza(oaza, ccode)

        if koaza:
            names = names + [[AddressLevel.AZA, koaza]]

        if names[-1][1] == AddressNode.NONAME:
            # NONAME oaza will be registrerd by GaikuConverter.
            return

        azacode = self.code_from_names(names)
        if azacode:
            note = 'aza_id:{}'.format(azacode[5:])

        self.print_line_with_postcode(names, x, y, note)

    def add_from_csvfile(self, csvfilepath: str, pref_code: str):
        """
        Register address notations from Geolonia 住所データ
        for the pref represented by pref_code.
        """
        with open(csvfilepath, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            try:
                for args in reader:
                    if args[0] != pref_code:
                        continue

                    self.process_line(args)
                    pre_args = args

            except UnicodeDecodeError:
                raise RuntimeError((
                    "変換できない文字が見つかりました。"
                    "処理中のファイルは {}, 直前の行は次の通りです。\n{}")
                    .format(csvfilepath, pre_args))

    def convert(self):
        """
        Read records from 'geolonia/latest.csv' file, format them,
        then output to 'output/xx_geolonia.txt'.
        """
        self.prepare_jiscode_table()

        for pref_code in self.targets:
            output_filepath = os.path.join(
                self.output_dir, '{}_geolonia.txt'.format(pref_code))
            if os.path.exists(output_filepath):
                logger.info("SKIP: {}".format(output_filepath))
                continue

            input_filepath = os.path.join(self.input_dir,  'latest.csv')
            if not os.path.exists(input_filepath):
                self.download_files()

            with open(output_filepath, 'w', encoding='utf-8') as fout:
                self.set_fp(fout)
                logger.debug("Reading from {}".format(input_filepath))
                self.add_from_csvfile(input_filepath, pref_code)

    def download_files(self):
        """
        Download zipped data files from
        'Geolonia 住所データ'
        https://geolonia.github.io/japanese-addresses/
        """
        # url = 'https://raw.githubusercontent.com/geolonia/' \
        #     + 'japanese-addresses/master/data/latest.csv'
        url = "https://geolonia.github.io/japanese-addresses/latest.csv"

        self.download(
            urls=[url],
            dirname=self.input_dir
        )
