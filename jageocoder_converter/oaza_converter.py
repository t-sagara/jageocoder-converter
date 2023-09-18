import csv
import glob
import io
from logging import getLogger
import os
from typing import Union, Optional, List
import zipfile

from jageocoder.node import AddressNode
from jageocoder.address import AddressLevel

from jageocoder_converter.base_converter import BaseConverter
from jageocoder_converter.data_manager import DataManager

logger = getLogger(__name__)


class OazaConverter(BaseConverter):
    """
    A converter that generates formatted text data at the
    area level from '大字・町丁目レベル位置参照情報'.

    Output 'output/xx_oaza.txt' for each prefecture.
    """
    dataset_name = "大字・町丁目レベル位置参照情報"
    dataset_url = "https://nlftp.mlit.go.jp/cgi-bin/isj/dls/_choose_method.cgi"

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
        self.nonames = {}

    def confirm(self) -> bool:
        """
        Show the terms of the license agreement and confirm acceptance.
        """
        terms = (
            "「大字・町丁目レベル位置参照情報」をダウンロードします。\n"
            "https://nlftp.mlit.go.jp/ksj/other/agreement.html の"
            "利用規約を必ず確認してください。\n"
        )
        return super().confirm(terms)

    def download_files(self) -> None:
        """
        Download zipped data files from
        '位置参照情報ダウンロードサービス'
        https://nlftp.mlit.go.jp/cgi-bin/isj/dls/_choose_method.cgi
        """
        urlbase = 'https://nlftp.mlit.go.jp/isj/dls/data'
        version = '16.0b'  # PY2022, 令和4年度
        urls = []
        for pref_code in self.targets:
            url = "{0}/{1}/{2}000-{1}.zip".format(
                urlbase, version, pref_code)
            urls.append(url)

        self.download(
            urls=urls,
            dirname=self.input_dir
        )

    def process_line(self, args):
        """
        Parse a line and add an address node.
        """
        if args[0] == '都道府県コード':
            return

        if args[2] == "07023":  # 07000-12.0b のバグ
            args[2] = "07203"

        note = None
        pcode, pname, ccode, cname, isj_code, oaza, y, x = args[0:8]
        ccode = ("00000" + ccode)[-5:]
        names = self.jiscodes[ccode]
        address = names + self.guessAza(oaza, ccode)
        aza = self.aza_from_names(address)
        if aza:
            note = 'aza_id:{}'.format(aza.code[5:])

        if aza is None or aza.startCountType != 1:
            if ccode not in self.nonames:
                self.nonames[ccode] = {
                    "address": names
                    + [[AddressLevel.OAZA, AddressNode.NONAME]],
                    "x": None,
                    "y": None,
                    "cns": [],
                }

            self.nonames[ccode]["cns"].append(oaza)

        self.print_line_with_postcode(address, x, y, note=note)

    def add_from_zipfile(self, zipfilepath):
        """
        Register address notations from 大字・町丁目レベル位置参照情報
        """
        with zipfile.ZipFile(zipfilepath) as z:
            for filename in z.namelist():
                if not filename.lower().endswith('.csv'):
                    continue

                pre_args = None
                with z.open(filename, mode='r') as f:
                    ft = io.TextIOWrapper(
                        f, encoding='CP932', newline='',
                        errors='backslashreplace')
                    reader = csv.DictReader(ft)
                    logger.debug('Processing {} in {}...'.format(
                        filename, zipfilepath))
                    try:
                        for row in reader:
                            args = [
                                row['都道府県コード'],
                                row['都道府県名'],
                                row['市区町村コード'],
                                row['市区町村名'],
                                row['大字町丁目コード'],
                                row['大字町丁目名'],
                                row['緯度'],
                                row['経度'],
                                row['原典資料コード'],
                                row['大字・字・丁目区分コード'],
                            ]
                            self.process_line(args)
                            pre_args = args

                    except UnicodeDecodeError:
                        raise RuntimeError((
                            "変換できない文字が見つかりました。"
                            "処理中のファイルは {}, 直前の行は次の通りです。\n{}")
                            .format(filename, pre_args))

    def convert(self):
        """
        Read records from 'oaza/xx000*.zip' files, format them,
        then output to 'output/xx_oaza.txt'.
        """
        self.prepare_jiscode_table()

        for pref_code in self.targets:
            self.nonames = {}
            output_filepath = os.path.join(
                self.output_dir, '{}_oaza.txt'.format(pref_code))
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

                # Output noname OAZA information with 'common names'.
                for ccode, values in self.nonames.items():
                    note = "cn:" + "|".join(values["cns"])
                    self.print_line(
                        names=values["address"],
                        x=None,
                        y=None,
                        note=note,
                    )
