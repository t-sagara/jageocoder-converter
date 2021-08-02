import csv
import glob
import io
from logging import getLogger
import os
from typing import Union, NoReturn, Optional, List
import zipfile

import jaconv
from jageocoder.address import AddressLevel

from jageocoder_converter.base_converter import BaseConverter

logger = getLogger(__name__)


class JushoConverter(BaseConverter):
    """
    電子国土基本図（地名情報）「住居表示住所」から住居表示レベルの
    整形済みテキストデータを生成するコンバータ。

    都道府県別に 'output/xx_jusho.txt' を出力します。
    """

    def __init__(self,
                 output_dir: Union[str, bytes, os.PathLike],
                 input_dir: Union[str, bytes, os.PathLike],
                 priority: Optional[int] = None,
                 targets: Optional[List[str]] = None) -> NoReturn:
        super().__init__(priority=priority, targets=targets)
        self.output_dir = output_dir
        self.input_dir = input_dir
        self.fp = None

    def process_line(self, args):
        """
        住居表示住所の１行を解析して住所ノードを追加する
        """
        if len(args) > 9:
            raise RuntimeError("Invalid line: {}".format(args))

        jcode, aza, gaiku, kiso, code, dummy, lon, lat, scale = args
        uppers = self.jiscodes[jcode]
        names = []
        level = AddressLevel.UNDEFINED

        # 大字，字レベル
        names = [] + self.guessAza(aza, jcode)

        # 街区レベル
        hugou = jaconv.h2z(gaiku, ascii=False, digit=False)
        names.append([AddressLevel.BLOCK, hugou + '番'])

        # 住居表示レベル
        number = jaconv.h2z(kiso, ascii=False, digit=False)
        names.append([AddressLevel.BLD, number + '号'])
        self.print_line(uppers + names, lon, lat)

    def add_from_zipfile(self, zipfilepath):
        """
        住居表示住所から住所表記を登録
        """
        with zipfile.ZipFile(zipfilepath) as z:
            for filename in z.namelist():
                if not filename.lower().endswith('.csv'):
                    continue

                with z.open(filename, mode='r') as f:
                    ft = io.TextIOWrapper(
                        f, encoding='CP932', newline='',
                        errors='backslashreplace')
                    reader = csv.reader(ft)
                    pre_args = None
                    try:
                        for args in reader:
                            self.process_line(args)
                            pre_args = args
                    except UnicodeDecodeError:
                        raise RuntimeError((
                            "変換できない文字が見つかりました。"
                            "処理中のファイルは {}, 直前の行は次の通りです。\n{}")
                            .format(filename, pre_args))

    def convert(self):
        """
        saigai.gsi.go.jp/jusho/download/data/xx000.zip を探して整形処理を行ない、
        output/xx_jusho.txt に出力する。
        """
        # 住居表示住所レベル
        for pref_code in self.targets:
            output_filepath = os.path.join(
                self.output_dir, '{}_jusho.txt'.format(pref_code))
            if os.path.exists(output_filepath):
                logger.info("SKIP: {}".format(output_filepath))
                continue

            with open(output_filepath, 'w', encoding='utf-8') as fout:

                basename = "{}000.zip".format(pref_code)
                self.set_fp(fout)

                for jusho_filepath in sorted(glob.glob(
                        os.path.join(self.input_dir,
                                     '{}*.zip'.format(pref_code)))):
                    logger.debug("Reading from {}".format(jusho_filepath))
                    self.add_from_zipfile(jusho_filepath)
