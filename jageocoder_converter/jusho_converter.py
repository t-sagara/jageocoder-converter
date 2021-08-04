import csv
import glob
import io
from logging import getLogger
import os
import re
from typing import Union, NoReturn, Optional, List
import urllib.request
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
                 targets: Optional[List[str]] = None,
                 quiet: Optional[bool] = False) -> NoReturn:
        super().__init__(priority=priority, targets=targets, quiet=quiet)
        self.output_dir = output_dir
        self.input_dir = input_dir
        self.fp = None
        self.prepare_jiscode_table()

    def process_line(self, args):
        """
        住居表示住所の１行を解析して住所ノードを追加する
        """
        if len(args) > 9:
            raise RuntimeError("Invalid line: {}".format(args))

        jcode, aza, gaiku, kiso, code, dummy, lon, lat, scale = args
        uppers = self.jiscodes[jcode]
        names = []

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
                    logger.debug('Processing {} in {}...'.format(
                        filename, zipfilepath))
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

            zipfiles = None
            while zipfiles is None:
                zipfiles = glob.glob(
                    os.path.join(self.input_dir,
                                 '{}???.zip'.format(pref_code)))
                if len(zipfiles) == 0:
                    self.download_files()

            with open(output_filepath, 'w', encoding='utf-8') as fout:
                self.set_fp(fout)

                for jusho_filepath in zipfiles:
                    logger.debug("Reading from {}".format(jusho_filepath))
                    self.add_from_zipfile(jusho_filepath)

    def download_files(self):
        """
        Download zipped data files from
        '電子国土基本図（地名情報）「住居表示住所」の閲覧・ダウンロード'
        https://saigai.gsi.go.jp/jusho/download/
        """
        urlbase = 'https://saigai.gsi.go.jp/jusho/download/pref/'
        urls = []
        for pref_code in self.targets:
            url = "{0}/{1}.html".format(urlbase, pref_code)
            urls += self._extract_zip_urls(url)

        self.download(
            urls=urls,
            dirname=self.input_dir,
            notes=(
                "「電子国土基本図（地名情報）「住居表示住所」を"
                "ダウンロードします。\n注意事項や利用条件については"
                "https://www.gsi.go.jp/kihonjohochousa/jukyo_jusho.html"
                "を必ず確認してください。\n"
            )
        )

    def _extract_zip_urls(self, url: str) -> List[str]:
        """
        Extract url list of zipped files from the index html
        such as https://saigai.gsi.go.jp/jusho/download/pref/01.html
        """
        with urllib.request.urlopen(url) as f:
            content = f.read().decode('utf-8')

        urls = []
        # pattern: <li><a href="../data/01101.zip">札幌市中央区</a></li>
        for m in re.finditer(r'<li><a href="([^"]+)">([^<]+)</a></li>',
                             content):
            zip_url = os.path.join(
                os.path.dirname(url), m.group(1))
            logger.debug("{}: {}".format(m.group(2), zip_url))
            urls.append(zip_url)

        return urls
