import csv
import glob
import io
from logging import getLogger
import os
import re
from typing import Union, Optional, List
import urllib.request
import zipfile

import jaconv
from jageocoder.address import AddressLevel

from jageocoder_converter.base_converter import BaseConverter
from jageocoder_converter.data_manager import DataManager

logger = getLogger(__name__)


class JushoConverter(BaseConverter):
    """
    A converter that generates formatted text data at the
    street number level from '電子国土基本図（地名情報）「住居表示住所」'.

    Output 'output/xx_jusho.txt' for each prefecture.
    """
    dataset_name = "電子国土基本図（地名情報）「住居表示住所」"
    dataset_url = "https://www.gsi.go.jp/kihonjohochousa/jukyo_jusho.html"

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
            "「電子国土基本図（地名情報）「住居表示住所」を"
            "ダウンロードします。\n注意事項や利用条件については"
            "https://www.gsi.go.jp/kihonjohochousa/jukyo_jusho.html"
            "を必ず確認してください。\n"
        )
        return super().confirm(terms)

    def process_line(self, args):
        """
        Parse a line and add an address node.
        """
        if len(args) != 9:
            logger.warning("Invalid line: {}. (Skipped)".format(args))
            return

        jcode, aza, gaiku, kiso, code, dummy, lon, lat, scale = args
        uppers = self.jiscodes[jcode]
        names = []

        # 平成30年 (2018年) 奥州市地域自治区解消対応
        if jcode == '03215':
            if aza == '水沢区水沢工業団地':
                aza = '水沢工業団地'
            else:
                aza = re.sub(
                    r'(水沢|江刺|前沢|胆沢|衣川)区',
                    r'\g<1>',
                    aza
                )

        # 平成27年 (2015年) 八戸市地域自治区解消対応
        if jcode == '02203':
            aza = re.sub(r'南郷区', r'南郷', aza)

        # 大字, 字 - street level
        names = [] + self.guessAza(aza, jcode)

        # 街区 - block level
        hugou = jaconv.h2z(gaiku, ascii=False, digit=False)
        names.append([AddressLevel.BLOCK, hugou + '番'])

        # 住居表示 - street number level
        number = jaconv.h2z(kiso, ascii=False, digit=False)
        names.append([AddressLevel.BLD, number + '号'])
        self.print_line(uppers + names, lon, lat)

    def add_from_zipfile(self, zipfilepath):
        """
        Register address notations from 住居表示住所
        """
        with zipfile.ZipFile(zipfilepath) as z:
            for filename in z.namelist():
                if not filename.lower().endswith('.csv'):
                    continue

                with z.open(filename, mode='r') as f:
                    ft = io.TextIOWrapper(
                        f, encoding='utf-8', newline='',
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
        Read records from 'jusho/xx000.zip' files, format them,
        then output to 'output/xx_jusho.txt'.
        """
        self.prepare_jiscode_table()

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

                zipfiles.sort()

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
            dirname=self.input_dir
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
            logger.debug("Extracting url for {}: {}".format(
                m.group(2), zip_url))
            urls.append(zip_url)

        return urls
