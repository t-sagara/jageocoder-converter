import csv
import io
from logging import getLogger
import os
from typing import Union, Optional, List
import zipfile

from jageocoder.address import AddressLevel

from jageocoder_converter import BaseConverter
from jageocoder_converter.data_manager import DataManager

logger = getLogger(__name__)


class ChibanConverter(BaseConverter):
    """
    A converter to generate formatted text data of Chiban
    from pre-processed chiban-csv data.

    Output 'output/xx_chiban.txt' for each prefecture.
    """
    dataset_name = "法務省登記所備付地図代表点データ"
    dataset_url = "https://front.geospatial.jp/moj-chizu-xml-readme/"

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
        self.records = {}

    def confirm(self) -> bool:
        """
        Show the terms of the license agreement and confirm acceptance.
        """
        terms = (
            "「法務省登記所備付地図代表点データ」をダウンロードします。\n"
            "利用条件等は {url} を確認してください。\n"
        ).format(url='https://www.geospatial.jp/ckan/dataset/houmusyouchizu-2022-1-1/resource/737ca982-2217-4721-8be4-eb5bf9cb1899')  # noqa: E501
        return super().confirm(terms)

    def download_files(self) -> None:
        """
        Download data files.
        """
        urlbase = 'https://www.info-proto.com/static/jageocoder/chiban'
        version = '2022'  # PY2022, 令和4年度
        urls = []
        for pref_code in self.targets:
            url = "{0}/{1}/{2}_chiban.zip".format(
                urlbase, version, pref_code)
            urls.append(url)

        self.download(
            urls=urls,
            dirname=self.input_dir
        )

    def add_from_zipfile(self, zipfilepath):
        """
        Register address notations from 登記所備付地図代表点データ
        """
        with zipfile.ZipFile(zipfilepath) as z:
            for filename in z.namelist():
                if not filename.lower().endswith('.csv'):
                    continue

                with z.open(filename, mode='r') as f:
                    ft = io.TextIOWrapper(
                        f, encoding='UTF-8', newline='',
                        errors='backslashreplace')
                    reader = csv.reader(ft)
                    pre_args = None
                    logger.debug('Processing {} in {}...'.format(
                        filename, zipfilepath))
                    for row in reader:
                        if row[0][0] in ('市', '(',):  # Skip header line
                            continue

                        names = self.jiscodes[row[0]][:]  # 市町村c
                        if row[1]:  # 大字名
                            names.append([AddressLevel.OAZA, row[1]])

                        if row[2]:  # 丁目名
                            names.append([AddressLevel.AZA, row[2]])

                        if row[3]:  # 小字名
                            names.append([AddressLevel.AZA, row[3]])

                        if row[4]:  # 予備名
                            names.append([AddressLevel.AZA, row[4]])

                        if row[5]:  # 地番
                            ban_list = row[5].split('-')
                            if len(ban_list) > 0:
                                names.append(
                                    [AddressLevel.BLOCK, ban_list[0] + '番地'])

                            for ban in ban_list[1:]:
                                names.append([AddressLevel.BLD, ban])

                        self.print_line(names, row[6], row[7])

    def convert(self):
        """
        Read records from 'chiban/xx_chiban.zip' files, format them,
        then output to 'output/xx_chiban.txt'.
        """
        self.prepare_jiscode_table()

        for pref_code in self.targets:
            output_filepath = os.path.join(
                self.output_dir, '{}_chiban.txt'.format(pref_code))
            if os.path.exists(output_filepath):
                logger.info("SKIP: {}".format(output_filepath))
                continue

            input_filepath = os.path.join(
                self.input_dir, '{}_chiban.zip'.format(pref_code))

            with open(output_filepath, 'w', encoding='utf-8') as fout:
                self.set_fp(fout)
                logger.debug("Reading from {}".format(input_filepath))
                self.add_from_zipfile(input_filepath)
