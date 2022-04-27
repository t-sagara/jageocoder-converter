import csv
import io
import json
from logging import getLogger
import os
import time
from typing import Union, NoReturn, Optional, List
import urllib.request
import zipfile

from jageocoder_converter.base_converter import BaseConverter

logger = getLogger(__name__)


class BaseRegistryConverter(BaseConverter):
    """
    A converter that generates formatted text data at the
    area level from 'Address-Base-Registry' from Japan Digital Agency.

    Output 'output/xx_base_registry.txt' for each prefecture. ?
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

    def confirm(self) -> bool:
        """
        Show the terms of the license agreement and confirm acceptance.
        """
        terms = (
            "「アドレス・ベース・レジストリ」をダウンロードします。\n"
            "https://www.digital.go.jp/policies/base_registry_address_tos/ の"
            "利用規約を必ず確認してください。\n"
        )
        return super().confirm(terms)

    def process_line(self, row: dict):
        """
        Parse a line and add an address node.

        "全国地方公共団体コード"
        "町字id"
        "住居表示フラグ"
        "代表点_経度"
        "代表点_緯度"
        "代表点_座標参照系"
        "代表点_地図情報レベル"
        "ポリゴン_ファイル名"
        "ポリゴン_キーコード"
        "ポリゴン_データフォーマット"
        "ポリゴン_座標参照系"
        "ポリゴン_地図情報レベル"
        "位置参照情報_大字町丁目コード"
        "位置参照情報_データ整備年度"
        "国勢調査_境界_小地域（町丁・字等別）"
        ”国勢調査_境界_データ整備年度"
        """
        citycode = row["全国地方公共団体コード"][0:5]
        aza_id = row["町字id"]
        names = self.azacodes[citycode + aza_id]
        x, y = row["代表点_経度"], row["代表点_緯度"]
        note = 'aza_id:{}'.format(aza_id)
        if not x or not y:
            raise RuntimeError(
                "x or y is empty. citycode={}, aza_id={}".format(
                    citycode, aza_id))

        self.print_line_with_postcode(names, x, y, note)

    def add_from_zipfile(self, zipfilepath: str):
        """
        Register address notations from JDA address base registry.
        """
        with zipfile.ZipFile(zipfilepath) as z:
            for filename in z.namelist():
                if not filename.lower().endswith('.csv'):
                    continue

                with z.open(filename, mode='r') as f:
                    ft = io.TextIOWrapper(
                        f, encoding='utf-8', newline='',
                        errors='backslashreplace')
                    reader = csv.DictReader(ft)
                    logger.debug('Processing {} in {}...'.format(
                        filename, zipfilepath))
                    for row in reader:
                        self.process_line(row)

    def convert(self):
        """
        Read records from 'geolonia/latest.csv' file, format them,
        then output to 'output/xx_geolonia.txt'.
        """
        self.prepare_jiscode_table()
        self.prepare_azacode_table()
        self.download_files()

        for pref_code in self.targets:
            output_filepath = os.path.join(
                self.output_dir, '{}_basereg.txt'.format(pref_code))
            if os.path.exists(output_filepath):
                logger.info("SKIP: {}".format(output_filepath))
                continue

            input_filepath = os.path.join(
                self.input_dir,
                'mt_town_pos_pref{:s}.csv.zip'.format(pref_code))

            with open(output_filepath, 'w', encoding='utf-8') as fout:
                self.set_fp(fout)
                logger.debug("Reading from {}".format(input_filepath))
                self.add_from_zipfile(input_filepath)

    def _local_authority_code(self, orig_code: str) -> str:
        """
        Returns the 6-digit code, adding a check digit to the JIS code.
        https://www.soumu.go.jp/main_content/000137948.pdf
        """
        if len(orig_code) != 5:
            raise RuntimeError('The original code must be a 5-digit string.')

        sum = int(orig_code[0]) * 6 + int(orig_code[1]) * 5 +\
            int(orig_code[2]) * 4 + int(orig_code[3]) * 3 +\
            int(orig_code[4]) * 2
        if sum < 11:
            checkdigit = str(11 - sum)
        else:
            remainder = sum % 11
            checkdigit = str(11 - remainder)[-1]

        return orig_code + checkdigit

    def download_files(self):
        """
        Download separated data files from
        'Base Registry Data Catalog Site'
        https://registry-catalog.registries.digital.go.jp/
        ex. https://registry-catalog.registries.digital.go.jp/-
            api/3/action/package_show?id=o1-130001_g2-000006
        """
        api_url = 'https://registry-catalog.registries.digital.go.jp/api/3/action/'
        dataset_id_list = []
        download_urls = []
        # Add '町字マスター位置参照拡張' of each prefecture
        for pref_code in self.targets:
            input_filepath = os.path.join(
                self.input_dir,
                'mt_town_pos_pref{:s}.csv.zip'.format(pref_code))
            if os.path.exists(input_filepath):
                continue

            pref_local_code = self._local_authority_code(pref_code + '000')
            dataset_id_list.append('o1-{}_g2-000006'.format(
                pref_local_code))

        for dataset_id in dataset_id_list:
            url = api_url + 'package_show?id={}'.format(dataset_id)
            logger.debug(
                "Getting metadata of package '{}'".format(dataset_id))
            with urllib.request.urlopen(url) as response:
                rawdata = response.read()
                metadata = json.loads(rawdata)
                for extra in metadata['result']['extras']:
                    if extra["key"].endswith('dcat:accessURL'):
                        url = extra["value"]
                        download_urls.append(url)

            time.sleep(1)

        self.download(
            urls=download_urls,
            dirname=self.input_dir
        )
