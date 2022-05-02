import csv
import io
import json
from logging import getLogger
import os
import time
from typing import Union, NoReturn, Optional, List
import urllib.parse
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

    def download_files(self) -> NoReturn:
        """
        Download separated data files from
        'Base Registry Data Catalog Site'
        https://registry-catalog.registries.digital.go.jp/
        ex. https://registry-catalog.registries.digital.go.jp/-
            api/3/action/package_show?id=o1-130001_g2-000006
        """
        api_url = (
            'https://registry-catalog.registries.digital.go.jp/'
            'api/3/action/')
        dataset_id_list = []
        download_urls = []

        # Add the following dataset of each prefecture
        # 0000004: 住居表示・街区マスター
        # 0000005: 住居表示・住居マスター
        # 0000006: 町字マスター位置参照拡張
        # 0000007: 住居表示－街区マスター位置参照拡張
        # 0000008: 住居表示－住居マスター位置参照拡張
        for pref_code in self.targets:
            input_filepath = os.path.join(
                self.input_dir,
                'mt_town_pos_pref{:s}.csv.zip'.format(pref_code))
            if os.path.exists(input_filepath):
                continue

            pref_local_code = self._local_authority_code(pref_code + '000')
            for dataset_code in (4, 5, 6, 7, 8):
                dataset_id_list.append('o1-{:s}_g2-{:06d}'.format(
                    pref_local_code, dataset_code))

        for dataset_id in dataset_id_list:
            url = api_url + 'package_show?id={}'.format(dataset_id)
            logger.debug(
                "Getting metadata of package '{}'".format(dataset_id))
            with urllib.request.urlopen(url) as response:
                rawdata = response.read()
                result = json.loads(rawdata)
                metadata = result['result']
                download_url = self.dataurl_from_metadata(
                    metadata, self.input_dir)
                if download_url is not None:
                    logger.debug(
                        "  {} is added to download list.".format(
                            download_url))
                    download_urls.append(download_url)

            time.sleep(1)

        """
        # Download list of "位置参照拡張"
        count = 0
        query_url = "{}package_search?q={}&sort=id+asc".format(
            api_url, urllib.parse.quote(
                '"位置参照拡張" or "住居表示・住居マスター"'))
        url = "{}&rows=0".format(query_url)  # Get number of packages
        logger.debug("Get record count from {}".format(url))
        with urllib.request.urlopen(url) as response:
            result = json.loads(response.read())
            count = result['result']['count']
            logger.debug("Found {} datasets.".format(count))

        for start in range(0, count, 100):
            url = "{}&rows=100&start={}".format(
                query_url, start)  # Get 100 packages
            logger.debug("Get 100 records from {}".format(url))
            with urllib.request.urlopen(url) as response:
                result = json.loads(response.read())
                for metadata in result['result']['results']:
                    download_url = self.dataurl_from_metadata(metadata)
                    if download_url is not None:
                        logger.debug(
                            "  {} is added to download list.".format(
                                download_url))
                        download_urls.append(download_url)

            time.sleep(1)
        """

        # Download data files in the list
        self.download(
            urls=download_urls,
            dirname=self.input_dir
        )
