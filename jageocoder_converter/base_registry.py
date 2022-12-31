import copy
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

from jageocoder.address import AddressLevel
from jageocoder_converter.base_converter import BaseConverter
from pyproj import Transformer

logger = getLogger(__name__)


class BaseRegistryConverter(BaseConverter):
    """
    A converter that generates formatted text data at the
    area level from 'Address-Base-Registry' from Japan Digital Agency.

    Output 'output/xx_base_registry.txt' for each prefecture. ?
    """
    dataset_name = "アドレス・ベース・レジストリ"
    dataset_url = "https://registry-catalog.registries.digital.go.jp/"

    def __init__(self,
                 output_dir: Union[str, bytes, os.PathLike],
                 input_dir: Union[str, bytes, os.PathLike],
                 manager: Optional["DataManager"] = None,
                 priority: Optional[int] = None,
                 targets: Optional[List[str]] = None,
                 quiet: Optional[bool] = False) -> NoReturn:
        super().__init__(
            manager=manager, priority=priority, targets=targets, quiet=quiet)
        self.output_dir = output_dir
        self.input_dir = input_dir
        self.fp = None
        self.blocks = None

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

    def process_lines_06(self, fin):
        """
        Parse lines and output address nodes in 'mt_town_pos_prefxx.csv'.

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
        reader = csv.DictReader(fin)
        for row in reader:
            citycode = row["全国地方公共団体コード"][0:5]
            aza_id = row["町字id"]
            names = self.names_from_code(citycode + aza_id)
            x, y = row["代表点_経度"], row["代表点_緯度"]
            note = 'aza_id:{}'.format(aza_id)
            if not x or not y:
                raise RuntimeError(
                    "x or y is empty. citycode={}, aza_id={}".format(
                        citycode, aza_id))

            self.print_line_with_postcode(names, x, y, note)

    def process_lines_07(self, fin):
        """
        Parse lines and output address nodes in 'mt_rsdtdsp_blk_pos_pref01.csv'.

        全国地方公共団体コード,町字id,街区id,
        住居表示フラグ,住居表示方式コード,
        代表点_経度,代表点_緯度,代表点_座標参照系,代表点_地図情報レベル,
        ポリゴン_ファイル名,ポリゴン_キーコード,ポリゴン_データ_フォーマット,
        ポリゴン_座標参照系,ポリゴン_地図情報レベル,
        位置参照情報_都道府県名,位置参照情報_市区町村名,位置参照情報_大字・町丁目名,
        位置参照情報_小字・通称名,位置参照情報_街区符号・地番,
        位置参照情報_データ整備年度,
        電子国土基本図(地図情報)「住居表示住所」_住所コード(可読),
        電子国土基本図（地名情報）「住居表示住所」_データ整備日
        """
        transformer = None
        crs = None
        self.blocks = {}

        # Read spatial attributes of the all records from fin_pos
        reader = csv.DictReader(fin)
        for row in reader:
            aza_code = row["全国地方公共団体コード"][0:5] + row["町字id"]
            block_code = aza_code + row["街区id"]
            if crs is None:
                crs = row["代表点_座標参照系"]
                transformer = Transformer.from_crs(crs, "EPSG:4326")
            elif crs != row["代表点_座標参照系"]:
                raise RuntimeError("CRS changed from {} to {}".format(
                    crs, row["代表点_座標参照系"]))

            y, x = transformer.transform(
                row["代表点_緯度"], row["代表点_経度"])
            block_name = row["位置参照情報_街区符号・地番"] + \
                ("番" if row["住居表示フラグ"] == "1" else "番地")
            names = copy.copy(self.names_from_code(aza_code))
            names.append([AddressLevel.BLOCK, block_name])
            self.blocks[block_code] = names
            if not x or not y:
                raise RuntimeError("x or y is empty. blockcode={}".format(
                    block_code))

            self.print_line(names, x, y)

    def process_lines_0508(self, fin, fin_pos):
        """
        Parse lines and output address nodes in
        'mt_rsdtdsp_rsdt_prefxx.csv' and 'mt_rsdtdsp_rsdt_pos_prefxx.csv'.

        (fin)
        全国地方公共団体コード,町字id,街区id,住居id,住居2id,
        市区町村名,政令市区名,大字・町名,丁目名,小字名,街区符号,住居番号,住居番号2,
        住居表示フラグ,住居表示方式コード,
        大字・町外字フラグ,小字外字フラグ,状態フラグ,効力発生日,廃止日,
        原典資料コード,備考

        (fin_pos)
        全国地方公共団体コード,町字id,街区id,住居id,住居2id,
        住居表示フラグ,住居表示方式コード,
        代表点_経度,代表点_緯度,代表点_座標参照系,代表点_地図情報レベル,
        電子国土基本図(地図情報)「住居表示住所」_住所コード(可読),
        電子国土基本図（地名情報）「住居表示住所」_データ整備日
        """
        def __calc_codes(row: dict) -> dict:
            codes = {}
            codes["aza"] = row["全国地方公共団体コード"][0:5] + row["町字id"]
            codes["block"] = codes["aza"] + row["街区id"]
            codes["building"] = codes["block"] + row["住居id"] + \
                (row["住居2id"] or '')
            return codes

        transformer = None
        crs = None

        # Read spatial attributes of the all records from fin_pos
        reader = csv.DictReader(fin)
        reader_pos = csv.DictReader(fin_pos)
        pos_pool = {}
        max_code_in_pool = None
        for row in reader:
            codes = __calc_codes(row)
            building_code = codes["building"]

            if max_code_in_pool is None or max_code_in_pool < building_code:
                pos_pool = {}
                while len(pos_pool) < 100:
                    pos_row = next(reader_pos, None)
                    if pos_row is None:
                        break

                    pos_codes = __calc_codes(pos_row)
                    pos_building_code = pos_codes["building"]
                    if pos_building_code < building_code:
                        continue

                    pos_pool[pos_building_code] = pos_row
                    max_code_in_pool = pos_building_code

            if building_code not in pos_pool:
                msg = "No location data for rsdt '{}'".format(",".join(
                    [row[k] for k in (
                        "市区町村名", "政令市区名", "大字・町名", "丁目名",
                        "小字名", "街区符号", "住居番号", "住居番号2")]))
                logger.warning(msg)
                continue  # Skip address without coordinates
            else:
                pos_row = pos_pool[building_code]
                if crs is None:
                    crs = pos_row["代表点_座標参照系"]
                    transformer = Transformer.from_crs(crs, "EPSG:4326")
                elif crs != pos_row["代表点_座標参照系"]:
                    raise RuntimeError("CRS changed from {} to {}".format(
                        crs, pos_row["代表点_座標参照系"]))

                y, x = transformer.transform(
                    pos_row["代表点_緯度"], pos_row["代表点_経度"])

            if codes["block"] not in self.blocks:
                msg = "No block data for rsdt '{}'".format(",".join(
                    [row[k] for k in (
                        "市区町村名", "政令市区名", "大字・町名", "丁目名",
                        "小字名", "街区符号", "住居番号", "住居番号2")]))
                # logger.warning(msg)
                names = copy.copy(self.names_from_code(codes["aza"]))
                if row["街区符号"]:
                    block_name = row["街区符号"] + \
                        ("番" if row["住居表示フラグ"] == "1" else "番地")
                    names.append([AddressLevel.BLOCK, block_name])

            else:
                names = copy.copy(self.blocks[codes["block"]])

            if row["住居番号2"]:
                name = row["住居番号"] + "－"
                names.append([AddressLevel.BLD, name])
                name = row["住居番号2"] + (
                    "号" if row["住居表示フラグ"] == "1" else "")
                names.append([AddressLevel.BLD, name])
            else:
                name = row["住居番号"] + (
                    "号" if row["住居表示フラグ"] == "1" else "")
                names.append([AddressLevel.BLD, name])

            if not x or not y:
                raise RuntimeError("x or y is empty. buildingcode={}".format(
                    building_code))

            self.print_line(names, x, y)

    def convert(self):
        """
        Read records from 'geolonia/latest.csv' file, format them,
        then output to 'output/xx_geolonia.txt'.
        """
        self.prepare_jiscode_table()

        for pref_code in self.targets:
            # 町字マスター
            output_filepath = os.path.join(
                self.output_dir, '{}_basereg_town.txt'.format(pref_code))
            if os.path.exists(output_filepath):
                logger.info("SKIP: {}".format(output_filepath))
            else:
                input_filepath = os.path.join(
                    self.input_dir,
                    'mt_town_pos_pref{:s}.csv.zip'.format(pref_code))
                with open(output_filepath, 'w', encoding='utf-8') as fout, \
                        self.open_csv_in_zipfile(input_filepath) as fin:
                    self.fp = fout
                    self.process_lines_06(fin)

            # 住居表示－街区マスター位置参照拡張
            output_filepath = os.path.join(
                self.output_dir, '{}_basereg_blk.txt'.format(pref_code))
            output_filepath_rsdt = os.path.join(
                self.output_dir, '{}_basereg_rsdt.txt'.format(pref_code))
            if os.path.exists(output_filepath) and \
                    os.path.exists(output_filepath_rsdt):
                logger.info("SKIP: {}".format(output_filepath))
            else:
                input_filepath_pos = os.path.join(
                    self.input_dir,
                    'mt_rsdtdsp_blk_pos_pref{:s}.csv.zip'.format(pref_code))
                with open(output_filepath, 'w', encoding='utf-8') as fout, \
                        self.open_csv_in_zipfile(input_filepath_pos) as fin:
                    self.fp = fout
                    self.process_lines_07(fin)

            # 住居表示・住居マスター，位置参照拡張
            if os.path.exists(output_filepath_rsdt):
                logger.info("SKIP: {}".format(output_filepath))
            else:
                input_filepath = os.path.join(
                    self.input_dir,
                    'mt_rsdtdsp_rsdt_pref{:s}.csv.zip'.format(pref_code))
                input_filepath_pos = os.path.join(
                    self.input_dir,
                    'mt_rsdtdsp_rsdt_pos_pref{:s}.csv.zip'.format(pref_code))
                with open(
                        output_filepath_rsdt, "w", encoding='utf-8') as fout,\
                        self.open_csv_in_zipfile(input_filepath) as fin,\
                        self.open_csv_in_zipfile(
                            input_filepath_pos) as fin_pos:
                    self.fp = fout
                    self.process_lines_0508(fin, fin_pos)

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
        # x 0000004: 住居表示・街区マスター
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
            for dataset_code in (5, 6, 7, 8):
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
