import copy
import csv
from logging import getLogger
import os
import tempfile
from typing import Union, Optional, List
import zipfile

from jageocoder.address import AddressLevel
from jageocoder_converter.base_converter import BaseConverter
from jageocoder_converter.data_manager import DataManager
from pyproj import Transformer

logger = getLogger(__name__)


class BaseRegistryConverter(BaseConverter):
    """
    A converter that generates formatted text data at the
    area level from 'Address-Base-Registry' from Japan Digital Agency.

    Output 'output/xx_base_registry.txt' for each prefecture. ?
    """
    dataset_name = "アドレス・ベース・レジストリ"
    dataset_url = "https://catalog.registries.digital.go.jp/rc/"

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
        self.blocks = None
        self._processed_azaid = None

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

    def process_lines_01(self, fin, pref_code):
        """
        Parse lines and output address nodes in 'mt_town_all.csv'.

        全国地方公共団体コード,
        町字id,
        町字区分コード,
        都道府県名,
        都道府県名_カナ,
        都道府県名_英字,
        郡名,
        郡名_カナ,
        郡名_英字,
        市区町村名,
        市区町村名_カナ,
        市区町村名_英字,
        政令市区名,
        政令市区名_カナ,
        政令市区名_英字,
        大字・町名,
        大字・町名_カナ,
        大字・町名_英字,
        丁目名,
        丁目名_カナ,
        丁目名_数字,
        小字名,
        小字名_カナ,
        小字名_英字,
        住居表示フラグ,
        住居表示方式コード,
        大字・町名_通称フラグ,
        小字名_通称フラグ,
        大字・町名_電子国土基本図外字,
        小字名_電子国土基本図外字,
        状態フラグ,
        起番フラグ,
        効力発生日,
        廃止日,
        原典資料コード,
        郵便番号,
        備考
        """
        reader = csv.DictReader(fin)
        for row in reader:
            citycode = row["全国地方公共団体コード"][0:5]
            if citycode[0:2] != pref_code:
                continue

            aza_id = row["町字id"]
            if aza_id in self._processed_azaid:
                continue

            names = self.names_from_code(citycode + aza_id)
            x, y = 999.9, 999.9
            note = 'aza_id:{}'.format(aza_id)
            self.print_line_with_postcode(names, x, y, note)

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
            if names is None:
                logger.warning("Aza_code '{} {}' is not found. (Skipped)".format(
                    citycode, aza_id))
                continue

            x, y = row["代表点_経度"], row["代表点_緯度"]
            note = 'aza_id:{}'.format(aza_id)
            if not x or not y:
                raise RuntimeError(
                    "x or y is empty. citycode={}, aza_id={}".format(
                        citycode, aza_id))

            self._processed_azaid.add(aza_id)
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
                msg = "No coodinates for rsdt '{}'".format(",".join(
                    [row[k] for k in (
                        "市区町村名", "政令市区名", "大字・町名", "丁目名",
                        "小字名", "街区符号", "住居番号", "住居番号2")]))
                logger.warning(msg)
                x, y = 999.9, 999.9
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
        Read records from 'mt_town_pos_prefxx.csv' file, format them,
        then output to 'text/xx_basereg_town.txt'.
        """
        self.prepare_jiscode_table()

        for pref_code in self.targets:
            # 町字マスター
            output_filepath = os.path.join(
                self.output_dir, f"{pref_code}_basereg_town.txt")
            if os.path.exists(output_filepath):
                logger.info(f"SKIP: {output_filepath}")
            else:
                all_zip = os.path.join(
                    self.input_dir, 'mt_town_pos_all.csv.zip')
                filename = f"mt_town_pos_pref{pref_code}.csv.zip"

                self._processed_azaid = set()
                with tempfile.NamedTemporaryFile("w+b") as nt:
                    with zipfile.ZipFile(all_zip) as z:
                        with z.open(filename, mode='r') as f:
                            nt.write(f.read())

                    with open(output_filepath, 'w', encoding='utf-8') as fout, \
                            self.manager.open_csv_in_zipfile(nt.name) as fin:
                        self.fp = fout
                        self.process_lines_06(fin)

                zip_filename = os.path.join(
                    self.input_dir, "mt_town_all.csv.zip")

                with open(output_filepath, 'a', encoding='utf-8') as fout, \
                        self.manager.open_csv_in_zipfile(zip_filename) as fin:
                    self.fp = fout
                    self.process_lines_01(fin, pref_code)

            # 住居表示－街区マスター位置参照拡張
            output_filepath = os.path.join(
                self.output_dir, f'{pref_code}_basereg_blk.txt')
            output_filepath_rsdt = os.path.join(
                self.output_dir, f'{pref_code}_basereg_rsdt.txt')
            if os.path.exists(output_filepath) and \
                    os.path.exists(output_filepath_rsdt):
                logger.info(f"SKIP: {output_filepath}")
            else:
                all_zip = os.path.join(
                    self.input_dir, 'mt_rsdtdsp_blk_pos_all.csv.zip')
                filename = f'mt_rsdtdsp_blk_pos_pref{pref_code}.csv.zip'
                with tempfile.NamedTemporaryFile("w+b") as nt:
                    with zipfile.ZipFile(all_zip) as z:
                        with z.open(filename, mode='r') as f:
                            nt.write(f.read())

                    with open(output_filepath, 'w', encoding='utf-8') as fout, \
                            self.manager.open_csv_in_zipfile(nt.name) as fin:
                        self.fp = fout
                        self.process_lines_07(fin)

            # 住居表示・住居マスター，位置参照拡張
            if os.path.exists(output_filepath_rsdt):
                logger.info(f"SKIP: {output_filepath}")
            else:
                all_zip = os.path.join(
                    self.input_dir, 'mt_rsdtdsp_rsdt_all.csv.zip')
                all_pos_zip = os.path.join(
                    self.input_dir, 'mt_rsdtdsp_rsdt_pos_all.csv.zip')
                filename = f'mt_rsdtdsp_rsdt_pref{pref_code}.csv.zip'
                filename_pos = f'mt_rsdtdsp_rsdt_pos_pref{pref_code}.csv.zip'
                with tempfile.NamedTemporaryFile("w+b") as nt, \
                        tempfile.NamedTemporaryFile("w+b") as nt_pos:
                    with zipfile.ZipFile(all_zip) as z:
                        with z.open(filename, mode='r') as f:
                            nt.write(f.read())

                    with zipfile.ZipFile(all_pos_zip) as z:
                        with z.open(filename_pos, mode='r') as f:
                            nt_pos.write(f.read())

                    with open(
                            output_filepath_rsdt, "w",
                            encoding='utf-8') as fout, \
                            self.manager.open_csv_in_zipfile(nt.name) as fin, \
                            self.manager.open_csv_in_zipfile(nt_pos.name) as fin_pos:
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

    def download_files(self) -> None:
        """
        Now all address-base data files are zipped in one file.
        The file should be already downloaded by
        BaseConverter.get_address_all(), but confirm here.
        """
        # 0000004: 住居表示・街区マスター
        # 0000005: 住居表示・住居マスター
        # 0000006: 町字マスター位置参照拡張
        # 0000007: 住居表示－街区マスター位置参照拡張
        # 0000008: 住居表示－住居マスター位置参照拡張
        targets = (
            'mt_city_all.csv.zip',
            'mt_pref_all.csv.zip',
            'mt_rsdtdsp_blk_all.csv.zip',
            'mt_rsdtdsp_blk_pos_all.csv.zip',
            'mt_rsdtdsp_rsdt_all.csv.zip',
            'mt_rsdtdsp_rsdt_pos_all.csv.zip',
            'mt_town_all.csv.zip',
            'mt_town_pos_all.csv.zip',
        )
        not_found_files = []
        for target in targets:
            zipfilepath = os.path.join(self.input_dir, target)
            if not os.path.exists(zipfilepath):
                not_found_files.append(target)

        # Download data files if the targets are missed.
        if len(not_found_files) > 0:
            self.get_address_all(self.input_dir)
