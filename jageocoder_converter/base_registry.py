import bz2
import copy
import csv
import json
from logging import getLogger
from pathlib import Path
import re
from typing import Optional, List

from jageocoder.address import AddressLevel
from jageocoder.node import AddressNode
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
    dataset_url = "https://dataset.address-br.digital.go.jp/"

    def __init__(self,
                 output_dir: Path,
                 input_dir: Path,
                 manager: DataManager,
                 priority: int,
                 targets: Optional[List[str]] = None,
                 quiet: Optional[bool] = False) -> None:
        super().__init__(
            manager=manager, priority=priority, targets=targets, quiet=quiet)
        self.output_dir = output_dir
        self.input_dir = input_dir
        self.fp = None
        self.blocks = {}
        self._processed_azaid = set()
        self._lg_names = {}

    def set_lgname(self, code: str, names: list):
        """
        Set local-government names to the code.
        """
        code = code[0:5]
        if code not in self._lg_names:
            lg_name = [x for x in names if x[0] < AddressLevel.OAZA]
            self._lg_names[code] = lg_name

    def get_lgname(self, code: str):
        """
        Get local-government names of the code.
        """
        code = code[0:5]
        if code in self._lg_names:
            return self._lg_names[code]

        # Net set yet... retrieve aza-master.
        pos = self.manager.aza_master.binary_search(
            code + '9999999'
        )
        _aza_record = self.manager.aza_master.get_record(pos)
        if _aza_record["code"][0:5] != code:
            return None

        _cand = json.loads(_aza_record["names"])
        lg_name = [x for x in _cand if x[0] < AddressLevel.OAZA]
        self._lg_names[code] = lg_name
        return lg_name

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

    def process_lines_in_machiaza(self, fin, pref_code):
        """
        Parse lines and output address nodes in 'mt_town_all.csv'.

        1, lg_code, 全国地方公共団体コード
        2, machiaza_id, 町字ID
        3, machiaza_type, 町字区分コード
        4, pref, 都道府県名
        5, pref_kana, 都道府県名_カナ
        6, pref_roma, 都道府県名_英字
        7, county, 郡名
        8, county_kana, 郡名_カナ
        9, county_roma, 郡名_英字
        10, city, 市区町村名
        11, city_kana, 市区町村名_カナ
        12, city_roma, 市区町村名_英字
        13, ward, 政令市区名
        14, ward_kana, 政令市区名_カナ
        15, ward_roma, 政令市区名_英字
        16, oaza_cho, 大字・町名
        17, oaza_cho_kana, 大字・町名_カナ
        18, oaza_cho_roma, 大字・町名_英字
        19, chome, 丁目名
        20, chome_kana, 丁目名_カナ
        21, chome_number, 丁目名_数字
        22, koaza, 小字名
        23, koaza_kana, 小字名_カナ
        24, koaza_roma, 小字名_英字
        25, machiaza_dist, 同一町字識別情報
        26, rsdt_addr_flg, 住居表示フラグ
        27, rsdt_addr_mtd_code, 住居表示方式コード
        28, oaza_cho_aka_flg, 大字・町名_通称フラグ
        29, koaza_aka_code, 小字名_通称コード
        30, oaza_cho_gsi_uncmn, 大字・町名_電子国土基本図外字
        31, koaza_gsi_uncmn, 小字名_電子国土基本図外字
        32, status_flg, 状態フラグ
        33, wake_num_flg, 起番フラグ
        34, efct_date, 効力発生日
        35, ablt_date, 廃止日
        36, src_code, 原典資料コード
        37, post_code, 郵便番号
        38, remarks, 備考
        """
        reader = csv.DictReader(fin)
        for row in reader:
            citycode = row["lg_code"][0:5]
            if citycode[0:2] != pref_code:
                continue

            aza_id = row["machiaza_id"]
            if aza_id in self._processed_azaid:
                continue

            names = self.names_from_code(citycode + aza_id)
            if names is None:
                continue

            x = AddressNode.NO_COORDINATE_VALUE
            y = AddressNode.NO_COORDINATE_VALUE
            note = 'aza_id:{}'.format(aza_id)
            self.print_line_with_postcode(names, x, y, note)

    def process_lines_in_machiaza_fullset(self, fin, pref_code):
        """
        Parse lines and output address nodes in 'mt_town_fullset_all.csv'.

        1, lg_code, 全国地方公共団体コード
        2, machiaza_id, 町字ID
        3, machiaza_type, 町字区分コード
        4, pref, 都道府県名
        5, pref_kana, 都道府県名_カナ
        6, pref_roma, 都道府県名_英字
        7, county, 郡名
        8, county_kana, 郡名_カナ
        9, county_roma, 郡名_英字
        10, city, 市区町村名
        11, city_kana, 市区町村名_カナ
        12, city_roma, 市区町村名_英字
        13, ward, 政令市区名
        14, ward_kana, 政令市区名_カナ
        15, ward_roma, 政令市区名_英字
        16, oaza_cho, 大字・町名
        17, oaza_cho_kana, 大字・町名_カナ
        18, oaza_cho_roma, 大字・町名_英字
        19, chome, 丁目名
        20, chome_kana, 丁目名_カナ
        21, chome_number, 丁目名_数字
        22, koaza, 小字名
        23, koaza_kana, 小字名_カナ
        24, koaza_roma, 小字名_英字
        25, machiaza_dist, 同一町字識別情報
        26, rsdt_addr_flg, 住居表示フラグ
        27, rsdt_addr_mtd_code, 住居表示方式コード (1:街区方式,2:道路方式,0:住居表示でない)
        28, oaza_cho_aka_flg, 大字・町名_通称フラグ (1:通称,0:通称でない)
        29, koaza_aka_code, 小字名_通称コード (0:通称でない,1:通称名,2:京都通り名,3:地名情報の字または通称)
        30, oaza_cho_gsi_uncmn, 大字・町名_電子国土基本図外字
        31, koaza_gsi_uncmn, 小字名_電子国土基本図外字
        32, status_flg, 状態フラグ (0:自治体確認待ち,1:地方自治法の町若しくは字,2:町若しくは字に非該当,3:不明)
        33, wake_num_flg, 起番フラグ (1:起番,2:非起番,0:登記情報に存在しない)
        34, efct_date, 効力発生日
        35, ablt_date, 廃止日
        36, src_code, 原典資料コード
        37, post_code, 郵便番号
        38, oaza_cho_uncmn_reg, 大字・町名_登記統一文字
        39, oaza_cho_uncmn_mj, 大字・町名_MJ文字図形名
        40, oaza_cho_uncmn_type, 大字・町名_任意外字コード種別
        41, oaza_cho_uncmn_code, 大字・町名_任意外字コード
        42, koaza_cho_uncmn_reg, 小字名_登記統一文字
        43, koaza_uncmn_mj, 小字名_MJ文字図形名
        44, koaza_uncmn_type, 小字名_任意外字コード種別
        45, koaza_uncmn_code, 小字名_任意外字コード
        46, alias_oaza, 別名称大字・町名
        47, alias_oaza_kana, 別名称大字・町名_カナ
        48, alias_oaza_rome, 別名称大字・町名_英字
        49, alias_koaza, 別名称小字名
        50, alias_koaza_kana, 別名称小字名_カナ
        51, alias_koaza_rome, 別名称小字名_英字
        32, remarks, 備考
        """
        reader = csv.DictReader(fin)
        for row in reader:
            citycode = row["lg_code"][0:5]
            if citycode[0:2] != pref_code:
                continue

            aza_id = row["machiaza_id"]
            if aza_id in self._processed_azaid:
                continue

            if row["koaza_aka_code"] == "2":
                # 京都通り名はリダイレクトに変換する
                toorina = row["koaza"]
                names = self.names_from_code(
                    citycode + aza_id[0:4] + '000')  # 大字
                if names is None:
                    continue

                oaza_element = names[-1]
                if oaza_element[0] == 5:
                    city_names = copy.copy(names[0:-1])
                    oaza_path = "".join([x[1] for x in names])
                    city_path = "".join([x[1] for x in city_names])

                    # 京都市北区新町通鞍馬口上る上清蔵口町 -> 京都市北区上清蔵口町
                    toorina_names = city_names + \
                        [(5, toorina + oaza_element[1])]
                    self.print_line_with_postcode(
                        names=toorina_names,
                        x=AddressNode.NO_COORDINATE_VALUE,
                        y=AddressNode.NO_COORDINATE_VALUE,
                        note=f"ref:{oaza_path}"
                    )

                    # 京都市北区新町通鞍馬口上る -> 京都市北区
                    toorina_names = city_names + [(5, toorina)]
                    self.print_line_with_postcode(
                        names=toorina_names,
                        x=AddressNode.NO_COORDINATE_VALUE,
                        y=AddressNode.NO_COORDINATE_VALUE,
                        note=f"ref:{city_path}"
                    )
                    continue

            names = self.names_from_code(citycode + aza_id)
            if names is None:
                raise RuntimeError(
                    f"'{citycode + aza_id} is not in aza_master.")

            x, y = 999.9, 999.9
            note = 'aza_id:{}'.format(aza_id)
            self.print_line_with_postcode(names, x, y, note)

    def process_lines_06(self, fin):
        """
        Parse lines and output address nodes in 'mt_town_pos_prefxx.csv'.

        1, lg_code, 全国地方公共団体コード
        2, machiaza_id, 町字ID
        3, rsdt_addr_flg, 住居表示フラグ
        4, rep_lon, 代表点_経度
        5, rep_lat, 代表点_緯度
        6, rep_srid, 代表点_座標参照系
        7, rep_scale, 代表点_地図情報レベル
        8, rep_src_code, 代表点_原典資料コード
        9, plygn_fname, ポリゴン_ファイル名
        10, plygn_kcode, ポリゴン_キーコード
        11, plygn_fmt, ポリゴン_データフォーマット
        12, plygn_srid, ポリゴン_座標参照系
        13, plygn_scale, ポリゴン_地図情報レベル
        14, plygn_src_code, ポリゴン_原典資料コード
        15, pos_oaza_cho_chome_code, 位置参照情報_大字町丁目コード
        16, pos_data_mnt_year, 位置参照情報_データ整備年度
        17, cns_bnd_s_area_kcode, 国勢調査_境界_小地域（町丁・字等別）_KEY_CODE
        18, cns_bnd_year, 国勢調査_境界_データ整備年度
        """
        reader = csv.DictReader(fin)
        for row in reader:
            citycode = row["lg_code"][0:5]
            aza_id = row["machiaza_id"]
            names = self.names_from_code(citycode + aza_id)
            if names is None:
                logger.warning(
                    f"Aza_code '{citycode} {aza_id}' is not found. (Skipped)"
                )
                continue

            x, y = str(row["rep_lon"]), str(row["rep_lat"])
            note = 'aza_id:{}'.format(aza_id)
            if not x or not y:
                raise RuntimeError(
                    "x or y is empty. citycode={}, aza_id={}".format(
                        citycode, aza_id))

            self._processed_azaid.add(aza_id)
            self.print_line_with_postcode(
                names,
                float(x),
                float(y),
                note)

    def process_lines_in_gaiku(self, fin):
        """
        Parse lines and output address nodes in 'mt_rsdtdsp_blk_pos_pref01.csv'.

        1, lg_code, 全国地方公共団体コード
        2, machiaza_id, 町字ID
        3, blk_id, 街区ID
        4, rsdt_addr_flg, 住居表示フラグ
        5, rsdt_addr_mtd_code, 住居表示方式コード
        6, rep_lon, 代表点_経度
        7, rep_lat, 代表点_緯度
        8, rep_srid, 代表点_座標参照系
        9, rep_scale, 代表点_地図情報レベル
        10, rep_src_code, 代表点_原典資料コード
        11, plygn_fname, ポリゴン_ファイル名
        12, plygn_kcode, ポリゴン_キーコード
        13, plygn_fmt, ポリゴン_データフォーマット
        14, plygn_srid, ポリゴン_座標参照系
        15, plygn_scale, ポリゴン_地図情報レベル
        16, plygn_src_code, ポリゴン_原典資料コード
        17, pos_pref, 位置参照情報_都道府県名
        18, pos_city, 位置参照情報_市区町村名
        19, pos_oaza_cho_chome, 位置参照情報_大字・町丁目名
        20, pos_koaza_aka, 位置参照情報_小字・通称名
        21, pos_blk_prc_num, 位置参照情報_街区符号・地番
        22, pos_data_mnt_year, 位置参照情報_データ整備年度
        23, rsdt_addr_code_rdbl, 電子国土基本図（地名情報）「住居表示住所」_住所コード（可読）
        24, rsdt_addr_data_mnt_date, 電子国土基本図（地名情報）「住居表示住所」_データ整備日
        """  # noqa: E501
        crs = "EPSG:4326"
        transformer = Transformer.from_crs(crs, "EPSG:4326")
        self.blocks = {}

        # Read spatial attributes of the all records from fin_pos
        reader = csv.DictReader(fin)
        for row in reader:
            aza_code = row["lg_code"][0:5] + row["machiaza_id"]
            block_code = aza_code + row["blk_id"]
            if crs != row["rep_srid"]:
                crs = row["rep_srid"]
                transformer = Transformer.from_crs(crs, "EPSG:4326")

            y, x = transformer.transform(
                row["rep_lat"], row["rep_lon"])
            block_name = row["pos_blk_prc_num"] + \
                ("番" if row["rsdt_addr_flg"] == "1" else "番地")
            _cand = self.names_from_code(aza_code)
            if _cand is not None:
                names = copy.copy(_cand)
            else:
                # 宮城県名取市閖上のコードは '042070004000'
                # '同一丁目' は '042070004001' のはずだが町字マスタにない
                m = re.match(
                    r'(.+)([一二三四五六七八九十]+丁目)$',
                    row["pos_oaza_cho_chome"])
                if m:
                    _cand = self.names_from_code(aza_code[0:-2] + '00')
                    if _cand is not None and _cand[-1][1] == m.group(1):
                        names = copy.copy(_cand)
                        names.append((AddressLevel.AZA, m.group(2)))

            if _cand is None:
                # 町字マスターに該当する町字コードの記載がない
                names = copy.copy(self.get_lgname(aza_code))
                if names is None:
                    message = "Cannot process '{}'. ".format(fin.name)
                    message += (
                        "Reason: '{}'(code:{}/{}) is not found "
                        "in 'town_all.csv'."
                    ).format(
                        row["pos_oaza_cho_chome"],
                        row["lg_code"],
                        row["machiaza_id"],
                    )
                    raise RuntimeError(message)

                if row["pos_oaza_cho_chome"]:
                    names.append(
                        [AddressLevel.OAZA, row["pos_oaza_cho_chome"]])

                if row["pos_koaza_aka"]:
                    names.append([AddressLevel.AZA, row["pos_koaza_aka"]])

            names.append((AddressLevel.BLOCK, block_name))
            self.blocks[block_code] = names
            if not x or not y:
                raise RuntimeError("x or y is empty. blockcode={}".format(
                    block_code))

            self.print_line(names, x, y)

    def process_lines_rsdt(self, fin, fin_pos):
        """
        Parse lines and output address nodes in
        'mt_rsdtdsp_rsdt_prefxx.csv' and 'mt_rsdtdsp_rsdt_pos_prefxx.csv'.

        (fin: mt_rsdtdsp_rsdt_prefxx.csv)
        1, lg_code, 全国地方公共団体コード
        2, machiaza_id, 町字ID
        3, blk_id, 街区ID
        4, rsdt_id, 住居ID
        5, rsdt2_id, 住居2ID
        6, city, 市区町村名
        7, ward, 政令市区名
        8, oaza_cho, 大字・町名
        9, chome, 丁目名
        10, koaza, 小字名
        11, blk_num, 街区符号
        12, rsdt_num, 住居番号
        13, rsdt_num2, 住居番号2
        14, basic_rsdt_div, 基礎番号・住居番号区分
        15, rsdt_addr_flg, 住居表示フラグ
        16, rsdt_addr_mtd_code, 住居表示方式コード
        17, status_flg, 状態フラグ
        18, efct_date, 効力発生日
        19, ablt_date, 廃止日
        20, src_code, 原典資料コード
        21, remarks, 備考

        (fin_pos: mt_rsdtdsp_rsdt_pos_prefxx.csv)
        1, lg_code, 全国地方公共団体コード
        2, machiaza_id, 町字ID
        3, blk_id, 街区ID
        4, rsdt_id, 住居ID
        5, rsdt2_id, 住居2ID
        6, rsdt_addr_flg, 住居表示フラグ
        7, rsdt_addr_mtd_code, 住居表示方式コード
        8, rep_lon, 代表点_経度
        9, rep_lat, 代表点_緯度
        10, rep_srid, 代表点_座標参照系
        11, rep_scale, 代表点_地図情報レベル
        12, rep_src_code, 代表点_原典資料コード
        13, rsdt_addr_code_rdbl, 電子国土基本図（地名情報）「住居表示住所」_住所コード（可読）
        14, rsdt_addr_data_mnt_date, 電子国土基本図（地名情報）「住居表示住所」_データ整備日
        15, basic_rsdt_div, 基礎番号・住居番号区分
        """
        def __calc_codes(row: dict) -> dict:
            codes = {}
            codes["aza"] = row["lg_code"][0:5] + row["machiaza_id"]
            codes["block"] = codes["aza"] + row["blk_id"]
            codes["building"] = codes["block"] + row["rsdt_id"] + \
                (row["rsdt2_id"] or '')
            return codes

        crs = "EPSG:4326"
        transformer = Transformer.from_crs(crs, "EPSG:4326")

        # Read spatial attributes of the all records from fin_pos
        reader = csv.DictReader(fin)
        reader_pos = csv.DictReader(fin_pos)

        pos_pool = {}
        for pos_row in reader_pos:
            pos_codes = __calc_codes(pos_row)
            pos_building_code = pos_codes["building"]
            pos_pool[pos_building_code] = pos_row

        for row in reader:
            codes = __calc_codes(row)
            building_code = codes["building"]

            if building_code not in pos_pool:
                msg = "No coodinates for rsdt '{}'".format(",".join(
                    [row[k] for k in (
                        "city", "ward", "oaza_cho", "chome",
                        "koaza", "blk_num", "rsdt_num", "rsdt_num2")]))
                logger.warning(msg)
                x, y = 999.9, 999.9
            else:
                pos_row = pos_pool[building_code]
                if crs != pos_row["rep_srid"]:
                    crs = pos_row["rep_srid"]
                    transformer = Transformer.from_crs(crs, "EPSG:4326")

                y, x = transformer.transform(
                    pos_row["rep_lat"], pos_row["rep_lon"])

            if codes["block"] not in self.blocks:
                msg = "No block data for rsdt '{}'".format(",".join(
                    [row[k] for k in (
                        "city", "ward", "oaza_cho", "chome",
                        "koaza", "blk_num", "rsdt_num", "rsdt_num2")]))
                # logger.warning(msg)

                _cand = self.names_from_code(codes["aza"])
                if _cand is None:
                    names = copy.copy(self.get_lgname(codes["aza"]))
                    if names is None:
                        logger.warning((
                            "Citycode '{}' is not registered. "
                            "Skip record '{}'."
                        ).format(
                            codes["aza"][0:5],
                            ','.join(row)
                        ))
                        continue

                    if row["oaza_cho"]:
                        names.append([AddressLevel.OAZA, row["oaza_cho"]])

                    if row["koaza"]:
                        names.append([AddressLevel.AZA, row["koaza"]])

                else:
                    names = copy.copy(_cand)

                self.set_lgname(codes["aza"], names)
                if row["blk_num"]:
                    # block_name = row["blk_num"] + \
                    #     ("番" if row["rsdt_addr_flg"] == "1" else "番地")
                    block_name = row["blk_num"] + "番"
                    names.append((AddressLevel.BLOCK, block_name))

            else:
                names = copy.copy(self.blocks[codes["block"]])

            if row["rsdt_num2"]:
                name = row["rsdt_num"] + "－"
                names.append((AddressLevel.BLD, name))
                name = row["rsdt_num2"] + (
                    "号" if row["rsdt_addr_flg"] == "1" else "")
                names.append((AddressLevel.BLD, name))
            else:
                name = row["rsdt_num"] + (
                    "号" if row["rsdt_addr_flg"] == "1" else "")
                names.append((AddressLevel.BLD, name))

            if not x or not y:
                raise RuntimeError("x or y is empty. buildingcode={}".format(
                    building_code))

            self.print_line(names, x, y)

    def process_lines_chiban(self, fin, fin_pos):
        """
        Parse lines and output address nodes in
        'mt_parcel_cityxxxxxx.csv' and 'mt_parcel_pos_cityxxxxxx.csv'.

        (fin: mt_parcel_cityxxxxxx.csv)
        1, lg_code, 全国地方公共団体コード
        2, machiaza_id, 町字ID
        3, prc_id, 地番ID
        4, city, 市区町村名
        5, ward, 政令市区名
        6, oaza_cho, 大字・町名
        7, chome, 丁目名
        8, koaza, 小字名
        9, machiaza_dist, ? (仕様書未記載)
        10, prc_num1, 地番1
        12, prc_num2, 地番2
        12, prc_num3, 地番3
        13, rsdt_addr_flg, 住居表示フラグ
        14, prc_rec_flg, 地番レコード区分フラグ
        15, prc_area_code, 地番区域コード
        16, efct_date, 効力発生日
        17, ablt_date, 廃止日
        18, src_code, 原典資料コード
        19, remarks, 備考
        20, real_prop_num, 不動産番号

        (fin_pos: mt_parcel_pos_cityxxxxxx.csv)
        1, lg_code, 全国地方公共団体コード
        2, machiaza_id, 町字ID
        3, prc_id, 地番ID
        4, rep_lon, 代表点_経度
        5, rep_lat, 代表点_緯度
        6, rep_srid, 代表点_座標参照系
        7, rep_scale, 代表点_地図情報レベル
        8, rep_src_code, 代表点_原典資料コード
        9, plygn_fname, ポリゴン_ファイル名
        10, plygn_kcode, ポリゴン_キーコード
        11, plygn_fmt, ポリゴン_データフォーマット
        12, plygn_srid, ポリゴン_座標参照系
        13, plygn_scale, ポリゴン_地図情報レベル
        14, plygn_src_code, ポリゴン_原典資料コード
        15, moj_map_city_code, 法務省地図_市区町村コード
        16, moj_map_oaza_code, 法務省地図_大字コード
        17, moj_map_chome_code, 法務省地図_丁目コード
        18, moj_map_koaza_code, 法務省地図_小字コード
        19, moj_map_spare_code, 法務省地図_予備コード
        20, moj_map_brushid, 法務省地図_筆id
        """
        def __calc_codes(row: dict) -> dict:
            codes = {}
            codes["aza"] = row["lg_code"][0:5] + row["machiaza_id"]
            codes["parcel"] = codes["aza"] + row["prc_id"]
            return codes

        crs = "EPSG:4326"
        transformer = Transformer.from_crs(crs, "EPSG:4326")
        reader = csv.DictReader(fin)
        pos_pool = {}

        # Read spatial attributes in to memory from fin_pos
        if fin_pos is not None:
            reader_pos = csv.DictReader(fin_pos)
            for pos_row in reader_pos:
                pos_codes = __calc_codes(pos_row)
                pos_prc_id = pos_codes["parcel"]
                pos_pool[pos_prc_id] = pos_row

        for row in reader:
            codes = __calc_codes(row)
            prc_id = codes["parcel"]
            if prc_id not in pos_pool:
                # line = ",".join(
                #     [row[k] for k in (
                #         "city", "ward", "oaza_cho", "chome",
                #         "koaza", "prc_num1", "prc_num2", "prc_num3")])
                # msg = f"No coodinates for parcel '{line}'"
                # logger.warning(msg)
                x, y = 999.9, 999.9
            else:
                pos_row = pos_pool[prc_id]
                if crs != pos_row["rep_srid"]:
                    crs = pos_row["rep_srid"]
                    transformer = Transformer.from_crs(crs, "EPSG:4326")

                lat, lon = float(pos_row["rep_lat"]), float(pos_row["rep_lon"])
                if lat > 100:
                    lat, lon = lon, lat  # 2025年度版で経緯度が反転している不具合

                y, x = transformer.transform(lat, lon)

            _cand = self.names_from_code(codes["aza"])
            if _cand is None:
                # 町字ID が「町字マスター」にない場合
                # 市区町村名まで町字マスター記載の表記を利用し、
                # 大字以下を追加する
                names = copy.copy(self.get_lgname(codes["aza"]))
                if names is None:
                    logger.warning((
                        "Citycode '{}' is not registered. "
                        "Skip record '{}'."
                    ).format(
                        codes["aza"][0:5],
                        ','.join(row)
                    ))
                    continue

                if row["oaza_cho"]:
                    names.append([AddressLevel.OAZA, row["oaza_cho"]])

                if row["koaza"]:
                    names.append([AddressLevel.AZA, row["koaza"]])

            else:
                names = copy.copy(_cand)

            self.set_lgname(codes["aza"], names)
            if row["prc_num1"]:
                # block_name = row["prc_num1"] + \
                #     ("番" if row["rsdt_addr_flg"] == "1" else "番地")
                block_name = row["prc_num1"] + "番地"
                names.append((AddressLevel.BLOCK, block_name))

            if row["prc_num3"]:
                name = row["prc_num2"] + "－"
                names.append((AddressLevel.BLD, name))
                name = row["prc_num2"] + "－" + row["prc_num3"]
                names.append((AddressLevel.BLD, name))
            elif row["prc_num2"]:
                names.append((AddressLevel.BLD, row["prc_num2"]))

            if not x or not y:
                raise RuntimeError(
                    "x or y is empty. buildingcode={}".format(prc_id))

            self.print_line(names, x, y)

    def convert(self):
        """
        Read records from 'mt_town_pos_prefxx.csv' file, format them,
        then output to 'text/xx_basereg_town.txt'.
        """
        self.prepare_jiscode_table()

        for pref_code in self.targets:
            pref_path = self.input_dir / pref_code

            # 町字マスター
            output_filepath = self.output_dir / \
                f"{pref_code}_basereg_town.txt.bz2"
            if output_filepath.exists():
                logger.info(f"SKIP: {output_filepath}")
            else:
                input_filepath = pref_path / \
                    f"mt_town_pos_pref{pref_code}.csv.zip"

                self._processed_azaid = set()
                with bz2.open(
                        filename=output_filepath,
                        mode='wt',
                        encoding='utf-8'
                    ) as fout, \
                        self.manager.open_csv_in_zipfile(
                            input_filepath) as fin:
                    self.fp = fout
                    self.process_lines_06(fin)

                zip_filename = self.input_dir / "mt_town_fullset_all.csv.zip"
                with bz2.open(
                        filename=output_filepath,
                        mode='at', encoding='utf-8'
                    ) as fout, \
                        self.manager.open_csv_in_zipfile(zip_filename) as fin:
                    self.fp = fout
                    self.process_lines_in_machiaza_fullset(fin, pref_code)

            # 住居表示－街区マスター位置参照拡張
            output_filepath = self.output_dir / \
                f'{pref_code}_basereg_blk.txt.bz2'
            output_filepath_rsdt = self.output_dir / \
                f'{pref_code}_basereg_rsdt.txt.bz2'
            if output_filepath.exists() and \
                    output_filepath_rsdt.exists():
                logger.info(f"SKIP: {output_filepath}")
            else:
                input_filepath = pref_path / \
                    f'mt_rsdtdsp_blk_pos_pref{pref_code}.csv.zip'
                with bz2.open(
                        filename=output_filepath,
                        mode='wt',
                        encoding='utf-8'
                    ) as fout, \
                        self.manager.open_csv_in_zipfile(
                            input_filepath) as fin:
                    self.fp = fout
                    self.process_lines_in_gaiku(fin)

            # 住居表示・住居マスター，位置参照拡張
            if output_filepath_rsdt.exists():
                logger.info(f"SKIP: {output_filepath}")
            else:
                input_filepath = pref_path / \
                    f'mt_rsdtdsp_rsdt_pref{pref_code}.csv.zip'
                input_pos_filepath = pref_path / \
                    f'mt_rsdtdsp_rsdt_pos_pref{pref_code}.csv.zip'
                with bz2.open(
                        filename=output_filepath_rsdt,
                        mode="wt",
                        encoding='utf-8'
                    ) as fout, \
                        self.manager.open_csv_in_zipfile(
                            input_filepath) as fin, \
                        self.manager.open_csv_in_zipfile(
                            input_pos_filepath) as fin_pos:
                    self.fp = fout
                    self.process_lines_rsdt(fin, fin_pos)

            # 地番マスター，位置参照拡張
            output_filepath_parcel = self.output_dir / \
                f'{pref_code}_basereg_parcel.txt.bz2'
            if output_filepath_parcel.exists():
                logger.info(f"SKIP: {output_filepath_parcel}")
            else:
                for zippath in pref_path.glob('mt_parcel_city*.csv.zip'):
                    zippath_pos = pref_path / zippath.name.replace(
                        'parcel_city', 'parcel_pos_city')
                    if not zippath_pos.exists():
                        logger.error(
                            f"SKIP: {zippath_pos} not exists.")
                        continue

                    with bz2.open(
                            filename=output_filepath_parcel,
                            mode="at",
                            encoding='utf-8'
                        ) as fout, \
                            self.manager.open_csv_in_zipfile(zippath) as fin, \
                            self.manager.open_csv_in_zipfile(
                                zippath_pos) as fin_pos:
                        self.fp = fout
                        self.process_lines_chiban(fin, fin_pos)

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
        Now all address-base data files except chiban-master are
        zipped in one file. The file should be already downloaded by
        BaseConverter.get_address_all(), but confirm here.

        - 全国
            https://data.address-br.digital.go.jp/mt_town/mt_town_all.csv.zip  町字マスター (不要)
            https://data.address-br.digital.go.jp/mt_city/mt_city_all.csv.zip  市区町村マスター (不要)
            https://data.address-br.digital.go.jp/mt_pref/mt_pref_all.csv.zip  都道府県マスター (不要)
            https://data.address-br.digital.go.jp/mt_city_pos/mt_city_pos_all.csv.zip  市区町村マスター位置参照拡張 (不要)
            https://data.address-br.digital.go.jp/mt_pref_pos/mt_pref_pos_all.csv.zip  都道府県マスター位置参照拡張 (不要)
            https://data.address-br.digital.go.jp/mt_town_fullset/mt_town_fullset_all.csv.zip  町字マスターフルセット (base_converter で取得)

        - 都道府県別
            https://data.address-br.digital.go.jp/mt_town_pos/pref/mt_town_pos_pref05.csv.zip  町字マスター位置参照拡張
            https://data.address-br.digital.go.jp/mt_rsdtdsp_blk/pref/mt_rsdtdsp_blk_pref31.csv.zip  住居表示-街区マスター
            https://data.address-br.digital.go.jp/mt_rsdtdsp_blk_pos/pref/mt_rsdtdsp_blk_pos_pref31.csv.zip  住居表示-街区マスター位置参照拡張
            https://data.address-br.digital.go.jp/mt_rsdtdsp_rsdt/pref/mt_rsdtdsp_rsdt_pref31.csv.zip  住居表示-住居マスター
            https://data.address-br.digital.go.jp/mt_rsdtdsp_rsdt_pos/pref/mt_rsdtdsp_rsdt_pos_pref31.csv.zip  住居表示―住居マスター位置参照拡張

        - 市区町村別
            https://data.address-br.digital.go.jp/mt_parcel/city/mt_parcel_city312011.csv.zip  地番マスター
            https://data.address-br.digital.go.jp/mt_parcel_pos/city/mt_parcel_pos_city312011.csv.zip  地番マスター位置参照拡張

        """  # noqa: E501

        # Download files by prefecture
        for pref in range(1, 48):
            pref_code = f"{pref:02d}"
            logger.info(
                f"Downloading base-registry datafiles (for pref {pref_code}).")
            download_dir = Path(self.input_dir) / pref_code
            urls = [
                f"https://data.address-br.digital.go.jp/mt_town_pos/pref/mt_town_pos_pref{pref_code}.csv.zip",  # noqa: E501
                f"https://data.address-br.digital.go.jp/mt_rsdtdsp_blk/pref/mt_rsdtdsp_blk_pref{pref_code}.csv.zip",  # noqa: E501
                f"https://data.address-br.digital.go.jp/mt_rsdtdsp_blk_pos/pref/mt_rsdtdsp_blk_pos_pref{pref_code}.csv.zip",  # noqa: E501
                f"https://data.address-br.digital.go.jp/mt_rsdtdsp_rsdt/pref/mt_rsdtdsp_rsdt_pref{pref_code}.csv.zip",  # noqa: E501
                f"https://data.address-br.digital.go.jp/mt_rsdtdsp_rsdt_pos/pref/mt_rsdtdsp_rsdt_pos_pref{pref_code}.csv.zip",  # noqa: E501
            ]
            errors = self.download(
                urls=urls, dirname=download_dir, overwrite=False
            )
            if len(errors) > 0:
                for e in errors:
                    logger.error(
                        "HTTP {} error downloading '{}': {}".format(
                            e[0], e[1], e[2]))

                raise

        # Download files by city
        citycodes = self.get_citycode_list()
        for code in citycodes:
            logger.info(
                f"Downloading base-registry datafiles (for city {code}).")
            pref_code = code[0:2]
            download_dir = Path(self.input_dir) / pref_code
            parcel_url = f"https://data.address-br.digital.go.jp/mt_parcel/city/mt_parcel_city{code}.csv.zip"  # noqa: E501
            pos_url = f"https://data.address-br.digital.go.jp/mt_parcel_pos/city/mt_parcel_pos_city{code}.csv.zip"  # noqa: E501
            errors = self.download(
                urls=[parcel_url, pos_url],
                dirname=download_dir,
                overwrite=False,
            )

            if len(errors) > 0:
                for e in errors:
                    logger.error(
                        "HTTP {} error downloading '{}': {}".format(
                            e[0], e[1], e[2]))
