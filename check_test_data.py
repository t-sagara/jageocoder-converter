#!/usr/bin/env python3
"""
Jageocoder のテストで利用できる住所を検索する。
対象は downloads/ から探すため、先にダウンロードしておくこと。
"""
import csv
import glob
import io
import logging
from pathlib import Path
import re
import zipfile

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def get_test_kana_no_after_numbers():
    """
    数字の後に「の」が出現し、その次の文字が地番の先頭になる
    可能性のある文字（'甲', 'イ' 等）である例を探す。
    """
    chiban_heads = (
        '甲乙丙丁戊己庚辛壬癸'
        '子丑寅卯辰巳午未申酉戌亥'
        '続新'
        'いろはにほへとちりぬるをわかよたれそつね'
        'イロハニホヘトチリヌルヲワカヨタレソツネ')
    targets = Path(__file__).parent / "download/gaiku/*.zip"
    for target in glob.glob(str(targets)):
        with zipfile.ZipFile(target, "r") as zipf:
            for fname in zipf.namelist():
                if not fname.endswith(".csv"):
                    continue
                with zipf.open(fname, "r") as fin:
                    wrap = io.TextIOWrapper(fin, encoding='cp932', newline="")
                    reader = csv.reader(wrap)
                    for row in reader:
                        # "都道府県名","市区町村名","大字_丁目名","小字_通称名","街区符号_地番",
                        # "座標系番号","Ｘ座標","Ｙ座標","緯度","経度","住居表示フラグ",
                        # "代表フラグ","更新前履歴フラグ","更新後履歴フラグ"

                        if len(row[3]) == 1 and row[3] in chiban_heads or \
                                row[4] != "" and row[4][0] in chiban_heads:
                            if row[2].endswith("丁目"):
                                print(row)


def get_test_datsurakuchi():
    """
    地番の最後に脱落地記号を含む例を探す。
    """
    targets = Path(__file__).parent / "download/gaiku/*.zip"
    for target in glob.glob(str(targets)):
        with zipfile.ZipFile(target, "r") as zipf:
            for fname in zipf.namelist():
                if not fname.endswith(".csv"):
                    continue
                with zipf.open(fname, "r") as fin:
                    wrap = io.TextIOWrapper(fin, encoding='cp932', newline="")
                    reader = csv.reader(wrap)
                    for row in reader:
                        # "都道府県名","市区町村名","大字_丁目名","小字_通称名","街区符号_地番",
                        # "座標系番号","Ｘ座標","Ｙ座標","緯度","経度","住居表示フラグ",
                        # "代表フラグ","更新前履歴フラグ","更新後履歴フラグ"
                        m = re.match(r'\d*$', row[4])
                        if m is None:
                            print(row)


if __name__ == "__main__":
    # get_test_kana_no_after_numbers()
    get_test_datsurakuchi()
