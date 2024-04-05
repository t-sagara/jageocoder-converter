"""
街区レベル位置参照情報の検証を行う。
Usage: python3 validate_gaiku.py
"""
import csv
import glob
import io
import logging
from pathlib import Path
import zipfile

logger = logging.getLogger(__name__)
known_errors = {}


def validate_line(fname: str, lineno: int, row: dict):
    gaiku = row["街区符号・地番"]
    if fname not in known_errors:
        known_errors[fname] = {}

    if "," in gaiku:
        if False and 'mn' in known_errors[fname]:
            pass
        else:
            print(f"{fname}[行{lineno:,}] 街区符号・地番に複数の値 '{gaiku}'")
            known_errors[fname]['mn'] = True

    if "." in gaiku:
        if False and 'rv' in known_errors[fname]:
            pass
        else:
            print(f"{fname}[行{lineno:,}] 街区符号・地番に実数値 '{gaiku}'")
            known_errors[fname]['rv'] = True


def process_files():
    zipfiles = glob.glob(Path(__file__).parent.parent /
                         'download/gaiku/*.0a.zip')
    zipfiles.sort()
    for zipfilepath in zipfiles:
        with zipfile.ZipFile(zipfilepath) as z:
            for filename in z.namelist():
                if not filename.lower().endswith('.csv'):
                    continue

                with z.open(filename, mode='r') as f:
                    ft = io.TextIOWrapper(
                        f, encoding='CP932', newline='',
                        errors='backslashreplace')
                    reader = csv.DictReader(ft)
                    pre_args = None
                    logger.debug('Processing {} in {}...'.format(
                        filename, zipfilepath))
                    lineno = 0
                    try:
                        for row in reader:
                            # args = [
                            #     row['都道府県名'],
                            #     row['市区町村名'],
                            #     row['大字・丁目名'],
                            #     row['小字・通称名'],
                            #     row['街区符号・地番'],
                            #     row['座標系番号'],
                            #     row['Ｘ座標'],
                            #     row['Ｙ座標'],
                            #     row['緯度'],
                            #     row['経度'],
                            #     row['住居表示フラグ'],
                            #     row['代表フラグ'],
                            #     row['更新前履歴フラグ'],
                            #     row['更新後履歴フラグ'],
                            # ]
                            validate_line(filename, lineno, row)
                            pre_args = row
                            lineno += 1

                    except UnicodeDecodeError:
                        raise RuntimeError((
                            "変換できない文字が見つかりました。"
                            "処理中のファイルは {}, "
                            "直前の行は次の通りです。\n{}").format(
                                filename, pre_args))


if __name__ == '__main__':
    process_files()
