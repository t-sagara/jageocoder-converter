import csv
import glob
import io
import logging
import zipfile

logger = logging.getLogger(__name__)
known_errors = {}


def validate_line(fname: str, lineno: int, row: dict):
    if fname not in known_errors:
        known_errors[fname] = {}

    if len(row["都道府県コード"]) != 2:
        if 'pcode' in known_errors[fname]:
            pass
        else:
            pcode = row["都道府県コード"]
            print(f"{fname}[行{lineno:,}] 都道府県コードが2桁ではない '{pcode}'")
            known_errors[fname]['pcode'] = True

    if len(row["市区町村コード"]) != 5:
        if 'ccode' in known_errors[fname]:
            pass
        else:
            ccode = row["市区町村コード"]
            print(f"{fname}[行{lineno:,}] 市区町村コードが5桁ではない '{ccode}'")
            known_errors[fname]['ccode'] = True

    lat = row["緯度"]
    try:
        lat = float(lat)
        if lat >= 20.0 and lat <= 50.0 or 'lat' in known_errors[fname]:
            pass
        else:
            print(f"{fname}[行{lineno:,}] 緯度が範囲外 '{lat:.4f}'")
            known_errors[fname]['lat'] = True
    except ValueError:
        print(f"{fname}[行{lineno:,}] 緯度が数値ではない '{lat}'")
        known_errors[fname]['lat'] = True

    lon = row["経度"]
    try:
        lon = float(lon)
        if lon >= 120.0 and lon <= 155.0 or 'lon' in known_errors[fname]:
            pass
        else:
            print(f"{fname}[行{lineno:,}] 経度が範囲外 '{lon:.4f}'")
            known_errors[fname]['lon'] = True
    except ValueError:
        print(f"{fname}[行{lineno:,}] 経度が数値ではない '{lon}'")
        known_errors[fname]['lon'] = True


def process_files():
    zipfiles = glob.glob('download/oaza/*-16.0b.zip')
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
                    lineno = 2
                    try:
                        for row in reader:
                            # args = [
                            # row['都道府県コード'],
                            # row['都道府県名'],
                            # row['市区町村コード'],
                            # row['市区町村名'],
                            # row['大字町丁目コード'],
                            # row['大字町丁目名'],
                            # row['緯度'],
                            # row['経度'],
                            # row['原典資料コード'],
                            # row['大字・字・丁目区分コード'],
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
