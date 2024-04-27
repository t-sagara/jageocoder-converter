from datetime import date
import glob
import hashlib
import logging
from pathlib import Path
import re
import shutil
import sys
from typing import List, Union

import jageocoder
import jageocoder_converter

logger = logging.getLogger(__name__)

versions = re.search(r"(\d+)\.(\d+)\.(.+)", jageocoder.__version__)
ver = "v" + versions.group(1) + versions.group(2)


def build_gaiku(base_db_dir: Path, targets: List[Union[str, None]]):
    global ver

    for pref in targets:
        if pref is not None:
            prefs = [pref]
            db_dir = base_db_dir / f"gaiku_{pref:s}_{ver:s}"
        else:
            prefs = None
            db_dir = base_db_dir / f"gaiku_all_{ver:s}"

        if db_dir.exists():
            logger.debug(f"Skip building '{db_dir}'.")
        else:
            jageocoder_converter.convert(
                prefs=prefs,
                use_oaza=True,
                use_gaiku=True,
                use_geolonia=True,
                use_jusho=False,
                use_chiban=False,
                use_basereg=False,
                db_dir=db_dir,
                download_dir=None,
                textdata_dir=None,
                quiet=True,
            )

        readme = db_dir / "README.md"
        if readme.exists():
            readme.unlink()

        with open(Path(__file__).parent / "doc/README-gaiku.md", "r") as fin, \
                open(readme, "w") as fout:
            fout.write(fin.read())


def build_jukyo(base_db_dir: Path, targets: List[Union[str, None]]):
    global ver

    for pref in targets:
        if pref is not None:
            prefs = [pref]
            db_dir = base_db_dir / f"jukyo_{pref:s}_{ver:s}"
        else:
            prefs = None
            db_dir = base_db_dir / f"jukyo_all_{ver:s}"

        if db_dir.exists():
            logger.debug(f"Skip building '{db_dir}'.")
        else:
            jageocoder_converter.convert(
                prefs=prefs,
                use_oaza=True,
                use_gaiku=True,
                use_geolonia=True,
                use_jusho=True,
                use_chiban=False,
                use_basereg=True,
                db_dir=db_dir,
                download_dir=None,
                textdata_dir=None,
                quiet=True,
            )

        readme = db_dir / "README.md"
        if readme.exists():
            readme.unlink()

        with open(Path(__file__).parent / "doc/README-jukyo.md", "r") as fin, \
                open(readme, "w") as fout:
            fout.write(fin.read())


def create_zipfiles(base_db_dir: Path):
    # Create zip files
    for v1_dir in glob.glob(str(base_db_dir / "*_v1*")):
        for fname in ("README.md", "address.db", "address.trie"):
            target = Path(v1_dir) / fname
            if not target.exists():
                logger.warning(f"'{target}' does not exists.")
                break

        else:  # When all files exist
            dest_dir = (base_db_dir / "v1")
            dest_dir.mkdir(mode=0o755, exist_ok=True)
            target = dest_dir / Path(v1_dir).name
            if target.with_suffix(".zip").exists():
                logger.info(f"File '{target}' exists, skip archiving.")
            else:
                logger.info(f"Archiving '{v1_dir}'")
                shutil.make_archive(
                    base_name=target,
                    format="zip",
                    root_dir=v1_dir,
                )

    for v2_dir in glob.glob(str(base_db_dir / "*_v2*")):
        for fname in (
            "README.md",
            "address.trie",
            "aza_master",
            "dataset",
            "trienode",
        ):
            target = Path(v2_dir) / fname
            if not target.exists():
                logger.warning(f"'{target}' does not exists.")
                break

        else:  # When all files exist
            dest_dir = (base_db_dir / "v2")
            dest_dir.mkdir(mode=0o755, exist_ok=True)
            target = dest_dir / Path(v2_dir).name
            if target.with_suffix(".zip").exists():
                logger.info(f"File '{target}' exists, skip archiving.")
            else:
                logger.info(f"Archiving '{v2_dir}'")
                shutil.make_archive(
                    base_name=target,
                    format="zip",
                    root_dir=v2_dir,
                )


def filelist_html(base_db_dir: Path) -> str:
    g = re.search(r"(\d{4}).*(\d{2}).*(\d{2})", base_db_dir.parent.name)
    published = (g.group(1), g.group(2), g.group(3))
    prefs = {
        "01": "北海道", "02": "青森県", "03": "岩手県", "04": "宮城県",
        "05": "秋田県", "06": "山形県", "07": "福島県", "08": "茨城県",
        "09": "栃木県", "10": "群馬県", "11": "埼玉県", "12": "千葉県",
        "13": "東京都", "14": "神奈川県", "15": "新潟県", "16": "富山県",
        "17": "石川県", "18": "福井県", "19": "山梨県", "20": "長野県",
        "21": "岐阜県", "22": "静岡県", "23": "愛知県", "24": "三重県",
        "25": "滋賀県", "26": "京都府", "27": "大阪府", "28": "兵庫県",
        "29": "奈良県", "30": "和歌山県", "31": "鳥取県", "32": "島根県",
        "33": "岡山県", "34": "広島県", "35": "山口県", "36": "徳島県",
        "37": "香川県", "38": "愛媛県", "39": "高知県", "40": "福岡県",
        "41": "佐賀県", "42": "長崎県", "43": "熊本県", "44": "大分県",
        "45": "宮崎県", "46": "鹿児島県", "47": "沖縄県"
    }
    html: str = r'''<!doctype html>
<html lang="ja-JP">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-EVSTQN3/azprG1Anm3QDgpJLIm9Nao0Yz1ztcQTwFspd3yD65VohhpuuCOmLASjC" crossorigin="anonymous">
    <title>Jageocoder Datafile list</title>
    <style type="text/css">
    .mono {
        font-family: monospace, serif;
    }
    </style>
  </head>
  <body>
    <div class="container">
        <h1>Jageocoder データファイル一覧</h1>
        <div class="row"justify-content-md-center>
            <div class="col-0 col-md-1 col-lg-1"></div>
            <div class="col-12 col-md-10 col-lg-10">
            <ul>
            <li>ここにあるファイルは
                <a href="https://t-sagara.github.io/jageocoder/" target="_blank">
                jageocoder </a> で利用する「住所データベースファイル」です。
            </li>
            <li>このデータを複製、転載したり、利用した結果を公開する場合には、
                データ提供元を記載するなどの条件があります。
                必ず同梱されている "README.md" を読んで利用条件を確認してください。
            </li>
            </ul>
            <div class="col-0 col-md-1 col-lg-1"></div>
            </div>
        </div>
        <!-- Content here -->
    </div>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/js/bootstrap.bundle.min.js" integrity="sha384-MrcW6ZMFYlzcLA8Nl+NtUVF0sA7MsXsP1UyJoMp4YLEuNSfAP+JcXn/tWtIaxVXM" crossorigin="anonymous"></script>
  </body>
</html>
'''  # noqa: E501
    # <th>ファイル名</th><th>サイズ(B)</th><th>レベル</th>
    # <th>地域</th><th>Jageocoderバージョン</th></tr></thead>
    content = (
        '<table class="table table-striped"><thead>\n'
        '<tr>\n'
        '  <th scope="col">#</th>\n'
        '  <th scope="col">作成日</th>\n'
        '  <th scope="col">ファイル名</th>\n'
        '  <th scope="col">レベル</th>\n'
        '  <th scope="col">地域</th>\n'
        '  <th scope="col">対応バージョン</th>\n'
        '  <th scope="col">サイズ(B)</th>\n'
        '  <th scope="col">sha1</th>\n'
        '</tr>\n'
        '</thead><tbody>\n'
    )
    for i, datafile in enumerate(sorted(glob.glob(str(base_db_dir / "*_v??.zip")))):
        datafile: Path = Path(datafile)
        filesize = datafile.stat().st_size
        filename = datafile.name
        with open(datafile, "rb") as f:
            sha1 = hashlib.sha1(f.read()).hexdigest()

        args = re.match(r"([a-z]+)_(all|\d{2})_v(\d{2}).zip", filename)
        if args is None:
            continue

        level = "街区" if args.group(1) == "gaiku" else "住居表示"
        area = "全国" if args.group(2) == "all" else prefs[args.group(2)]
        if args.group(3) == "14":
            vers = "1.3, 1.4"
        elif args.group(3) == "20":
            vers = "2.0, 2.1"
        else:
            vers = "2.1"

        content += (
            '<tr>\n'
            f'  <th scope="row">{i + 1}</th>\n'
            f'  <td>{"-".join(published)}</td>\n'
            f'  <td><a href="{filename}">{filename}</a></td>\n'
            f'  <td>{level}</td>\n'
            f'  <td>{area}</td>\n'
            f'  <td>{vers}</td>\n'
            f'  <td class="text-end mono">{filesize:,}</td>\n'
            f'  <td class="mono">{sha1}</td>\n'
            '</tr>\n'
        )

    content += "</tbody></table>\n"
    html = html.replace('<!-- Content here -->', content)
    return html


if __name__ == "__main__":
    import sys
    do_build_gaiku = '--gaiku' in sys.argv[1:] or '--all' in sys.argv[1:]
    do_build_jukyo = '--jukyo' in sys.argv[1:] or '--all' in sys.argv[1:]
    do_create_zip = '--zip' in sys.argv[1:] or '--all' in sys.argv[1:]
    do_create_index = '--index' in sys.argv[1:] or '--all' in sys.argv[1:]
    if do_build_gaiku | do_build_jukyo | do_create_zip | do_create_index:
        pass
    else:
        print((
            f"Usage: python {sys.argv[0]} [--db-dir=<dbdir>] "
            "[--gaiku] [--jukyo] [--zip] [--index] [--all]"
        ))
        exit(1)

    # Set logger
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(
        logging.Formatter('%(levelname)s:%(name)s:%(lineno)s:%(message)s')
    )
    for target in ('jageocoder', 'jageocoder_converter',):
        logger = logging.getLogger(target)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(console_handler)

    for argv in sys.argv[1:]:
        if argv.startswith('--db-dir='):
            base_db_dir = Path(argv[9:])
            break

    else:
        base_db_dir = Path("./") / "db_{}".format(
            date.today().strftime("%Y%m%d")
        )

    all_prefs = ["{:02d}".format(x) for x in range(1, 48)] + [None]
    if do_build_gaiku:
        build_gaiku(base_db_dir, targets=[None])

    if do_build_jukyo:
        # build_jukyo(base_db_dir, targets=all_prefs)
        build_jukyo(base_db_dir, targets=[None])

    if do_create_zip:
        create_zipfiles(base_db_dir)

    if do_create_index:
        v1_dir = base_db_dir / "v1"
        if v1_dir.exists():
            html = filelist_html(v1_dir)
            with open(v1_dir / "index.html", "w") as f:
                f.write(html)

        v2_dir = base_db_dir / "v2"
        if v2_dir.exists():
            html = filelist_html(v2_dir)
            with open(v2_dir / "index.html", "w") as f:
                f.write(html)
