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
                use_chiban=True,
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
    html: str = rf'''<!doctype html>
<html lang="ja-JP">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.0.2/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-EVSTQN3/azprG1Anm3QDgpJLIm9Nao0Yz1ztcQTwFspd3yD65VohhpuuCOmLASjC" crossorigin="anonymous">
    <title>Jageocoder Datafile list</title>
    <style type="text/css">
    .mono {{
        font-family: monospace, serif;
    }}
    </style>
  </head>
  <body>
    <div class="container">
        <h1>Jageocoder データファイル一覧 ({published[0]}-{published[1]}-{published[2]}版)</h1>
        <div class="row"justify-content-md-center>
            <div class="col-0 col-md-1 col-lg-1"></div>
            <div class="col-12 col-md-10 col-lg-10">
            <ul>
            <li>ここにあるファイルは
                <a href="https://t-sagara.github.io/jageocoder/" target="_blank">
                jageocoder </a> で利用する「住所データファイル」です。
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
        '<table class="table table-striped">'
        '<thead><tr>'
        '<th scope="col">#</th>'
        '<th scope="col">ファイル名</th>'
        '<th scope="col">レベル</th>'
        '<th scope="col">地域</th>'
        '<th scope="col">対応バージョン</th>'
        '<th scope="col">サイズ(B)</th>'
        '<th scope="col">sha1</th>'
        '</tr></thead>'
        '<tbody>'
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
        vers = "1.3, 1.4" if args.group(3) == "14" else "2.0"
        content += (
            '<tr>'
            f'<th scope="row">{i + 1}</th>'
            f'<td><a href="{filename}">{filename}</a></td>'
            f'<td>{level}</td>'
            f'<td>{area}</td>'
            f'<td>{vers}</td>'
            f'<td class="text-end mono">{filesize:,}</td>'
            f'<td class="mono">{sha1}</td>'
            '</tr>'
        )

    content += "</tbody></table>"
    html = html.replace('<!-- Content here -->', content)
    return html


if __name__ == "__main__":
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

    if len(sys.argv) < 2:
        base_db_dir = Path("./") / "db_{}".format(
            date.today().strftime("%Y%m%d")
        )
    else:
        base_db_dir = Path(sys.argv[1])

    all_prefs = ["{:02d}".format(x) for x in range(1, 48)] + [None]
    build_gaiku(base_db_dir, targets=[None])
    build_jukyo(base_db_dir, targets=all_prefs)
    create_zipfiles(base_db_dir)

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
