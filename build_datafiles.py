from datetime import date
import glob
import logging
from pathlib import Path
import re
import shutil
import sys

import jageocoder
import jageocoder_converter

logger = logging.getLogger(__name__)

versions = re.search(r"(\d+)\.(\d+)\.(.+)", jageocoder.__version__)
ver = "v" + versions.group(1) + versions.group(2)


def build_gaiku(base_db_dir: Path):
    global ver

    for pref in [None]:
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


def build_jukyo(base_db_dir: Path):
    global ver

    all_prefs = ["{:02d}".format(x) for x in range(1, 48)] + [None]

    for pref in all_prefs:
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

    build_gaiku(base_db_dir)
    build_jukyo(base_db_dir)

    # Create zip files
    if versions.group(1) == "1":
        for v1_dir in glob.glob(str(base_db_dir / "*_v1*")):
            for fname in ("README.md", "address.db", "address.trie"):
                target = Path(v1_dir) / fname
                if not target.exists():
                    logger.warning(f"'{target}' does not exists.")
                    break

            else:  # When all files exist
                logger.info(f"Archiving '{v1_dir}'")
                shutil.make_archive(
                    base_name=v1_dir,
                    format="zip",
                    root_dir=v1_dir,
                )

    if versions.group(1) == "2":
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
                logger.info(f"Archiving '{v2_dir}'")
                shutil.make_archive(
                    base_name=v2_dir,
                    format="zip",
                    root_dir=v2_dir,
                )
