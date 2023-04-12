from datetime import date
import logging
from pathlib import Path
import re

import jageocoder
import jageocoder_converter

logger = logging.getLogger(__name__)

versions = re.search(r"(\d+)\.(\d+)\.(.+)", jageocoder.__version__)
ver = "v" + versions.group(1) + versions.group(2)
base_db_dir = Path("./") / "db_{}".format(
    date.today().strftime("%Y%m%d")
)


def build_gaiku():
    global ver
    global base_db_dir

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

        readme.hardlink_to(
            Path(__file__).parent / "doc/README-gaiku.md"
        )


def build_jukyo():
    global ver
    global base_db_dir

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

        readme.hardlink_to(
            Path(__file__).parent / "doc/README-jukyo.md"
        )


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

    build_gaiku()
    build_jukyo()
