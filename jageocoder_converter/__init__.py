from importlib.metadata import version
from logging import getLogger
from pathlib import Path
from typing import Optional, List

__version__ = '2.1.3'

import jageocoder
import jageocoder_converter.config
from jageocoder_converter.base_converter import BaseConverter
from jageocoder_converter.city_converter import CityConverter
from jageocoder_converter.oaza_converter import OazaConverter
from jageocoder_converter.gaiku_converter import GaikuConverter
from jageocoder_converter.geolonia_converter import GeoloniaConverter
from jageocoder_converter.jusho_converter import JushoConverter
from jageocoder_converter.base_registry import BaseRegistryConverter
from jageocoder_converter.chiban_converter import ChibanConverter
from jageocoder_converter.data_manager import DataManager
from jageocoder_converter.postcoder import PostCoder


__version__ = version("jageocoder-converter")  # The package version
__all__ = [
    "BaseConverter",
    "CityConverter",
    "OazaConverter",
    "GaikuConverter",
    "JushoConverter",
    "DataManager",
]

logger = getLogger(__name__)


def __prepare_postcoder(directory: Path):
    """
    Create an instance of PostCoder

    Parameters
    ----------
    directory: Path
        The postalcode file directory.
        If the file (ken_all.zip) doesn't exist, download it.
    """
    postcoder = PostCoder.get_instance(directory)
    return postcoder


def convert(
    prefs: Optional[List[str]] = None,
    use_postcode: bool = True,
    # use_geolod: bool = True,
    use_oaza: bool = True,
    use_gaiku: bool = True,
    use_geolonia: bool = True,
    use_jusho: bool = True,
    use_basereg: bool = True,
    use_chiban: bool = True,
    db_dir: Optional[Path] = None,
    download_dir: Optional[Path] = None,
    textdata_dir: Optional[Path] = None,
    quiet: bool = False,
) -> Path:
    basedir = Path.cwd()
    download_dir = download_dir if download_dir else basedir / 'download'
    output_dir = textdata_dir if textdata_dir else basedir / 'text'
    jageocoder_converter.config.base_download_dir = download_dir

    download_dir.mkdir(mode=0o755, exist_ok=True)
    output_dir.mkdir(mode=0o755, exist_ok=True)

    targets = prefs  # Process all prefectures

    # Create data manager
    _db_dir = db_dir if db_dir is not None else jageocoder.get_db_dir(mode='w')
    if _db_dir is None:
        raise RuntimeError("Can't determin 'db_dir'")
    manager = DataManager(
        db_dir=_db_dir,
        text_dir=output_dir,
        targets=targets)

    # Prepare a converter for the target data set
    converters = []

    if use_oaza:
        converter = CityConverter(
            manager=manager,
            input_dir=download_dir / 'geonlp',
            output_dir=output_dir,
            priority=1,
            targets=targets,
            quiet=quiet
        )
        BaseConverter.unescape_texts(output_dir, 'city')
        converters.append(converter)
    else:
        BaseConverter.escape_texts(output_dir, 'city')

    if use_oaza:
        converter = OazaConverter(
            manager=manager,
            input_dir=download_dir / 'oaza',
            output_dir=output_dir,
            priority=8,
            targets=targets,
            quiet=quiet
        )
        BaseConverter.unescape_texts(output_dir, 'oaza')
        converters.append(converter)
    else:
        BaseConverter.escape_texts(output_dir, 'oaza')

    if use_gaiku:
        converter = GaikuConverter(
            manager=manager,
            input_dir=download_dir / 'gaiku',
            output_dir=output_dir,
            priority=3,
            targets=targets,
            quiet=quiet
        )
        BaseConverter.unescape_texts(output_dir, 'gaiku')
        converters.append(converter)
    else:
        BaseConverter.escape_texts(output_dir, 'gaiku')

    if use_geolonia:
        converter = GeoloniaConverter(
            manager=manager,
            input_dir=download_dir / 'geolonia',
            output_dir=output_dir,
            priority=2,
            targets=targets,
            quiet=quiet
        )
        BaseConverter.unescape_texts(output_dir, 'geolonia')
        converters.append(converter)
    else:
        BaseConverter.escape_texts(output_dir, 'geolonia')

    if use_jusho:
        converter = JushoConverter(
            manager=manager,
            input_dir=download_dir / 'jusho',
            output_dir=output_dir,
            priority=4,
            targets=targets,
            quiet=quiet
        )
        BaseConverter.unescape_texts(output_dir, 'jusho')
        converters.append(converter)
    else:
        BaseConverter.escape_texts(output_dir, 'jusho')

    if use_basereg:
        converter = BaseRegistryConverter(
            manager=manager,
            input_dir=download_dir / 'base_registry',
            output_dir=output_dir,
            priority=9,
            targets=targets,
            quiet=quiet
        )
        BaseConverter.unescape_texts(output_dir, 'basereg_town')
        BaseConverter.unescape_texts(output_dir, 'basereg_blk')
        BaseConverter.unescape_texts(output_dir, 'basereg_rsdt')
        BaseConverter.unescape_texts(output_dir, 'basereg_parcel')
        converters.append(converter)
    else:
        BaseConverter.escape_texts(output_dir, 'basereg_town')
        BaseConverter.escape_texts(output_dir, 'basereg_blk')
        BaseConverter.escape_texts(output_dir, 'basereg_rsdt')
        BaseConverter.escape_texts(output_dir, 'basereg_parcel')

    if use_chiban:
        converter = ChibanConverter(
            manager=manager,
            input_dir=download_dir / 'chiban',
            output_dir=output_dir,
            priority=7,
            targets=targets,
            quiet=quiet
        )
        BaseConverter.unescape_texts(output_dir, 'chiban')
        converters.append(converter)
    else:
        BaseConverter.escape_texts(output_dir, 'chiban')

    # Confirm acceptance of terms of uses.
    for converter in converters:
        converter.confirm()

    # Download data
    logger.info("データファイルをダウンロードします。")
    for converter in converters:
        converter.download_files()

    # Prpare PostCode table
    if use_postcode:
        logger.info("郵便番号テーブルを作成します。")
        __prepare_postcoder(download_dir / 'japanpost')

    # Converts location reference information from various sources
    # into the text format.
    aza_data_dir = download_dir / 'base_registry'
    converters[0].get_address_all(aza_data_dir)
    manager.prepare_aza_table(aza_data_dir)
    for converter in converters:
        logger.info("{} で変換処理を実行中".format(converter))
        converter.convert()

    # Sort data, register to the database, then create index
    manager.write_datasets(converters)
    logger.info("データベースファイルを作成します。")
    manager.register()
    manager.create_index()

    return _db_dir
