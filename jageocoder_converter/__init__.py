from logging import getLogger
import os
from typing import Optional, List, Union

__version__ = '2.0.3'

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

__all__ = [
    BaseConverter,
    CityConverter,
    OazaConverter,
    GaikuConverter,
    JushoConverter,
    DataManager
]

PathLike = Union[str, bytes, os.PathLike]
logger = getLogger(__name__)


def __prepare_postcoder(directory: PathLike):
    """
    Create an instance of PostCoder

    Parameters
    ----------
    directory: PathLike
        The postalcode file directory.
        If the file (ken_all.zip) doesn't exist, download it.
    """
    postcoder = PostCoder.get_instance(directory)
    return postcoder


def convert(
    prefs: Optional[List[str]] = None,
    use_postcode: bool = True,
    use_geolod: bool = True,
    use_oaza: bool = True,
    use_gaiku: bool = True,
    use_geolonia: bool = True,
    use_jusho: bool = True,
    use_basereg: bool = True,
    use_chiban: bool = True,
    db_dir: Optional[PathLike] = None,
    download_dir: Optional[PathLike] = None,
    textdata_dir: Optional[PathLike] = None,
    quiet: bool = False,
) -> PathLike:
    basedir = os.getcwd()
    download_dir = download_dir if download_dir else os.path.join(
        basedir, 'download')
    output_dir = textdata_dir if textdata_dir else os.path.join(
        basedir, 'text')
    jageocoder_converter.config.base_download_dir = download_dir

    os.makedirs(download_dir, mode=0o755, exist_ok=True)
    os.makedirs(output_dir, mode=0o755, exist_ok=True)

    targets = prefs  # Process all prefectures

    # Create data manager
    manager = DataManager(
        db_dir=db_dir or jageocoder.get_db_dir(mode='w'),
        text_dir=output_dir,
        targets=targets)

    # Prepare a converter for the target data set
    converters = []

    converter = CityConverter(
        manager=manager,
        input_dir=os.path.join(download_dir, 'geonlp'),
        output_dir=output_dir,
        priority=1,
        targets=targets,
        quiet=quiet
    )
    if use_oaza:
        converter.unescape_texts('city')
        converters.append(converter)
    else:
        converter.escape_texts('city')

    converter = OazaConverter(
        manager=manager,
        input_dir=os.path.join(download_dir, 'oaza'),
        output_dir=output_dir,
        priority=8,
        targets=targets,
        quiet=quiet
    )
    if use_oaza:
        converter.unescape_texts('oaza')
        converters.append(converter)
    else:
        converter.escape_texts('oaza')

    converter = GaikuConverter(
        manager=manager,
        input_dir=os.path.join(download_dir, 'gaiku'),
        output_dir=output_dir,
        priority=3,
        targets=targets,
        quiet=quiet
    )
    if use_gaiku:
        converter.unescape_texts('gaiku')
        converters.append(converter)
    else:
        converter.escape_texts('gaiku')

    converter = GeoloniaConverter(
        manager=manager,
        input_dir=os.path.join(download_dir, 'geolonia'),
        output_dir=output_dir,
        priority=2,
        targets=targets,
        quiet=quiet
    )
    if use_geolonia:
        converter.unescape_texts('geolonia')
        converters.append(converter)
    else:
        converter.escape_texts('geolonia')

    converter = JushoConverter(
        manager=manager,
        input_dir=os.path.join(
            download_dir, 'jusho'),
        output_dir=output_dir,
        priority=4,
        targets=targets,
        quiet=quiet
    )
    if use_jusho:
        converter.unescape_texts('jusho')
        converters.append(converter)
    else:
        converter.escape_texts('jusho')

    converter = BaseRegistryConverter(
        manager=manager,
        input_dir=os.path.join(
            download_dir, 'base_registry'),
        output_dir=output_dir,
        priority=9,
        targets=targets,
        quiet=quiet
    )
    if use_basereg:
        converter.unescape_texts('basereg_town')
        converter.unescape_texts('basereg_blk')
        converter.unescape_texts('basereg_rsdt')
        converters.append(converter)
    else:
        converter.escape_texts('basereg_town')
        converter.escape_texts('basereg_blk')
        converter.escape_texts('basereg_rsdt')

    converter = ChibanConverter(
        manager=manager,
        input_dir=os.path.join(download_dir, 'chiban'),
        output_dir=output_dir,
        priority=7,
        targets=targets,
        quiet=quiet
    )
    if use_chiban:
        converter.unescape_texts('chiban')
        converters.append(converter)
    else:
        converter.escape_texts('chiban')

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
        __prepare_postcoder(os.path.join(download_dir, 'japanpost'))

    # Converts location reference information from various sources
    # into the text format.
    aza_data_dir = os.path.join(download_dir, 'base_registry')
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

    return db_dir
