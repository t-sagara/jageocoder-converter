import os
from typing import Optional, List, Union

import jageocoder
from jageocoder_converter.base_converter import BaseConverter
from jageocoder_converter.city_converter import CityConverter
from jageocoder_converter.oaza_converter import OazaConverter
from jageocoder_converter.gaiku_converter import GaikuConverter
from jageocoder_converter.jusho_converter import JushoConverter
from jageocoder_converter.data_manager import DataManager

__all__ = [
    BaseConverter,
    CityConverter,
    OazaConverter,
    GaikuConverter,
    JushoConverter,
    DataManager,
]

PathLike = Union[str, bytes, os.PathLike]


def convert(
    prefs: Optional[List[str]] = None,
    use_oaza: bool = True,
    use_gaiku: bool = True,
    use_jusho: bool = True,
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

    os.makedirs(download_dir, mode=0o755, exist_ok=True)
    os.makedirs(output_dir, mode=0o755, exist_ok=True)

    targets = prefs  # Process all prefectures
    # targets = ['11', '12', '13', '14']

    # Converts location reference information from various sources
    # into the text format.
    CityConverter(
        input_dir=os.path.join(download_dir, 'geonlp'),
        output_dir=output_dir,
        priority=1,
        targets=targets,
        quiet=quiet,
    ).convert()

    if use_oaza:
        OazaConverter(
            input_dir=os.path.join(download_dir, 'oaza'),
            output_dir=output_dir,
            priority=9,
            targets=targets,
            quiet=quiet,
        ).convert()

    if use_gaiku:
        GaikuConverter(
            input_dir=os.path.join(download_dir, 'gaiku'),
            output_dir=output_dir,
            priority=2,
            targets=targets,
            quiet=quiet,
        ).convert()

    if use_jusho:
        JushoConverter(
            input_dir=os.path.join(
                download_dir, 'jusho'),
            output_dir=output_dir,
            priority=3,
            targets=targets,
            quiet=quiet,
        ).convert()

    # Create a jageocoder dictionary from the text data.
    if db_dir is None:
        db_dir = jageocoder.get_db_dir(mode='w')

    manager = DataManager(
        db_dir=db_dir,
        text_dir=output_dir,
        targets=targets)
    manager.register()
    manager.create_index()

    return db_dir
