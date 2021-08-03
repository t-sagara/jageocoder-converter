import json
import logging
import os

from jageocoder_converter import \
    CityConverter, OazaConverter, GaikuConverter, JushoConverter, \
    DataManager

if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)

    basedir = os.path.dirname(__file__)
    download_dir = os.path.join(basedir, 'download')
    output_dir = os.path.join(basedir, 'textdata')
    os.makedirs(output_dir, mode=0o755, exist_ok=True)

    targets = None  # Process all prefectures
    # targets = ['11', '12', '13', '14']

    # Converts location reference information from various sources
    # into the text format.
    CityConverter(
        input_dir=os.path.join(download_dir, 'geonlp'),
        output_dir=output_dir,
        priority=1,
        targets=targets,
    ).convert()

    if True:
        OazaConverter(
            input_dir=os.path.join(download_dir, 'oaza'),
            output_dir=output_dir,
            priority=9,
            targets=targets,
        ).convert()

    if True:
        GaikuConverter(
            input_dir=os.path.join(download_dir, 'gaiku'),
            output_dir=output_dir,
            priority=2,
            targets=targets,
        ).convert()

    if True:
        JushoConverter(
            input_dir=os.path.join(
                download_dir, 'jusho'),
            output_dir=output_dir,
            priority=3,
            targets=targets,
        ).convert()

    # Create a jageocoder dictionary from the text data.
    manager = DataManager(
        db_dir='dbtest',
        text_dir=output_dir,
        targets=targets)
    manager.register()
    manager.create_index()
