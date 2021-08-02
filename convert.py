import json
import logging
import os

from jageocoder_converter import \
    CityConverter, OazaConverter, GaikuConverter, JushoConverter

if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)

    basedir = os.path.dirname(__file__)
    output_dir = os.path.join(basedir, 'output')
    os.makedirs(output_dir, mode=0o755, exist_ok=True)

    targets = ['02']

    if True:
        CityConverter(
            input_dir=os.path.join(basedir, 'geonlp'),
            output_dir=output_dir,
            priority=1,
            targets=targets,
        ).convert()

    if True:
        OazaConverter(
            input_dir=os.path.join(basedir, 'oaza'),
            output_dir=output_dir,
            priority=9,
            targets=targets,
        ).convert()

    if True:
        GaikuConverter(
            input_dir=os.path.join(basedir, 'gaiku'),
            output_dir=output_dir,
            priority=2,
            targets=targets,
        ).convert()

    if True:
        JushoConverter(
            input_dir=os.path.join(
                basedir, 'saigai.gsi.go.jp/jusho/download/data/'),
            output_dir=output_dir,
            priority=3,
            targets=targets,
        ).convert()
