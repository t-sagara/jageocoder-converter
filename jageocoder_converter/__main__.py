import jageocoder_converter
import logging
import os
from docopt import docopt

HELP = """
Convert location reference information to jageocoder dictionary.

Usage:
  {p} [-h]
  {p} [-d] [-q] [--no-postcode] [--no-geolod] [--no-oaza] [--no-gaiku] \
      [--no-geolonia] [--no-jusho] [--no-basereg] [--no-chiban] \
      [--db-dir=<dir>] [--output-dir=<dir>] [--download-dir=<dir>] \
      [--textdata-dir=<dir>] [<prefcodes>...]

Options:
  -h --help       Show this help.
  -d --debug      Show debug messages.
  -q --quiet      Quiet mode. Skip confirming the terms of use.
  --no-postcode   Don't assign postcode.
  --no-geolod     Don't use 歴史的行政区域データセットβ版地名辞書
  --no-oaza       Don't use 大字・町丁目レベル位置参照情報.
  --no-gaiku      Don't use 街区レベル位置参照情報.
  --no-geolonia   Don't use Geolonia 住所データ.
  --no-jusho      Don't use 電子国土基本図「住居表示住所」.
  --no-basereg    Don't use JDA Address Base Registry.
  --no-chiban     Don't use 法務省登記所備付地図.
  --db-dir=<dir>        Dictionary creation directory. [default: db]
  --output-dir=<dir>    Parent directory of download-dir and textdata-dir
                        [default: ./]
  --download-dir=<dir>  Directory to download location reference information
                        [default: download]
  --textdata-dir=<dir>  Directory to store text format data [default: text]
  prefcodes       List of prefecture codes to be included in the dictionary.
                  If omitted, all prefectures will be included.

Example:

  python -m {p} --no-jusho --db-dir=test 11 12 13 14

  will create a dictionary under 'test' directory including
  埼玉県, 千葉県, 東京都, 神奈川県 from 大字・町丁目レベル and
  街区レベル位置参照情報, but not 住居表示住所.
""".format(p='jageocoder_converter')

if __name__ == '__main__':
    args = docopt(HELP)
    if args['--debug']:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    # Set logger
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(
        logging.Formatter('%(levelname)s:%(name)s:%(lineno)s:%(message)s')
    )
    for target in ('jageocoder', 'jageocoder_converter',):
        logger = logging.getLogger(target)
        logger.setLevel(log_level)
        logger.addHandler(console_handler)

    # Set parameters
    kwargs = {
        'use_postcode': not args['--no-postcode'],
        'use_geolod': not args['--no-geolod'],
        'use_oaza': not args['--no-oaza'],
        'use_gaiku': not args['--no-gaiku'],
        'use_geolonia': not args['--no-geolonia'],
        'use_jusho': not args['--no-jusho'],
        'use_basereg': not args['--no-basereg'],
        'use_chiban': not args['--no-chiban'],
        'quiet': args['--quiet'],
    }

    # Set paths
    basedir = os.getcwd()
    output_dir = args['--output-dir']

    if args['--db-dir'] is None:
        kwargs['db_dir'] = None
    elif os.path.isabs(args['--db-dir']):
        kwargs['db_dir'] = args['--db-dir']
    else:
        kwargs['db_dir'] = os.path.join(
            output_dir, args['--db-dir']
        )

    if os.path.isabs(args['--download-dir']):
        kwargs['download_dir'] = args['--download-dir']
    else:
        kwargs['download_dir'] = os.path.join(
            output_dir, args['--download-dir']
        )

    if os.path.isabs(args['--textdata-dir']):
        kwargs['textdata_dir'] = args['--textdata-dir']
    else:
        kwargs['textdata_dir'] = os.path.join(
            output_dir, args['--textdata-dir']
        )

    # Set target prefectures
    if len(args['<prefcodes>']) == 0:
        kwargs['prefs'] = None
    else:
        kwargs['prefs'] = args['<prefcodes>']

    # Run converters
    db_dir = jageocoder_converter.convert(**kwargs)

    print("Finished. The dictionary created in {}.".format(
        os.path.abspath(db_dir)))
    print((
        "You may delete '{d}/' containing downloaded files "
        "and '{t}/' containg text files created during "
        "the conversion process.").format(
        d=os.path.abspath(kwargs['download_dir']),
        t=os.path.abspath(kwargs['textdata_dir'])
    ))
