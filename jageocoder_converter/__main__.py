import jageocoder_converter
import logging
import os
from docopt import docopt

HELP = """
Convert location reference information to jageocoder dictionary.

Usage:
  {p} [-h]
  {p} convert [-dq] [--no-oaza] [--no-gaiku] [--no-jusho] \
    [--db-dir=<dir>] [--download-dir=<dir>] [--textdata-dir=<dir>] \
    [<prefcodes>...]

Options:
  -h --help       Show this help.
  -d --debug      Show debug messages.
  -q --quiet      Quiet mode. Skip confirming the terms of use.
  --no-oaza       Don't use 大字・町丁目レベル位置参照情報.
  --no-gaiku      Don't use 街区レベル位置参照情報.
  --no-jusho      Don't use 電子国土基本図「住居表示住所」.
  --db-dir=<dir>        Dictionary creation directory.
  --download-dir=<dir>  Directory to download location reference information [default: download]
  --textdata-dir=<dir>  Directory to store text format data [default: text]
  prefcodes       List of prefecture codes to be included in the dictionary.
                  If omitted, all prefectures will be included.

Example:

  python -m {p} convert --no-jusho --db-dir=test 11 12 13 14

  will create a dictionary under 'test' directory including
  埼玉県, 千葉県, 東京都, 神奈川県 from 大字・町丁目レベル and
  街区レベル位置参照情報, but not 住居表示住所.
""".format(p='jageocoder_converter')

if __name__ == '__main__':
    args = docopt(HELP)

    if args['--debug']:
        logging.basicConfig(level=logging.DEBUG)

    basedir = os.getcwd()
    kwargs = {
        'use_oaza': not args['--no-oaza'],
        'use_gaiku': not args['--no-gaiku'],
        'use_jusho': not args['--no-jusho'],
        'download_dir': os.path.join(
            basedir, args['--download-dir']),
        'textdata_dir': os.path.join(
            basedir, args['--textdata-dir']),
        'quiet': args['--quiet'],
    }
    if args['--db-dir']:
        kwargs['db_dir'] = os.path.join(
            basedir, args['--db-dir'])
    else:
        kwargs['db_dir'] = None

    if len(args['<prefcodes>']) == 0:
        kwargs['prefs'] = None
    else:
        kwargs['prefs'] = args['<prefcodes>']

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
