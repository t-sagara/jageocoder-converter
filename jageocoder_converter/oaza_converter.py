import csv
import glob
import io
from logging import getLogger
import os
from typing import Union, NoReturn, Optional, List
import zipfile

from jageocoder_converter.base_converter import BaseConverter

logger = getLogger(__name__)


class OazaConverter(BaseConverter):
    """
    大字・町丁目レベル位置参照情報から大字レベルの
    整形済みテキストデータを生成するコンバータ。

    都道府県別に 'output/xx_oaza.txt' を出力します。
    """

    def __init__(self,
                 output_dir: Union[str, bytes, os.PathLike],
                 input_dir: Union[str, bytes, os.PathLike],
                 priority: Optional[int] = None,
                 targets: Optional[List[str]] = None,
                 quiet: Optional[bool] = False) -> NoReturn:
        super().__init__(priority=priority, targets=targets, quiet=quiet)
        self.output_dir = output_dir
        self.input_dir = input_dir
        self.fp = None
        self.prepare_jiscode_table()

    def process_line(self, args):
        """
        大字・町丁目レベル位置参照情報の１行を解析して住所ノードを追加する
        """
        if args[0] == '都道府県コード':
            return

        if args[2] == "07023":  # 07000-12.0b のバグ
            args[2] = "07203"

        pcode, pname, ccode, cname, isj_code, oaza, y, x = args[0:8]
        names = self.jiscodes[ccode]
        self.print_line(names + self.guessAza(oaza, ccode), x, y)

    def add_from_zipfile(self, zipfilepath):
        """
        大字・町丁目レベル位置参照情報から住所表記を登録
        """
        with zipfile.ZipFile(zipfilepath) as z:
            for filename in z.namelist():
                if not filename.lower().endswith('.csv'):
                    continue

                pre_args = None
                with z.open(filename, mode='r') as f:
                    ft = io.TextIOWrapper(
                        f, encoding='CP932', newline='',
                        errors='backslashreplace')
                    reader = csv.reader(ft)
                    logger.debug('Processing {} in {}...'.format(
                        filename, zipfilepath))
                    try:
                        for args in reader:
                            self.process_line(args)
                            pre_args = args
                    except UnicodeDecodeError:
                        raise RuntimeError((
                            "変換できない文字が見つかりました。"
                            "処理中のファイルは {}, 直前の行は次の通りです。\n{}")
                            .format(filename, pre_args))

    def convert(self):
        """
        oaza/xx000.zip を探して整形処理を行ない、
        output/xx_oaza.txt に出力する。
        """
        for pref_code in self.targets:
            output_filepath = os.path.join(
                self.output_dir, '{}_oaza.txt'.format(pref_code))
            if os.path.exists(output_filepath):
                logger.info("SKIP: {}".format(output_filepath))
                continue

            input_filepath = None
            while input_filepath is None:
                zipfiles = glob.glob(
                    os.path.join(self.input_dir,
                                 '{}000*.zip'.format(pref_code)))
                if len(zipfiles) == 0:
                    self.download_files()
                else:
                    input_filepath = zipfiles[0]

            with open(output_filepath, 'w', encoding='utf-8') as fout:
                self.set_fp(fout)
                logger.debug("Reading from {}".format(input_filepath))
                self.add_from_zipfile(input_filepath)

    def download_files(self):
        """
        Download zipped data files from
        '位置参照情報ダウンロードサービス'
        https://nlftp.mlit.go.jp/cgi-bin/isj/dls/_choose_method.cgi
        """
        urlbase = 'https://nlftp.mlit.go.jp/isj/dls/data'
        version = '14.0b'  # PY2020, 令和2年度
        urls = []
        for pref_code in self.targets:
            url = "{0}/{1}/{2}000-{1}.zip".format(
                urlbase, version, pref_code)
            urls.append(url)

        self.download(
            urls=urls,
            dirname=self.input_dir,
            notes=(
                "「大字・町丁目レベル位置参照情報」をダウンロードします。\n"
                "https://nlftp.mlit.go.jp/ksj/other/agreement.html の"
                "利用規約を必ず確認してください。\n"
            )
        )
