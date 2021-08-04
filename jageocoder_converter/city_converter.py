import csv
import json
import os
from typing import Union, NoReturn, Optional, List

from jageocoder.address import AddressLevel

from jageocoder_converter import BaseConverter


class CityConverter(BaseConverter):
    """
    A converter to generate formatted text data of prefecture and city
    from GeoNLP CSV data.

    Output 'output/xx_city.txt' for each prefecture.
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
        self.records = {}

    def read_pref_file(self):
        """
        Read 'japan_pref.csv'
        """
        input_filepath = os.path.join(
            self.input_dir, 'japan_pref.csv')
        if not os.path.exists(input_filepath):
            self.download(urls=['file://' + os.path.join(
                os.path.dirname(__file__), 'data/japan_pref.csv')],
                dirname=self.input_dir)

        with open(input_filepath, 'r', encoding='cp932', newline='') as f:
            reader = csv.reader(f)
            for rows in reader:
                if rows[0] == 'geonlp_id':
                    continue

                jiscode, name, lon, lat = rows[1], rows[6], rows[11], rows[12]
                self.records[jiscode] = [[
                    [[AddressLevel.PREF, name]], lon, lat,
                    'jisx0401:'+jiscode]]

                # Register names that omit '都', '府' and '県' also
                name = rows[2]
                if name != '北海':
                    self.records[jiscode].append([
                        [[AddressLevel.PREF, name]],
                        lon, lat, 'jisx0401:'+jiscode])

    def read_city_file(self):
        """
        Read 'geoshape-city.csv'
        """
        input_filepath = os.path.join(
            self.input_dir, 'geoshape-city.csv')
        if not os.path.exists(input_filepath):
            self.download(
                urls=['http://agora.ex.nii.ac.jp/GeoNLP/dict/geoshape-city.csv'],
                dirname=self.input_dir,
                notes=(
                    "「歴史的行政区域データセットβ版地名辞書」をダウンロードします。\n"
                    "利用条件等は https://geonlp.ex.nii.ac.jp/dictionary/geoshape-city/ "
                    "を確認してください。\n"
                )
            )

        jiscodes = {}
        with open(input_filepath, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f)
            head = {}
            for rows in reader:
                if rows[0] in ('geonlp_id', 'entry_id'):
                    for i, row in enumerate(rows):
                        head[row] = i

                    continue

                for pref in rows[head['prefname']].split('/'):
                    for county in rows[head['countyname']].split('/'):

                        body = rows[head['body']] + rows[head['suffix']]
                        suffix = rows[head['suffix']]
                        lon = rows[head['longitude']]
                        lat = rows[head['latitude']]
                        jiscode = rows[head['code']]

                        if len(jiscode) < 5:  # 境界未確定地域
                            continue

                        jiscode = jiscode[0:5]

                        level = AddressLevel.CITY
                        if suffix == '区' and pref != '東京都':
                            level = AddressLevel.WORD

                        names = [[AddressLevel.PREF, pref]]
                        if body != county and county != '':
                            names.append([AddressLevel.COUNTY, county])

                        names.append([level, body])

                        if lon and lat:
                            self.records[jiscode[0:2]].append(
                                [names, lon, lat, 'jisx0402:' + jiscode])

                        if jiscode not in jiscodes:
                            jiscodes[jiscode] = [names, rows[head['valid_to']]]
                            continue

                        if jiscodes[jiscode][1] == '':
                            continue

                        if rows[head['valid_to']] == '' or \
                                rows[head['valid_to']] > jiscodes[jiscode][1]:
                            jiscodes[jiscode] = [names, rows[head['valid_to']]]

        with open(self.get_jiscode_json_path(), 'w', encoding='utf-8') as f:
            for jiscode, args in jiscodes.items():
                print(json.dumps({jiscode: args[0]}, ensure_ascii=False),
                      file=f)

    def write_city_files(self):
        """
        Output 'output/xx_city.txt'
        """
        for pref_code in self.targets:
            with open(os.path.join(
                    self.output_dir, '{}_city.txt'.format(pref_code)),
                    'w') as fout:

                self.set_fp(fout)
                for record in self.records[pref_code]:
                    self.print_line(*record)

    def convert(self):
        self.records = {}
        self.read_pref_file()
        self.read_city_file()
        self.write_city_files()
