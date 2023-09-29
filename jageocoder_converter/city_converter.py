import csv
import json
from logging import getLogger
import os
from typing import Union, Optional, List

from jageocoder.address import AddressLevel

from jageocoder_converter import BaseConverter
from jageocoder_converter.data_manager import DataManager

logger = getLogger(__name__)


class CityConverter(BaseConverter):
    """
    A converter to generate formatted text data of prefecture and city
    from GeoNLP CSV data.

    Output 'output/xx_city.txt' for each prefecture.
    """
    dataset_name = "歴史的行政区域データセットβ版地名辞書"
    dataset_url = "https://geonlp.ex.nii.ac.jp/dictionary/geoshape-city/"

    def __init__(self,
                 output_dir: Union[str, bytes, os.PathLike],
                 input_dir: Union[str, bytes, os.PathLike],
                 manager: Optional[DataManager] = None,
                 priority: Optional[int] = None,
                 targets: Optional[List[str]] = None,
                 quiet: Optional[bool] = False) -> None:
        super().__init__(
            manager=manager, priority=priority, targets=targets, quiet=quiet)
        self.output_dir = output_dir
        self.input_dir = input_dir
        self.records = {}

    def confirm(self) -> bool:
        """
        Show the terms of the license agreement and confirm acceptance.
        """
        terms = (
            "「歴史的行政区域データセットβ版地名辞書」をダウンロードします。\n"
            "利用条件等は {url} を確認してください。\n"
        ).format(url='https://geonlp.ex.nii.ac.jp/dictionary/geoshape-city/')
        return super().confirm(terms)

    def download_files(self) -> None:
        """
        Download data files.
        """
        input_filepath = os.path.join(
            self.input_dir, 'geoshape-pref-geolod-2021.csv')
        if not os.path.exists(input_filepath):
            # Copy file to the download directory
            src = os.path.join(os.path.dirname(
                __file__), 'data/geoshape-pref-geolod-2021.csv')
            dst = os.path.join(
                self.input_dir, 'geoshape-pref-geolod-2021.csv')
            dst_dir = os.path.dirname(dst)
            if not os.path.exists(dst_dir):
                logger.debug('Create directory {}'.format(dst_dir))
                os.makedirs(dst_dir, mode=0o755)

            with open(src, 'rb') as f:
                content = f.read()

            with open(dst, 'wb') as f:
                f.write(content)

        input_filepath = os.path.join(
            self.input_dir, 'geoshape-city-geolod.csv')
        if not os.path.exists(input_filepath):
            self.download(
                urls=[
                    'https://geonlp.ex.nii.ac.jp/dictionary/geoshape-city/geoshape-city-geolod.csv',
                    # 'http://agora.ex.nii.ac.jp/GeoNLP/dict/geoshape-city.csv'
                ],
                dirname=self.input_dir
            )

    def read_pref_file(self):
        """
        Read 'geoshape-pref-geolod-2021.csv'
        """
        input_filepath = os.path.join(
            self.input_dir, 'geoshape-pref-geolod-2021.csv')
        with open(input_filepath, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f)
            for rows in reader:
                if rows[0] in ('geonlp_id', 'entry_id', 'geolod_id'):
                    continue

                jiscode, name = rows[1], rows[6]
                lon, lat = rows[11], rows[12]
                code = rows[8]
                self.records[jiscode] = [[
                    [[AddressLevel.PREF, name]], lon, lat, code]]

                # Register names that omit '都', '府' and '県' also
                name = rows[2]
                if name != '北海':
                    self.records[jiscode].append([
                        [[AddressLevel.PREF, name]],
                        lon, lat, code])

    def read_city_file(self):
        """
        Read 'geoshape-city-geolod.csv'
        """
        input_filepath = os.path.join(
            self.input_dir, 'geoshape-city-geolod.csv')
        jiscodes = {}
        with open(input_filepath, 'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f)
            head = {}

            # 政令市が指定前と指定後で別のコードを持っている問題と、
            # 大阪市北区が合併して名称が変わらずコードだけ変わった問題に
            # 対応するため、先に名前とコードの対応表を作る。
            names_to_jiscodes = {}
            city_records = {}

            for rows in reader:
                if rows[0] in ('geonlp_id', 'entry_id', 'geolod_id'):
                    for i, row in enumerate(rows):
                        head[row] = i

                    continue

                for pref in rows[head['prefname']].split('/'):
                    for county in rows[head['countyname']].split('/'):

                        city_id = rows[head['entry_id']]
                        suffix = rows[head['suffix']].rstrip('/')
                        body = rows[head['body']] + suffix
                        lon = rows[head['longitude']]
                        lat = rows[head['latitude']]
                        code = rows[head['code']]
                        valid_to = rows[head['valid_to']]

                        if len(code) < 5:  # 境界未確定地域
                            continue

                        level = AddressLevel.CITY
                        if suffix == '区' and pref != '東京都':
                            level = AddressLevel.WARD

                        names = [[AddressLevel.PREF, pref]]
                        if body != county and county != '':
                            if level == AddressLevel.WARD:
                                names.append([AddressLevel.CITY, county])
                            else:
                                names.append([AddressLevel.COUNTY, county])

                        names.append([level, body])

                        if lon and lat:
                            prefcode = city_id[0:2]
                            if prefcode not in city_records:
                                city_records[prefcode] = []

                            city_records[prefcode].append(
                                [names, lon, lat, 'geoshape_city_id:' + city_id])

                        if city_id[5] == 'A':
                            key = ''.join([x[1] for x in names])
                            jiscode = city_id[0:5]
                            if key not in names_to_jiscodes:
                                names_to_jiscodes[key] = []

                            names_to_jiscodes[key].append([
                                jiscode,
                                valid_to if valid_to != '' else '2999-12-31'
                            ])

                            if jiscode not in jiscodes:
                                jiscodes[jiscode] = [names, valid_to]
                                continue

                            if jiscodes[jiscode][1] == '':
                                continue

                            if valid_to == '' or valid_to > jiscodes[jiscode][1]:
                                jiscodes[jiscode] = [names, valid_to]

        # 自治体名から対応するコードのリストを取得し、
        # ノート欄に上書きする。
        for prefcode, records in city_records.items():
            registered = set()
            for record in records:
                key = ''.join([x[1] for x in record[0]])
                if key in registered:
                    continue

                if key in names_to_jiscodes:
                    jiscodes_desc_order = sorted(
                        names_to_jiscodes[key],
                        key=lambda x: x[1],
                        reverse=True
                    )
                    if jiscodes_desc_order[0][0] not in record[3]:
                        # 最新の市区町村のレコードではないのでスキップ
                        continue

                    new_note = record[3] + '/' + '/'.join([
                        'jisx0402:' + x[0] for x in jiscodes_desc_order])
                else:
                    new_note = record[3]

                self.records[prefcode].append(
                    [record[0], record[1], record[2], new_note]
                )
                registered.add(key)

        with open(self.get_jiscode_json_path(), 'w', encoding='utf-8') as f:
            for jiscode, args in jiscodes.items():
                print(json.dumps(
                    {jiscode: args[0]}, ensure_ascii=False), file=f)

    def write_city_files(self):
        """
        Output 'output/xx_city.txt'
        """
        for pref_code in self.targets:
            with open(os.path.join(
                    self.output_dir, '{}_city.txt'.format(pref_code)),
                    mode='w', encoding='utf-8') as fout:

                self.set_fp(fout)
                for record in self.records[pref_code]:
                    self.print_line_with_postcode(*record)

    def convert(self):
        self.records = {}
        self.read_pref_file()
        self.read_city_file()
        self.write_city_files()
