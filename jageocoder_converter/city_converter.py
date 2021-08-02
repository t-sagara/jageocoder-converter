import csv
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
                 targets: Optional[List[str]] = None) -> NoReturn:
        super().__init__(priority=priority, targets=targets)
        self.output_dir = output_dir
        self.input_dir = input_dir
        self.records = {}

    def read_pref_file(self):
        """
        Read 'geonlp/japan_pref.csv'
        """
        input_filepath = os.path.join(
            self.input_dir, 'japan_pref.csv')
        with open(input_filepath, 'r', encoding='cp932', newline='') as f:
            reader = csv.reader(f)
            for rows in reader:
                if rows[0] == 'geonlp_id':
                    continue

                jiscode, name, lon, lat = rows[1], rows[6], rows[11], rows[12]
                self.records[jiscode] = [
                    [[[AddressLevel.PREF, name]], lon, lat]]

                # Register names that omit '都', '府' and '県' also
                name = rows[2]
                if name != '北海':
                    self.records[jiscode].append(
                        [[[AddressLevel.PREF, name]], lon, lat])

    def read_city_file(self):
        """
        Read 'geonlp/japan_city.csv'
        """
        input_filepath = os.path.join(
            self.input_dir, 'japan_city.csv')
        with open(input_filepath, 'r', encoding='cp932', newline='') as f:
            reader = csv.reader(f)
            for rows in reader:
                if rows[0] == 'geonlp_id':
                    continue

                jiscode = rows[1]
                names = self.jiscodes[jiscode]
                lon, lat = rows[11], rows[12]

                if lon and lat:
                    self.records[jiscode[0:2]].append(
                        [names, lon, lat])

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
