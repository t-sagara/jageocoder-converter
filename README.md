# jageocoder-converter

This package is a CUI application for creating an address dictionary for use with the address geocoder [jageocoder](https://github.com/t-sagara/jageocoder).
It can be used when you want to create a dictionary that includes only some prefectures.

For more detailed information, please refer to
[the Japanese document](doc/index.rst).

### Prerequisites

Requires Python 3.7 or later.

### Installing

Install the package using `pip install jageocoder-converter`.

## Uninstalling

Remove the directory containing the database.
Then, do `pip uninstall jageocoder-converter`.

## Run

Show help with the following command.

```sh
python -m jageocoder_converter -h
``` 

To create a dictionary, run the following command.

```sh
python -m jageocoder_converter
```

During the process, it downloads the location reference information
needed to create the dictionary from the web. Before downloading,
you will be prompted with a link to the terms of use.
Be sure to read them, and enter a capital 'Y'.

**More examples**

To create a dictionary, including only 東京都,
without records from 住居表示住所, run the following command.

```sh
python -m jageocoder_converter --no-jusho 13
```

You may create a dictionary in the specified directory.

```sh
python -m jageocoder_converter --db-dir=oazadb --no-gaiku --no-jusho
```

If you already understand the terms of use, etc., and want to process it
in the background, you can specify the quiet option. It may be useful
to add a debug option to check the progress in the log file.

```sh
nohup python -m jageocoder_converter --quiet --debug &
```

## Authors

* Takeshi SAGARA - [Info-proto Co.,Ltd.](https://www.info-proto.com/)

## License

This project is licensed under [the MIT License](https://opensource.org/licenses/mit-license.php).

This is not the scope of the dictionary data license.
Please follow the terms of use of the respective dictionary data.
