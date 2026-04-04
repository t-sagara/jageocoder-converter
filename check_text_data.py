"""
Check *.txt.bz2 files.
"""
import bz2
import csv
import glob
from pathlib import Path


def check_bz2(target: Path):
    with bz2.open(target, "rt", newline="") as bzin:
        basename = target.name
        reader = csv.reader(bzin)
        for i, row in enumerate(reader):
            nfields = len(row)
            if nfields > 20:
                print((
                    f"File '{basename}', line {i}, "
                    f"too many fields ({nfields})."
                ))


def main():
    target_path = Path(__file__).absolute().parent / "text/*.txt.bz2"
    for target in glob.glob(str(target_path)):
        target = Path(target)
        basename = target.name
        print(f"Processing '{basename}'...")
        check_bz2(target)


if __name__ == "__main__":
    main()
