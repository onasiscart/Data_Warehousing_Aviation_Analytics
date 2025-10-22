import extract
from dw import DW


if __name__ == "__main__":
    dw = DW(create=False)
    print(dw.debug_tdr())
    result = extract.debug_baseline_tdr()
    for row in result:
        print(row)
