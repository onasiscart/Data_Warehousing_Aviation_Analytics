from dw import DW
import extract
import transform
import load

if __name__ == "__main__":
    # create a data warehouse object
    dw = DW(create=True)
    # fill it with data extracted and transformed from sources provided
    load.load(dw, transform.transform(extract.extract()))
    dw.close()
