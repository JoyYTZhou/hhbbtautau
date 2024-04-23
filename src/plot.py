from utils.rootutil import DataLoader
from config.selectionconfig import cleansetting

def postprocess():
    DataLoader.hadd_cfs()
    DataLoader.hadd_roots()

def getcf():
    DataLoader.merge_cf()

def getobj():
    dl = DataLoader()
    dl.get_objs()

if __name__ == '__main__':
    postprocess()
    # getcf()
    