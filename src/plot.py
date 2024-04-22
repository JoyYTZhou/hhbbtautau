from utils.rootutil import DataLoader
from config.selectionconfig import cleansetting

def postprocess():
    dl = DataLoader()
    dl.hadd_cfs()
    dl.hadd_roots(cleancfg=cleansetting, wgt_dict=dl.wgt_dict)

def getcf():
    dl = DataLoader()
    dl.merge_cf()

def getobj():
    dl = DataLoader()
    dl.get_objs()

if __name__ == '__main__':
    # postprocess()
    getcf()
    