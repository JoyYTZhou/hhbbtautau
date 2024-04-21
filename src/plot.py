from utils.rootutil import DataLoader
from config.selectionconfig import cleansetting

def postprocess():
    dl = DataLoader(cleansetting)
    dl.hadd_cfs()
    dl.hadd_roots(cleancfg=cleansetting, wgt_dict=dl.wgt_dict)

def getcf():
    dl = DataLoader(cleansetting)
    dl.weight_rawcf(dirbase='hadded')
    dl.get_totraw(dirbase='hadded')
    dl.get_totwgt()

def getobj():
    dl = DataLoader(cleansetting)
    dl.get_objs()

if __name__ == '__main__':
    # postprocess()
    getobj
    