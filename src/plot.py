from utils.rootutil import DataLoader
from config.selectionconfig import cleansetting

def hadd():
    dl = DataLoader(cleansetting)
    # dl.hadd_roots(cleancfg=cleansetting, wgt_dict=dl.wgt_dict)
    # dl.hadd_cfs()
    dl.weight_rawcf(dirbase='hadded')
    # dl.get_totcf(appendname='jetskim')
    # dl.get_objs()

if __name__ == '__main__':
    hadd()