from utils.rootutil import DataLoader
from config.selectionconfig import cleansetting

def hadd():
    dl = DataLoader(cleansetting)
    dl()

if __name__ == '__main__':
    hadd()