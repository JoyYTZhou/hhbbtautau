from config.projectconfg import cleansetting
from src.plotting.postprocessor import PostProcessor
import argparse

def __main__():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--mode', choices=['hadd', 'yield'], required=True, 
                        help='Choose the mode to run the postprocessor')
    parser.add_argument('--group', type=str, nargs='+', required=False, default=None, 
                        help='Group of the files to be hadded, e.g. DYJets TTbar etc.')
    
    args = parser.parse_args()
    pp = PostProcessor(cleansetting, groups=args.group)

    if args.mode == 'hadd':
        pp()
        
if __name__ == '__main__':
    __main__()