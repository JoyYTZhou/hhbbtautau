from config.projectconfg import cleansetting
from src.plotting.postprocess import PostProcessor
import argparse

def __main__():
    parser = argparse.ArgumentParser()
    

    pp = PostProcessor(cleansetting)
    pp.run()