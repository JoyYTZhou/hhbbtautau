import unittest, os, glob

from config.selectionconfig import runsetting as rs
from src.analysis.spawnjobs import JobLoader

pjoin = os.path.join

class TestLoader(unittest.TestCase):
    def setUp(self): 
        curr_dir = os.path.dirname(os.path.realpath(__file__))
        analysis_path = os.path.dirname(curr_dir)
        self.datapath = pjoin(analysis_path, "data/preprocessed")
        self.outpath = pjoin(curr_dir, "testout")
        self.jl = JobLoader(self.datapath, self.outpath)
    
    def test_prep_jobs(self):
        inputpath = pjoin(self.datapath, "ZZ.json.gz")
        returned = self.jl.prep_jobs(inputpath)

        
        self.assertTrue(os.path.exists(self.outpath), f"Directory {self.outpath} does not exist!")

    