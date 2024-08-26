import unittest, os, glob

from src.analysis.processor import Processor
from config.selectionconfig import runsetting as rs
from src.analysis.custom import switch_selections

class TestProcessor(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.preprocessed = {
        "files": {
                "root://cmsdcadisk.fnal.gov:1094//dcache/uscmsdisk/store/mc/Run3Summer22EENanoAODv12/GluGlutoHHto2B2Tau_kl-1p00_kt-1p00_c2-0p00_TuneCP5_13p6TeV_powheg-pythia8/NANOAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v3/2540000/1161da1c-05e7-4f35-a15c-ab3ce7a0aab4.root": {
            "object_path": "Events",
            "steps": [
            [
                0,
                5580
            ]
            ],
            "num_entries": 5580,
            "uuid": "3985bc58-ab6d-11ee-b5bf-0e803c0abeef"
        }},
        "metadata": {
            "xsection": 0.002489,
            "shortname": "GluGlutoHHto2B2Tau"
            },
        }

        eventSelection = switch_selections(rs.SEL_NAME)
        cls.proc = Processor(rs, cls.preprocessed, transferP=None, evtselclass=eventSelection)

    def test_dir_init(self):
        expected = self.proc.outdir
        self.assertTrue(os.path.exists(expected), f"Directory {expected} does not exist!")
        
    def test_proc_load_remote(self):
        result = self.proc.loadfile_remote(self.preprocessed)
        print(result.attrs)
        self.assertIsNotNone(result)
        self.assertTrue(hasattr(result, 'fields'), "Events do not have fields attribute")
    
    def test_proc_run_file(self): 
        """Run the processor for selecting on a single file"""
        result = self.proc.runfiles(write_npz=False)
        expected = os.path.join(self.proc.outdir, "*.root")
        matched = glob.glob(expected)
        self.assertTrue(len(matched) > 0, f"No root output files found in {expected}")

        expected = os.path.join(self.proc.outdir, "*.csv")
        matched = glob.glob(expected)
        self.assertTrue(len(matched) > 0, f"No cutflow csv files found in {expected}")

        self.assertEqual(result, 1, "Error encountered")
    
class TestLoader(unittest.TestCase):
    def setUp(cls): 
        cls.datapath = "data/preprocessed/ZZ.json.gz"
        pass

    
if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(TestProcessor('test_dir_init'))
    suite.addTest(TestProcessor('test_proc_load_remote'))
    suite.addTest(TestProcessor('test_proc_run_file'))

    # Run the TestSuite
    runner = unittest.TextTestRunner()
    runner.run(suite)

