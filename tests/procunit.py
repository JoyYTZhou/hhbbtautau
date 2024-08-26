import unittest, os, glob
from src.analysis.processor import Processor
from config.selectionconfig import runsetting as rs
from src.analysis.custom import switch_selections

class TestProcessor(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.preprocessed = {
            "files": {
                "root://cmsdcadisk.fnal.gov:1094//dcache/uscmsdisk/store/mc/Run3Summer22EENanoAODv12/TTto2L2Nu_TuneCP5_13p6TeV_powheg-pythia8/NANOAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v2/2520000/07a6b4e8-a99d-4cd4-8ab0-9a51635f6a6f.root": {
                    "object_path": "Events",
                    "steps": [
                        [
                            0,
                            10038
                        ],
                        [
                            10038,
                            20076
                        ],
                        [
                            20076,
                            30114
                        ],
                        [
                            30114,
                            40152
                        ],
                        [
                            40152,
                            50190
                        ],
                        [
                            50190,
                            60228
                        ],
                        [
                            60228,
                            70266
                        ],
                        [
                            70266,
                            80304
                        ],
                        [
                            80304,
                            90342
                        ],
                        [
                            90342,
                            100380
                        ],
                        [
                            100380,
                            110418
                        ],
                        [
                            110418,
                            120456
                        ],
                        [
                            120456,
                            130494
                        ],
                        [
                            130494,
                            140532
                        ],
                        [
                            140532,
                            150570
                        ],
                        [
                            150570,
                            160608
                        ],
                        [
                            160608,
                            170646
                        ],
                        [
                            170646,
                            180684
                        ],
                        [
                            180684,
                            190721
                        ]
                    ],
                    "num_entries": 190721,
                    "uuid": "7155c6ee-6353-11ee-baac-6501a8c0beef"}},
            "metadata": {
                "xsection": 96.978,
                "shortname": "TTto2L2N"
            }
        }
        eventSelection = switch_selections(rs.SEL_NAME)
        cls.proc = Processor(rs, 'TTto2L2N', transferP=None, evtselclass=eventSelection)

    def test_dir_init(self):
        expected = self.proc.outdir
        self.assertTrue(os.path.exists(expected), f"Directory {expected} does not exist!")
        
    def test_proc_load_remote(self):
        result = self.proc.loadfile_remote(self.preprocessed)
        self.assertIsNotNone(result)
        self.assertTrue(hasattr(result, 'fields'), "Events do not have fields attribute")
    
    def test_proc_run_file(self): 
        """Run the processor for selecting on a single file"""
        result = self.proc.runfile(self.preprocessed, write_npz=False)
        expected = os.path.join(self.proc.outdir, "*.root")
        matched = glob.glob(expected)
        self.assertTrue(len(matched) > 0, f"No root output files found in {expected}")

        expected = os.path.join(self.proc.outdir, "*.csv ")
        matched = glob.glob(expected)
        self.assertTrue(len(matched) > 0, f"No cutflow csv files found in {expected}")

        self.assertEqual(result, 1, "Error encountered for file index in TTto2L2N")
    
class TestRunner(unittest.TestCase):
    pass
    
if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTest(TestProcessor('test_dir_init'))
    suite.addTest(TestProcessor('test_proc_load_remote'))
    suite.addTest(TestProcessor('test_proc_run_file'))

    # Run the TestSuite
    runner = unittest.TextTestRunner()
    runner.run(suite)

