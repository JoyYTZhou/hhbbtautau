import unittest, os, glob

from src.analysis.processor import Processor
from src.utils.filesysutil import FileSysHelper, XRootDHelper
from config.projectconfg import runsetting as rs
from config.customEvtSel import switch_selections

pjoin = os.path.join

class TestProcessor(unittest.TestCase):
    def setUp(self):
        self.preprocessed = {
        "files": {
            "root://xrootd-local.unl.edu:1094//store/mc/Run3Summer22EENanoAODv12/DYJetsToLL_M-50_TuneCP5_13p6TeV-madgraphMLM-pythia8/NANOAODSIM/forPOG_130X_mcRun3_2022_realistic_postEE_v6-v2/2520000/e389fc53-6ebe-48cd-8eae-c1b7bbca7cbe.root": {
                "object_path": "Events",
                "steps": [
                    [
                        0,
                        9937
                    ],
                    [
                        9937,
                        19874
                    ],
                    [
                        19874,
                        29811
                    ],
                    [
                        29811,
                        39748
                    ],
                    [
                        39748,
                        49685
                    ],
                    [
                        49685,
                        59622
                    ],
                    [
                        59622,
                        69559
                    ],
                    [
                        69559,
                        79496
                    ],
                    [
                        79496,
                        89433
                    ],
                    [
                        89433,
                        99370
                    ],
                    [
                        99370,
                        109307
                    ],
                    [
                        109307,
                        119244
                    ],
                    [
                        119244,
                        129181
                    ],
                    [
                        129181,
                        139118
                    ],
                    [
                        139118,
                        149055
                    ],
                    [
                        149055,
                        158992
                    ],
                    [
                        158992,
                        168929
                    ],
                    [
                        168929,
                        178866
                    ],
                    [
                        178866,
                        188803
                    ],
                    [
                        188803,
                        198740
                    ],
                    [
                        198740,
                        208677
                    ],
                    [
                        208677,
                        218614
                    ],
                    [
                        218614,
                        228551
                    ],
                    [
                        228551,
                        238488
                    ],
                    [
                        238488,
                        248425
                    ],
                    [
                        248425,
                        258362
                    ],
                    [
                        258362,
                        268299
                    ],
                    [
                        268299,
                        278236
                    ],
                    [
                        278236,
                        288173
                    ],
                    [
                        288173,
                        298110
                    ],
                    [
                        298110,
                        308047
                    ],
                    [
                        308047,
                        317984
                    ],
                    [
                        317984,
                        327921
                    ],
                    [
                        327921,
                        337858
                    ],
                    [
                        337858,
                        347795
                    ],
                    [
                        347795,
                        357732
                    ],
                    [
                        357732,
                        367669
                    ],
                    [
                        367669,
                        377606
                    ],
                    [
                        377606,
                        387543
                    ],
                    [
                        387543,
                        397480
                    ],
                    [
                        397480,
                        407417
                    ],
                    [
                        407417,
                        417354
                    ],
                    [
                        417354,
                        427291
                    ],
                    [
                        427291,
                        437228
                    ],
                    [
                        437228,
                        447165
                    ],
                    [
                        447165,
                        457102
                    ],
                    [
                        457102,
                        467039
                    ],
                    [
                        467039,
                        476976
                    ],
                    [
                        476976,
                        486913
                    ],
                    [
                        486913,
                        496850
                    ],
                    [
                        496850,
                        506787
                    ],
                    [
                        506787,
                        516724
                    ],
                    [
                        516724,
                        526661
                    ],
                    [
                        526661,
                        536598
                    ],
                    [
                        536598,
                        546535
                    ],
                    [
                        546535,
                        556472
                    ],
                    [
                        556472,
                        566409
                    ],
                    [
                        566409,
                        576346
                    ],
                    [
                        576346,
                        586283
                    ],
                    [
                        586283,
                        596220
                    ],
                    [
                        596220,
                        606157
                    ],
                    [
                        606157,
                        616094
                    ],
                    [
                        616094,
                        626031
                    ],
                    [
                        626031,
                        635968
                    ],
                    [
                        635968,
                        645905
                    ],
                    [
                        645905,
                        655842
                    ],
                    [
                        655842,
                        665779
                    ],
                    [
                        665779,
                        675716
                    ],
                    [
                        675716,
                        685653
                    ],
                    [
                        685653,
                        695590
                    ],
                    [
                        695590,
                        705481
                    ]
                ],
                "num_entries": 705481,
                "uuid": "d2855dd2-62e3-11ee-bbb7-6f01a8c0beef"
        }},
        "metadata": {
            "shortname": "DYJets"
        }}

        self.eventSelection = switch_selections(rs.SEL_NAME)
        self.proc = Processor(rs, self.preprocessed, transferP=None, evtselclass=self.eventSelection)
    
    def tearDown(self) -> None:
        files = glob.glob(os.path.join(self.proc.outdir, "*"))
        for f in files: os.remove(f)
        xrdhelper = XRootDHelper()
        xrdhelper.remove_files(rs.TRANSFER_PATH)
    
    def test_dir_init(self):
        expected = self.proc.outdir
        self.assertTrue(os.path.exists(expected), f"Local output Directory {expected} does not exist!")
        
    def test_proc_load_remote(self):
        result = self.proc.loadfile_remote(self.preprocessed)
        print(result.attrs)
        self.assertIsNotNone(result)
        self.assertTrue(hasattr(result, 'fields'), "Events do not have fields attribute")
    
    def test_proc_run_file(self): 
        """Run the processor for selecting on a single file"""
        result = self.proc.runfiles(write_npz=False)
        expected = pjoin(self.proc.outdir, "*.root")
        matched = glob.glob(expected)
        self.assertTrue(len(matched) > 0, f"No root output files found in {expected}")

        expected = os.path.join(self.proc.outdir, "*.csv")
        matched = glob.glob(expected)
        self.assertTrue(len(matched) > 0, f"No cutflow csv files found in {expected}")

        self.assertEqual(result, 0, "Error encountered")
    
    def test_transfer_file(self):
        proc = Processor(rs, self.preprocessed, transferP=rs.TRANSFER_PATH, evtselclass=self.eventSelection) 
        result = proc.runfiles(write_npz=False)

        self.assertEqual(result, 0, "Error encountered")
        
        expected_files = ['ggF_3985bc58-ab6d-11ee-b5bf-0e803c0abeef_cutflow.csv', 
                          'ggF_3985bc58-ab6d-11ee-b5bf-0e803c0abeef-part0.root']
        produced = FileSysHelper.glob_files(proc.transfer)

        for file in expected_files:
            self.assertIn(file, produced, f"File {file} not found in {proc.transfer}")

        local_files = FileSysHelper.glob_files(proc.outdir)
        local_files = [f for f in local_files if not os.path.basename(f).startswith('.')]
        self.assertEqual(len(local_files), 0, f"Files not removed from {proc.outdir}")
    
if __name__ == '__main__':
    unittest.main()

