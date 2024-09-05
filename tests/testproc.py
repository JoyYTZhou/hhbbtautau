import unittest, os, glob

from src.analysis.processor import Processor
from src.analysis.spawnjobs import filterExisting, checkpath
from src.utils.filesysutil import glob_files
from config.selectionconfig import runsetting as rs
from src.analysis.custom import switch_selections

pjoin = os.path.join

class TestFilter(unittest.TestCase):
    def setUp(self):
        self.temp_dir = pjoin(rs.OUTPUTDIR_PATH, 'temp')
        checkpath(rs.OUTPUTDIR_PATH, createdir=True)
        checkpath(self.temp_dir, createdir=True)
        with open(pjoin(self.temp_dir, 'test_3985bc58-ab6d-11ee-b5bf-0e803c0abeef_cutflow.csv'), 'w') as f: 
            f.write("0")
        with open(pjoin(self.temp_dir, 'test_3985bc58-ab6d-11ee-b5bf-0e803c0abeef.root'), 'w') as f: 
            f.write("0")
        self.ds = "test"
        self.dsdata = {"files": {
        "root://cmsxrootd.hep.wisc.edu:1094//store/mc/Run3Summer22EENanoAODv12/WWW_4F_TuneCP5_13p6TeV_amcatnlo-madspin-pythia8/NANOAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v2/2520000/d78224ba-afff-44f0-b559-242cce95af4f.root": {
            "object_path": "Events",
            "steps": [
            [
                0,
                9766
            ],
            [
                9766,
                19532
            ]
            ],
            "num_entries": 19532,
            "uuid": "3985bc58-ab6d-11ee-b5bf-0e803c0abeef"
        },
        "root://cmsxrootd.hep.wisc.edu:1094//store/mc/Run3Summer22EENanoAODv12/WWW_4F_TuneCP5_13p6TeV_amcatnlo-madspin-pythia8/NANOAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v2/2520000/f0eecfe6-d5f7-4163-88a0-3e5a059c3a8c.root": {
            "object_path": "Events",
            "steps": [
            [
                0,
                6682
            ]
            ],
            "num_entries": 6682,
            "uuid": "4e4b117c-661e-11ee-9492-9484e4a9beef"
        }}}
        
    def testFilter(self):
        need_process = filterExisting(self.ds, self.dsdata, tsferP=self.temp_dir)
        self.assertTrue(need_process, "")
        self.assertEqual(len(self.dsdata['files']), 1, "One file is left to be processed")

    def tearDown(self) -> None:
        for f in glob.glob(pjoin(self.temp_dir, '*')): os.remove(f)

class TestProcessor(unittest.TestCase):
    def setUp(self):
        self.preprocessed = {
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
            "shortname": "ggF"
        }}

        self.eventSelection = switch_selections(rs.SEL_NAME)
        self.proc = Processor(rs, self.preprocessed, transferP=None, evtselclass=self.eventSelection)
    
    def tearDown(self) -> None:
        files = glob.glob(os.path.join(self.proc.outdir, "*"))
        for f in files: os.remove(f)
    
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
        expected = pjoin(self.proc.outdir, "*.root")
        matched = glob.glob(expected)
        self.assertTrue(len(matched) > 0, f"No root output files found in {expected}")

        expected = os.path.join(self.proc.outdir, "*.csv")
        matched = glob.glob(expected)
        self.assertTrue(len(matched) > 0, f"No cutflow csv files found in {expected}")

        self.assertEqual(result, 0, "Error encountered")
    
    def test_transfer_file(self):
        proc = Processor(rs, self.preprocessed, transferP='/store/user/joyzhou/tests/ggF', evtselclass=self.eventSelection) 
        result = proc.runfiles(write_npz=False)

        self.assertEqual(result, 0, "Error encountered")
        
        expected_files = ['GluGlutoHHto2B2Tau_3985bc58-ab6d-11ee-b5bf-0e803c0abeef_cutflow.csv', 
                          'GluGlutoHHto2B2Tau_3985bc58-ab6d-11ee-b5bf-0e803c0abeef-part0.root']
        produced = glob_files(proc.transfer)
        for file in expected_files:
            self.assertIn(file, produced, f"File {file} not found in {proc.transfer}")
        
        local_files = glob_files(proc.outdir)
        self.assertEqual(len(local_files), 0, f"Files not removed from {proc.outdir}")
            
    
if __name__ == '__main__':
    unittest.main()

