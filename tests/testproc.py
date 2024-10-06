import unittest, os, glob, json

from src.analysis.processor import Processor
from src.utils.filesysutil import FileSysHelper, XRootDHelper
from config.projectconfg import runsetting as rs
from config.customEvtSel import switch_selections

pjoin = os.path.join

class TestProcessor(unittest.TestCase):
    def setUp(self):
        with open(rs.TEST_JSON, 'r') as f:
            self.preprocessed = json.load(f)

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

        prefix = self.preprocessed['metadata']['shortname']
        uuid = self.preprocessed['files'].values()[0]['uuid']

        expected_files = [f'{prefix}_{uuid}_cutflow.csv', f'{prefix}_{uuid}-part0.root']
        
        produced = FileSysHelper.glob_files(proc.transfer)

        for file in expected_files:
            self.assertIn(file, produced, f"File {file} not found in {proc.transfer}")

        local_files = FileSysHelper.glob_files(proc.outdir)
        local_files = [f for f in local_files if not os.path.basename(f).startswith('.')]
        self.assertEqual(len(local_files), 0, f"Files not removed from {proc.outdir}")
    
if __name__ == '__main__':
    unittest.main()

