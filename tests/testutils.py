from src.analysis.spawnjobs import filterExisting, checkpath
from src.utils.filesysutil import glob_files, cross_check
from config.selectionconfig import runsetting as rs

import unittest, os, glob

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
    
    def testGlob(self):
        files = glob_files(self.temp_dir)
        self.assertTrue(len(files) > 0, "No files found in the directory! Double check glob_files function")
    
    def testCrossCheck(self):
        if_exist = cross_check("*.root", glob_files(self.temp_dir))
        self.assertTrue(if_exist, "Search for files containing wildcards failed. Double check cross_check function")
        if_exist = cross_check("test_3985bc58-ab6d-11ee-b5bf-0e803c0abeef_cutflow.csv", glob_files(self.temp_dir))
        self.assertTrue(if_exist, "File search failed. Double check cross_check function")
    
    def testFilter(self):
        need_process = filterExisting(self.ds, self.dsdata, tsferP=self.temp_dir)
        self.assertTrue(need_process, "Files not filtered correctly. Check filterExisting function")
        self.assertEqual(len(self.dsdata['files']), 1, "Files not filtered correctly, check filterExisting function")

    def tearDown(self) -> None:
        for f in glob.glob(pjoin(self.temp_dir, '*')): os.remove(f)
    
if __name__ == '__main__':
    unittest.main()