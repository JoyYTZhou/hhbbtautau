import unittest, os

class TestProcessor(unittest.TestCase):
    def setUp(self):
        os.environ['PROCESS_NAME'] = 'TTto2L2N'

        from config.selectionconfig import runsetting as rs
        from src.analysis.processor import Processor
        from src.analysis.custom import switch_selections

        selname = rs.SEL_NAME
        eventSelection = switch_selections(selname)
        self.proc = Processor(rs, 'TTto2L2N', transferP=None, evtselclass=eventSelection)
        
    def test_proc_load_remote(self):
        """Run the processor for skimming single file"""
        preprocessed = {
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
        result = self.proc.loadfile_remote(preprocessed)
        print(result)

        self.assertIsNotNone(result)
    
if __name__ == '__main__':
    unittest.main()

