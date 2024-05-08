from src.config.selectionconfig import runsetting as rs
from src.analysis.custom import switch_selections
from src.analysis.processor import Processor

def test_skim(selname=rs.SEL_NAME):
    """Run the processor for a single file.
    """
    eventSelection = switch_selections(selname)
    fn = "root://cmsxrootd.fnal.gov//store/mc/Run3Summer22EENanoAODv12/GluGlutoHHto2B2Tau_kl-1p00_kt-1p00_c2-0p00_TuneCP5_13p6TeV_powheg-pythia8/NANOAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v3/80000/5998ad11-ab82-41bf-a740-7a2c38b85a14.root"
    proc = Processor(rs, 'ggF', eventSelection)
    proc.runfile(fn, 0)