import uproot, time

if __name__ == '__main__':
    tree = uproot.open("root://xrootd-local.unl.edu:1094//store/mc/Run3Summer22EENanoAODv12/ZH_Hto2B_Zto2L_M-125_TuneCP5_13p6TeV_powheg-pythia8/NANOAODSIM/130X_mcRun3_2022_realistic_postEE_v6-v2/2540000/db994e43-3b90-4cfb-91d3-054df6f8eb03.root:Events")
    start = time.time()
    events = tree.arrays()
    end = time.time()
    elapsed_time = end - start
    print(f"Elapsed time: {elapsed_time} seconds")