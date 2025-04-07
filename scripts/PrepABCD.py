import sys, os, logging
import pandas as pd

from src.plotting.visutil import CSVPlotter
from src.plotting.summary import PostProcessor, PostPreselProcessor
from src.utils.mathutil import MathUtil

luminosity = {"2022PreEE": 41.5/2 * 1000, "2022PostEE": 41.5 * 1000/2, "2023Summer": 32.7 * 1000}

pjoin = os.path.join

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(root_dir, 'data')

post_process_base = '/Users/yuntongzhou/Desktop/Dihiggszztt/output/postprocessed'
local_cutflow_base = '/Users/yuntongzhou/Desktop/Dihiggszztt/output/ABCD_Cutflow'

logging.info(f"Root directory: {root_dir}")

cfg_1b = {"DIRNAME": None, "DATA_DIR": data_dir, 
        "INPUTDIR": pjoin(post_process_base, 'oneb'),  
        "LOCALOUTPUT": pjoin(local_cutflow_base, 'oneb'),
        "TRANSFERPATH": None, 
        "IS_MC": True}

cfg_2b = cfg_1b.copy()
cfg_2b["INPUTDIR"] = pjoin(post_process_base, 'twob')
cfg_2b["LOCALOUTPUT"] = pjoin(local_cutflow_base, 'twob')

def add_inv_mass_dR(df):
    """Prepare the invariant mass and dR for the given dataframe."""
    MathUtil.add_system_4vec(df, 'LDTau', 'SDTau', 'DiTau')
    MathUtil.add_system_4vec(df, 'LDBjet', 'SDBjet', 'DiJet')
    MathUtil.add_dR(df, 'LDTau', 'SDTau', 'Tau_dR')
    MathUtil.add_dR(df, 'LDBjet', 'SDBjet', 'Bjet_dR')
    MathUtil.add_dR(df, 'DiTau', 'DiJet', 'RecoH_dR')
    MathUtil.add_HT(df, ['LDTau', 'SDTau', 'LDBjet', 'SDBjet'], 'HT')
    MathUtil.add_f_momentum(df, 'LDTau')
    MathUtil.add_f_momentum(df, 'SDTau')
    MathUtil.add_f_momentum(df, 'LDBjet')
    MathUtil.add_f_momentum(df, 'SDBjet')
    MathUtil.add_f_momentum(df, 'DiTau')
    MathUtil.add_f_momentum(df, 'DiJet')
    df['OS'] = df['LDTau_charge']*df['SDTau_charge'] < 0

def selOS(df):
    add_inv_mass_dR(df)
    OS_df = df[df['OS']]
    return OS_df

def selSS(df):
    add_inv_mass_dR(df)
    SS_df = df[~df['OS']]
    return SS_df

def regroup(df, keywords, new_value):
    mask = df['dataset'].apply(lambda x: any(keyword in x for keyword in keywords))
    df.loc[mask, 'group'] = new_value
    return df

def getABCDdf(oneb_data, twob_data):
    cp = CSVPlotter(outdir='/Users/yuntongzhou/Desktop/Dihiggszztt/output/plots')
    wgt_name = 'Generator_weight_values'
    meta_dir = '/Users/yuntongzhou/Desktop/Dihiggszztt/HHtobbtautau/data/weightedMC'
    output_dir = '/Users/yuntongzhou/Desktop/Dihiggszztt/output/postprocessed'
    dfDs = []
    dfBs = []
    pjoin = os.path.join
    for year in os.listdir(oneb_data):
        if not year.startswith('.'):
            lumi = luminosity[year]
            meta_path = pjoin(meta_dir, f'{year}.json')
            df_src = pjoin(oneb_data, year)
            postp = pjoin(output_dir, 'oneb', year)
            args = {'metadata_path': meta_path, 'postp_output': postp, 'per_evt_wgt': wgt_name, 'luminosity': lumi}
            dfB_year = cp.process_datasets(datasource=df_src, **args, extraprocess=selOS, selname='OS Tau')
            dfBs.append(dfB_year)
            dfD_year = cp.process_datasets(datasource=df_src, **args, extraprocess=selSS, selname='SS Tau')
            dfDs.append(dfD_year)
    dfD = pd.concat(dfDs)
    dfB = pd.concat(dfBs)
    
    dfCs = []
    dfAs = []
    for year in os.listdir(twob_data):
        if not year.startswith('.'):
            lumi = luminosity[year]
            meta_path = pjoin(meta_dir, f'{year}.json')
            df_src = pjoin(twob_data, year)
            postp = pjoin(output_dir, 'twob', year)
            args = {'metadata_path': meta_path, 'postp_output': postp, 'per_evt_wgt': wgt_name, 'luminosity': lumi}
            dfA_year = cp.process_datasets(datasource=df_src, **args, extraprocess=selOS, selname='OS Tau')
            dfAs.append(dfA_year)
            dfC_year = cp.process_datasets(datasource=df_src, **args, extraprocess=selSS, selname='SS Tau')
            dfCs.append(dfC_year)
    dfC = pd.concat(dfCs)
    dfA = pd.concat(dfAs)
    
    dfC.to_csv("/Users/yuntongzhou/Desktop/Dihiggszztt/output/ABCD/RegionC.csv")
    dfD.to_csv("/Users/yuntongzhou/Desktop/Dihiggszztt/output/ABCD/RegionD.csv")
    dfA.to_csv("/Users/yuntongzhou/Desktop/Dihiggszztt/output/ABCD/RegionA.csv")
    dfB.to_csv("/Users/yuntongzhou/Desktop/Dihiggszztt/output/ABCD/RegionB.csv")
    return dfA, dfB, dfC, dfD

# Select only certain datasets for training
def prep_training(df, savename, groups=['TTbar', 'DYJets']):
    mask = df.group.str.contains('|'.join(groups))
    filtered = df[mask]
    filtered.to_csv(pjoin("/Users/yuntongzhou/Desktop/Dihiggszztt/output/ABCD", savename))


if __name__ == "__main__":
    # oneb_path = '/Users/yuntongzhou/Desktop/Dihiggszztt/output/tightskim_onelooseb_hadded'
    # twob_path = '/Users/yuntongzhou/Desktop/Dihiggszztt/output/tightskim_twolooseb_hadded'
    # dfA, dfB, dfC, dfD = getABCDdf(oneb_path, twob_path)

    pp = PostPreselProcessor(cfg_1b, luminosity)
    pp.get_yield('OS')
    pp.get_yield('SS')

    pp = PostPreselProcessor(cfg_2b, luminosity)
    pp.get_yield('OS')
    pp.get_yield('SS')

    # prep_training(dfA, 'Res2b_OS.csv')
    # prep_training(dfB, 'Res1b_OS.csv')
    # prep_training(dfC, 'Res2b_SS.csv')
    # prep_training(dfD, 'Res1b_SS.csv')