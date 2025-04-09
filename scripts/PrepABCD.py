import sys, os, logging
import pandas as pd

from hep_rewgt_tk.reweight_app import SingleXGBReweighter
from hep_rewgt_tk.reweight_nn import SingleMLPRwgter
from hep_rewgt_tk.reweight_adversarial import AdversarialReweighter

from src.plotting.visutil import CSVPlotter
from src.plotting.summary import PostPreselProcessor
from src.utils.mathutil import MathUtil, ABCDUtil
from src.utils.ioutil import setup_logging

luminosity = {"2022PreEE": 41.5/2 * 1000, "2022PostEE": 41.5 * 1000/2, "2023Summer": 32.7 * 1000}

pjoin = os.path.join

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_dir = os.path.join(root_dir, 'data')

post_process_base = '/Users/yuntongzhou/Desktop/Dihiggszztt/output/postprocessed'
local_cutflow_base = '/Users/yuntongzhou/Desktop/Dihiggszztt/output/ABCD_Cutflow'
results_dir = "/Users/yuntongzhou/Desktop/Dihiggszztt/output"

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
    
    dfC.to_csv("/Users/yuntongzhou/Desktop/Dihiggszztt/output/ABCD/SS2b.csv")
    dfD.to_csv("/Users/yuntongzhou/Desktop/Dihiggszztt/output/ABCD/SS1b.csv")
    dfA.to_csv("/Users/yuntongzhou/Desktop/Dihiggszztt/output/ABCD/OS2b.csv")
    dfB.to_csv("/Users/yuntongzhou/Desktop/Dihiggszztt/output/ABCD/OS1b.csv")
    return dfA, dfB, dfC, dfD

def split_sign(df_ori):
    df = df_ori.copy()
    condition = df['OS'] == False
    ss = df[condition]
    os = df[~condition]
    return ss, os

# further split the data into training and testing based on mass
def split_mass(df_ori):
    df = df_ori.copy()[(df_ori['DiJet_mass'] > 90)]
    condition = df['DiJet_mass'] > 160
    train = df[condition]
    target = df[~condition]
    return train, target

def get_train_test(df_SS, df_OS, split_func):
    """Create train and test sets for the given dataframes.
    
    Return 
    train_SS, val_SS, train_OS, val_OS"""
    train_SS, val_SS = ABCDUtil.split_dataframe(df_SS, split_func)
    train_OS, val_OS = ABCDUtil.split_dataframe(df_OS, split_func)
    return train_SS, val_SS, train_OS, val_OS

# Select only certain datasets for training
def prep_training(df, savename, groups=['TTbar', 'DYJets']):
    mask = df.group.str.contains('|'.join(groups))
    filtered = df[mask]
    filtered.to_csv(pjoin("/Users/yuntongzhou/Desktop/Dihiggszztt/output/ABCD", savename))

def split_tauid(df_ori):
    df = df_ori.copy()
    condition = (df_ori['SDTau_idvsjet'] >= 5)
    control = df[~condition]
    signal = df[condition]
    return control, signal

def get_tauid_train_test(df_SS, df_OS):
    tau_id = lambda df: df['SDTau_idvsjet'] >= 5
    return get_train_test(df_SS, df_OS, tau_id)

def get_mbb_train_test(df_SS, df_OS):
    filter_mass = lambda df: df.copy()[df['DiJet_mass'] > 90]
    df_SS = filter_mass(df_SS)
    df_OS = filter_mass(df_OS)
    return get_train_test(df_SS, df_OS, lambda df: df['DiJet_mass'] < 160)

def apply_all(dfs, func):
    filtered = [None] * len(dfs)
    for i, df in enumerate(dfs):
        filtered[i] = func(df.copy())
    return filtered

def keep_fake_ttbar(df):
    df = df.copy()[df.group=='TTbar']
    cond_1 = df['LDTau_genflav'] < 5
    cond_2 = df['SDTau_genflav'] < 5 
    return df[((cond_1) | (cond_2))]

def keep_real_ttbar(df):
    df = df[df.group=='TTbar']
    cond_1 = df['LDTau_genflav'] >= 5
    cond_2 = df['SDTau_genflav'] >= 5 
    return df[((cond_1) | (cond_2))]

def signal_id_level(df):
    mask_1 = df['LDTau_idvsjet'] >= 1 # VVV loose WP
    mask_2 = df['SDTau_idvsjet'] >= 1 # VVV loose WP
    mask_3 = df['LDTau_idvse'] >= 3 # VLoose
    mask_4 = df['SDTau_idvse'] >= 3 # VLoose
    mask_5 = df['LDTau_idvsmu'] >= 4 # Tight
    mask_6 = df['SDTau_idvsmu'] >= 4 # Tight
    return df[(mask_1) & (mask_2) & (mask_3) & (mask_4) & (mask_5) & (mask_6)]

prep_sign_rwgt = lambda df: keep_fake_ttbar(signal_id_level(df))

def ABCDTable(inputpath1, inputpath2):
    for year in os.listdir(inputpath1):
        AC_df = pd.read_csv(f'{inputpath2}/year/scaledyield.csv')
        AC_df.drop(columns=['Sig Eff', 'Bkg Eff'], inplace=True)
        AC_df = AC_df.apply(pd.to_numeric, errors='coerce')
        BD_df = pd.read_csv(f'{inputpath1}/year/scaledyield.csv')
        BD_df = BD_df.apply(pd.to_numeric, errors='coerce')
        BD_df.drop(columns=['Sig Eff', 'Bkg Eff'], inplace=True)
        A_row = AC_df.iloc[-1]
        C_row = AC_df.iloc[-2]-AC_df.iloc[-1]
        B_row = BD_df.iloc[-1]
        D_row = BD_df.iloc[-2]-BD_df.iloc[-1]
        ABCD_tab = pd.DataFrame([A_row, B_row, C_row, D_row])
        ABCD_tab['SvB Ratio'] = ABCD_tab['Tot Sig']/ABCD_tab['Tot Bkg']
        ABCD_tab.index = ['Region A', 'Region B', 'Region C', 'Region D']
        ABCD_tab = ABCD_tab.dropna(axis=1, how='any')
        ABCD_tab.to_csv(f'/Users/yuntongzhou/Desktop/Dihiggszztt/output/ABCD_{year}.csv')

def train_mlp_rwgt(train_ori, train_tar):
    """Train a MLP reweighter from one region to another."""
    drop_kwds = ['Gen', 'weight_values', 'Weight_values', 'OS', 'group', 'gen', 'dataset', 'label', 'id', 'year', 'Tau_charge', 'X_num']
    mlp_rwgter = SingleMLPRwgter(train_ori, train_tar, 'weight', f"{results_dir}/training/TopQuark")
    mlp_rwgter.prep_data(drop_kwds)
    shallow_args= {
        'num_epochs': 100,
        'hidden_arch': 'high_dim',
        'batch_size': 256,
        'lr': 0.001,
        'save': True,
        'savename': 'basic_model.pth',
        'save_interval': 30}
    mlp_rwgter.train(**shallow_args)

    return mlp_rwgter

def read_two_channels_df():
    # define data and results directory, load data
    ABCD_dir = "/Users/yuntongzhou/Desktop/Dihiggszztt/output/ABCD"
    plot_dir = "/Users/yuntongzhou/Desktop/Dihiggszztt/output/plots/ABCD_ttOnly"

    Res1b_SS = pd.read_csv(f'{ABCD_dir}/RegionD.csv', index_col=0)
    Res2b_SS = pd.read_csv(f'{ABCD_dir}/RegionC.csv', index_col=0)
    Res1b_OS = pd.read_csv(f'{ABCD_dir}/RegionB.csv', index_col=0)
    Res2b_OS = pd.read_csv(f'{ABCD_dir}/RegionA.csv', index_col=0)

    return Res1b_SS, Res2b_SS, Res1b_OS, Res2b_OS

if __name__ == "__main__":
    setup_logging()
    oneb_path = '/Users/yuntongzhou/Desktop/Dihiggszztt/output/tightskim_onelooseb_hadded'
    twob_path = '/Users/yuntongzhou/Desktop/Dihiggszztt/output/tightskim_twolooseb_hadded'
    OS_2b, OS_1b, SS_2b, SS_1b = getABCDdf(oneb_path, twob_path)

    pp = PostPreselProcessor(cfg_1b, luminosity)
    logging.info("Processing 1b, OS")
    pp.get_yield('OS')
    logging.info("Processing 1b, SS")
    pp.get_yield('SS')

    pp = PostPreselProcessor(cfg_2b, luminosity)
    logging.info("Processing 2b, OS")
    pp.get_yield('OS')
    logging.info("Processing 2b, SS")
    pp.get_yield('SS')

    

    # ABCDTable()

    # prep_training(dfA, 'Res2b_OS.csv')
    # prep_training(dfB, 'Res1b_OS.csv')
    # prep_training(dfC, 'Res2b_SS.csv')
    # prep_training(dfD, 'Res1b_SS.csv')