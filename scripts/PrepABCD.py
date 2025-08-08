import os, logging
import pandas as pd
import numpy as np
import re
from hep_rewgt_tk.reweight_nn import SingleMLPRwgter
from src.plotting.visutil import CSVPlotter
from src.utils.mathutil import MathUtil, ABCDUtil
from src.utils.filesysutil import FileSysHelper
from src.utils.ioutil import setup_logging
from src.utils.datautil import CutflowProcessor
from src.utils.displayutil import RichArgumentParser, print_dataframe_rich
pjoin = os.path.join

PARENT_DIR = os.path.dirname(os.path.realpath(__file__))
SRC_DIR = os.path.dirname(PARENT_DIR)
META_DIR = os.path.join(SRC_DIR, 'data')

luminosity = {"2022PreEE": (5.0104+2.9700) * 1000, "2022PostEE": (5.8070+17.7819+3.0828) * 1000, "2023Summer": 32.7 * 1000, "2022": 1}
regroup_dict = {"Others": ['WJets', 'WZ', 'WW', 'WWW', 'ZZZ', 'WZZ', 'WWZ'], 'HH': ['ggF']}

drop_kwds = ['Gen', 'weight_values', 'Weight_values', 'OS', 'group', 'gen', 'dataset', 'label', 'id', 'year', 'Tau_charge', 'X_num'] 

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from config.plotsetting import dR, H_pt, HT, infer_H_mass, train_H_mass, H_mass, tau_pt


def get_ABCD_results(dfA, dfB, dfC, dfD, channel_name=''):
    out_dir = f'/Users/yuntongzhou/Desktop/Dihiggszztt/output/plots/{channel_name}'
    ABCD_Helper = ABCDUtil(dfA, dfB, dfC, dfD, 'weight')
    print(f"Calculating ABCD results for {channel_name}...")
    ABCD_Helper.get_all_stats()

    rwgt_df = pd.read_csv(f'/Users/yuntongzhou/Desktop/Dihiggszztt/output/training/TopQuark/{channel_name}.csv')
    
    A_pred = ABCD_Helper.shape_reweight()
    filter_func = lambda df: df.copy()[df['DiJet_mass'] < 160]
    base_args = {
        'list_of_evts': [filter_func(dfA), A_pred, filter_func(rwgt_df)],
        'labels': ['Signal Region', 'Shape Average', 'MLP Reweighted'],
        'ratio_ylabel': 'Pred/Actual',
        'outdir': out_dir,
        'save_suffix': 'rwgt_comp',
        'title': channel_name
    }
    attr_dicts = [infer_H_mass, dR, H_pt, HT]
    if not os.path.exists(out_dir): os.makedirs(out_dir)
    for attr_dict in attr_dicts:
        plot_config = base_args.copy()
        plot_config['attridict'] = attr_dict
        CSVPlotter.plot_shape(**plot_config)

def plot_rwgt_results(src_df, tar_df, rwgt_df, out_dir):
    # Base configuration that's common for all plots
    base_config = {
        'list_of_evts': [tar_df, src_df, rwgt_df],
        'labels': ['OS', 'original SS', 'Reweighted SS'],
        'ratio_ylabel': 'Pred/Actual',
        'outdir': out_dir,
        'save_suffix': 'rwgt'
    }
    
    # List of attribute dictionaries to plot
    attr_dicts = [dR, H_pt, HT, H_mass]
    
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # Plot each attribute dictionary
    for attr_dict in attr_dicts:
        plot_config = base_config.copy()
        plot_config['attridict'] = attr_dict
        CSVPlotter.plot_shape(**plot_config)

def get_top_bjets(df):
    """Compute and extract the top two b-jets based on their HHbtag scores."""
    pattern = re.compile(r'Bjet(\d+)_.+')
    matching_columns = [col for col in df.columns if pattern.match(col)]
    bjet_columns_df = df[matching_columns]
    
    missing_columns = [f'Bjet{i}_HHbtag' for i in range(1, 11) if f'Bjet{i}_HHbtag' not in bjet_columns_df.columns]
    if missing_columns:
        raise ValueError(f"Missing expected columns: {', '.join(missing_columns)}")
    
    hhbtags = bjet_columns_df[[f'Bjet{i}_HHbtag' for i in range(1, 11)]].to_numpy()
    hhbtags = np.nan_to_num(hhbtags, nan=-np.inf)  # Replace NaN with -inf to treat them as the lowest scores
    
    top_two_indices = np.argsort(hhbtags, axis=1)[:, -2:]  # Indices of the two largest values
    top_two_indices = np.sort(top_two_indices, axis=1)[:, ::-1]  # Sort in descending order of scores
    
    top_two_jet_indices = top_two_indices + 1  # Increment by 1 to match 'Bjet{i}' indexing
    
    top_bjets_data = {
        f'TopBjet{j+1}_{col.split("_", 1)[1]}': bjet_columns_df.iloc[:, top_two_jet_indices[:, j] - 1].to_numpy()
        for j in range(2) for col in matching_columns
    }
    
    top_bjets_df = pd.DataFrame(top_bjets_data, index=df.index)
    return top_bjets_df

def add_extra_features(df):
    """Add extra features to the dataframe for further analysis."""
    pattern = re.compile(r'Bjet(\d+)_.+')
    bjet_columns = [col for col in df.columns if pattern.match(col)]
    other_columns = [col for col in df.columns if col not in bjet_columns]
    
    bjet_df = df[bjet_columns].copy()
    other_df = df[other_columns].copy()
    
    bjet_df = get_top_bjets(bjet_df)
    df = pd.concat([other_df, bjet_df], axis=1)
    MathUtil.add_system_4vec(df, 'Bjet1', 'Bjet2', 'DiJet')
    MathUtil.add_system_4vec(df, 'DiTau', 'DiJet', 'DiHiggs')
    df['OS'] = ((df['LDTau_charge'] * df['SDTau_charge']) < 0)
    df['HT'] = df['Bjet1_pt'] + df['Bjet2_pt'] + df['LDTau_pt'] + df['SDTau_pt'] + df['MET_pt'] # Total transverse energy
    return df

def neg_wgt(df) -> pd.DataFrame:
    """Return a copy of the dataframe with negative weights for non-data groups."""
    df_copy = df.copy()
    mask = df_copy['group'] != 'Data'
    df_copy.loc[mask, 'weight'] = -df_copy.loc[mask, 'weight']
    return df_copy

def regroup(df, keywords, new_value):
    mask = df['dataset'].apply(lambda x: any(keyword in x for keyword in keywords))
    df.loc[mask, 'group'] = new_value
    return df


class ARUtil:
    @staticmethod
    def get_ss(data_path='oneb_dfs', channel_name='oneb'):
        data_path = pjoin(raw_base_dir, data_path)
        output_path = pjoin(df_base_dir, channel_name)
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        _, ss_df = OSSSUtil.get_os_ss(data_path, channel_name)
        ss_df.to_csv(pjoin(df_base_dir, channel_name, 'SS.csv'), index=False)
        ss_cfdf = CutflowProcessor.cutflow_by_sum(ss_df, f'{channel_name}_ss', None)
        ss_cfdf= CutflowProcessor.categorize_processes(ss_cfdf, regroup_dict)
        ss_cfdf.to_csv(pjoin(df_base_dir, channel_name, f'SS_cutflow.csv'))
        logging.info(f"Processed SS dataframe saved to {pjoin(df_base_dir, channel_name, 'SS.csv')}")

        return ss_df
    
    @staticmethod
    def get_os_CR(data_path='oneb_dfs', channel_name='oneb', 
                  filter_func=lambda df: df.copy()[df['DiJet_mass'] >= 200]):
        """Get the OS control region dataframe."""
        data_path = pjoin(raw_base_dir, data_path)
        output_path = pjoin(df_base_dir, channel_name)
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        os_df, _ = OSSSUtil.get_os_ss(data_path, channel_name)
        os_df = filter_func(os_df)
        os_df.to_csv(pjoin(df_base_dir, channel_name, 'OS_CR.csv'), index=False)
        
        return os_df

class OSSSUtil:
    @staticmethod
    def plot_key_comparisons(channel_name, df_filter_func=neg_wgt, title='0b OS/SS'):
        data_dir = pjoin(df_base_dir, channel_name)
        os_df = pd.read_csv(pjoin(data_dir, 'OS.csv'))
        ss_df = pd.read_csv(pjoin(data_dir, 'SS.csv'))
        if df_filter_func:
            os_df = df_filter_func(os_df)
            ss_df = df_filter_func(ss_df)
       
        plt_out_dir = pjoin(plt_base_dir, channel_name) 
        if not os.path.exists(plt_out_dir):
            os.makedirs(plt_out_dir)
        
        base_args = {'ratio_ylabel': 'OS/SS',
            'outdir': plt_out_dir,
            'save_suffix': '_ss_os', 'title': title}
        
        for attr_dict in [dR, H_pt, HT, H_mass, tau_pt]:
            plot_config = base_args.copy()
            plot_config['list_of_evts'] = [ss_df, os_df]
            plot_config['labels'] = ['SS', 'OS']
            plot_config['attridict'] = attr_dict
            CSVPlotter.plot_shape(**plot_config)
        
class FakeUtil:
    @staticmethod
    def keep_real_taus(df, groups=['TTbar', 'DYJets']):
        """Return a dataframe with real tau events in specified groups and all other events unfiltered.
        
        Args:
            df (pd.DataFrame): Input dataframe.
            groups (list): List of group names to filter for real taus.
        """
        filtered = []
        other_df = df[~df['group'].isin(groups)]
        for group in groups:
            group_df = df[df['group'] == group]
            cond_1 = group_df['LDTau_genflav'] >= 5
            cond_2 = group_df['SDTau_genflav'] >= 5
            filtered_group = group_df[cond_1 | cond_2]
            print(f"Total number of MC {group} events with real taus: {filtered_group['weight'].sum()}")
            filtered.append(filtered_group)
        # Combine filtered groups with unfiltered others
        return pd.concat(filtered + [other_df], ignore_index=True)
    
    @staticmethod
    def keep_fakes(df) -> pd.DataFrame:
        """Return a dataframe with only TTbar + DYJets fake tau events."""
        ttbar_df = df[df['group'] == 'TTbar']
        # Apply the condition only to TTbar group
        cond_1 = ttbar_df['LDTau_genflav'] < 5
        cond_2 = ttbar_df['SDTau_genflav'] < 5
        filtered_ttbar = ttbar_df[cond_1 & cond_2]
        dyjets_df = df[df['group'] == 'DYJets']
        filtered_dyjets = dyjets_df[(dyjets_df['LDTau_genflav'] < 5) & (dyjets_df['SDTau_genflav'] < 5)]
        print(f"Total number of MC TTbar events with fake taus: {filtered_ttbar['weight'].sum()}")
        print(f"Total number of MC DYJets events with fake taus: {filtered_dyjets['weight'].sum()}")
        return pd.concat([filtered_ttbar, filtered_dyjets], ignore_index=True)
    
    @staticmethod
    def count_real_taus(df, groups=['TTbar']):
        """Return the number of MC events with real taus."""
        copy = df.copy()
        print(f"Total number of events in data: {copy[copy['group'] == 'Data']['weight'].sum()}")
        copy = copy[copy['group'] != 'Data']  # Exclude data
        MCdf = FakeUtil.keep_real_taus(copy, groups)
        _ = FakeUtil.keep_fakes(copy)
        print(f"Total Number of MC events with real taus: {MCdf['weight'].sum()}")
        return MCdf
    

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

def get_mbb_train_test(df_SS, df_OS, mbb_cut=90):
    """Obtain train and test sets based on the DiJet mass region division."""
    filter_mass = lambda df: df.copy()[df['DiJet_mass'] > mbb_cut]
    df_SS = filter_mass(df_SS)
    df_OS = filter_mass(df_OS)
    return get_train_test(df_SS, df_OS, lambda df: df['DiJet_mass'] < 160)

def apply_all(dfs, func):
    filtered = [None] * len(dfs)
    for i, df in enumerate(dfs):
        filtered[i] = func(df.copy())
    return filtered

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

def train_mlp_rwgt(train_ori, train_tar, session_name, name='TopQuark'):
    """Train a MLP reweighter from one region to another."""
    mlp_rwgter = SingleMLPRwgter(train_ori, train_tar, 'weight', f"{df_base_dir}/training/{name}", drop_kwd=drop_kwds)
    mlp_rwgter.prep_data()
    shallow_args= {
        'num_epochs': 100,
        'hidden_arch': 'high_dim',
        'batch_size': 256,
        'lr': 0.001,
        'save': True,
        'savename': f'{session_name}.pth',
        'save_interval': 50}
    mlp_rwgter.train(**shallow_args)

    return mlp_rwgter

def train_and_reweight(ori, tar, reweight_name):
    """Train and load the MLP reweighter."""
    train_ori, val_ori, train_tar, val_tar = get_mbb_train_test(ori, tar, mbb_cut=0)
    mlp_rwgter = train_mlp_rwgt(train_ori, train_tar, reweight_name)
    drop_kwds = ['Gen', 'weight_values', 'Weight_values', 'OS', 'group', 'gen', 'dataset', 'label', 'id', 'year', 'Tau_charge', 'X_num']
    rwgt = mlp_rwgter.reweight(ori, tar.weight.sum(), drop_kwds, True, save_name=reweight_name)
    plot_dir_name = f'/Users/yuntongzhou/Desktop/Dihiggszztt/output/plots/{reweight_name}'
    show_infer = lambda df: df.copy()[df['DiJet_mass'] < 160]
    plot_rwgt_results(val_ori, val_tar, show_infer(rwgt), plot_dir_name)
    return mlp_rwgter, rwgt

def load_and_plot(ori, tar, reweight_name):
    show_infer = lambda df: df.copy()[(df['DiJet_mass'] < 160) & (df['DiJet_mass'] > 90)]
    show_val = lambda df: df.copy()[df['DiJet_mass'] < 90]
    show_training = lambda df: df.copy()[df['DiJet_mass'] > 160]
    rwgt = pd.read_csv(f'/Users/yuntongzhou/Desktop/Dihiggszztt/output/training/TopQuark/{reweight_name}.csv')
    plot_rwgt_results(show_infer(ori), show_infer(tar), show_infer(rwgt), f'/Users/yuntongzhou/Desktop/Dihiggszztt/output/plots/{reweight_name}')
    plot_rwgt_results(show_training(ori), show_training(tar), show_training(rwgt), f'/Users/yuntongzhou/Desktop/Dihiggszztt/output/plots/{reweight_name}_train')
    plot_rwgt_results(show_val(ori), show_val(tar), show_val(rwgt), f'/Users/yuntongzhou/Desktop/Dihiggszztt/output/plots/{reweight_name}_val')


def load_and_select(input_name, mode, out_dir, **extra_kwargs):
    input_df = pd.read_csv(input_name)
    input_df = add_extra_features(input_df)
    input_prefix = input_name.split('/')[-1].replace('.csv', '')
    if mode == 'OSSS':
        os_df, ss_df = ABCDUtil.split_dataframe(input_df, lambda df: df['OS'] == True)
        os_df.to_csv(pjoin(out_dir, f'{input_prefix}_OS.csv'), index=False)
        ss_df.to_csv(pjoin(out_dir, f'{input_prefix}_SS.csv'), index=False)
        logging.info(f"OS dataframe saved to {pjoin(out_dir, f'{input_prefix}_OS.csv')}")
        logging.info(f"SS dataframe saved to {pjoin(out_dir, f'{input_prefix}_SS.csv')}")
    elif mode == 'MBB':
        mbb_cut = extra_kwargs.get('mbb_cut', 90)
        filter_func = lambda df: df.copy()[df['DiJet_mass'] > mbb_cut]
        high_mbb, low_mbb = ABCDUtil.split_dataframe(input_df, filter_func)
        high_mbb.to_csv(pjoin(out_dir, f'{input_prefix}_highMbb.csv'), index=False)
        low_mbb.to_csv(pjoin(out_dir, f'{input_prefix}_lowMbb.csv'), index=False)
        logging.info(f"High Mbb dataframe saved to {pjoin(out_dir, f'{input_prefix}_highMbb.csv')}")
        logging.info(f"Low Mbb dataframe saved to {pjoin(out_dir, f'{input_prefix}_lowMbb.csv')}")
    else:
        raise ValueError(f"Unsupported mode: {mode}. Choose either 'OSSS' or 'MBB'.")

if __name__ == "__main__":
    setup_logging()
    
    parser = RichArgumentParser()
    parser.add_argument('-i', '--input', required=True, help="Input filename containing data after HH-btag inference.")
    parser.add_argument('-o', '--output', required=True, help="Output directory to save the processed data.")
    parser.add_argument('mode', choices=['OSSS', 'MBB'], help="Mode of operation: OSSS for OS/SS analysis, MBB for DiJet-mass-based analysis.")
    parser.add_argument('--mbb_cut', type=float, default=120, help="Mbb cut value for MBB mode. Default is 120 GeV.")
    args = parser.parse_args()
    
    FileSysHelper.checkpath(args.output, createdir=True)
    if not os.path.isfile(args.input):
        raise FileNotFoundError(f"Input file {args.input} does not exist.")
    
    load_and_select(args.input, args.mode, args.output, mbb_cut=args.mbb_cut)
    