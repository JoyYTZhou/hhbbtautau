import os, logging, re
import pandas as pd
import numpy as np
from rich.table import Table
from rich.console import Console
from src.plotting.visutil import CSVPlotter
from src.utils.mathutil import MathUtil, ABCDUtil
from src.utils.filesysutil import FileSysHelper
from src.utils.ioutil import setup_logging
from src.utils.datautil import CutflowProcessor
from src.utils.displayutil import RichArgumentParser, print_dataframe_rich
import matplotlib

pjoin = os.path.join
matplotlib.use('Agg')  # Use a non-interactive backend for plotting

PARENT_DIR = os.path.dirname(os.path.realpath(__file__))
SRC_DIR = os.path.dirname(PARENT_DIR)
META_DIR = os.path.join(SRC_DIR, 'data')

luminosity = {"2022PreEE": (5.0104+2.9700) * 1000, "2022PostEE": (5.8070+17.7819+3.0828) * 1000, "2023Summer": 32.7 * 1000, "2022": 1}
regroup_dict = {"Others": ['WJets', 'WZ', 'WW', 'WWW', 'ZZZ', 'WZZ', 'WWZ'], 'HH': ['ggF']}

drop_kwds = ['Gen', 'weight_values', 'Weight_values', 'OS', 'group', 'gen', 'dataset', 'label', 'id', 'year', 'Tau_charge', 'X_num'] 

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


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

def plot_histograms(df, plot_dir, region_name=''):
    """Plot histograms for the given dataframe and save them to the specified directory."""
    cp = CSVPlotter(outdir=plot_dir)
    from config.plotsetting import H_mass, tau_pt, tau_eta, bjet_pt, bjet_mass, dR

    att_dicts = H_mass | tau_pt | tau_eta | bjet_pt | bjet_mass | dR
    if not os.path.exists(plot_dir):
        os.makedirs(plot_dir)
    
    cp.plot_SvB(df, att_dicts, title=region_name, save_name=f'SvB', lumi=34.65, rescale_sig=1)
    logging.warning(f"Histograms for {region_name} saved to {plot_dir}")

def get_top_bjets(df):
    """Get features for the two bjets with highest HHbtag scores."""
    # Get HHbtag scores and find top 2 indices
    hhbtag_cols = [f'Bjet{i}_HHbtag' for i in range(1, 11)]
    hhbtags = df[hhbtag_cols].fillna(-np.inf)
    
    # Find indices of top 2 bjets for each row
    top_indices = hhbtags.values.argsort(axis=1)[:, -2:][:, ::-1]  # Descending order

    result_data = {}
    
    # Get all bjet feature columns
    bjet_features = [col for col in df.columns if col.startswith('Bjet')]
    
    # Extract features for top 2 bjets
    for feature_base in set(col.split('_', 1)[1] for col in bjet_features):
        # Get all columns for this feature (e.g., all 'pt' columns: Bjet1_pt, Bjet2_pt, etc.)
        feature_cols = [col for col in bjet_features if col.endswith(f'_{feature_base}')]
        feature_values = df[feature_cols].values  # Shape: (n_events, n_bjets)
        
        # Extract values for top 2 bjets using advanced indexing
        for i in range(2):
            bjet_indices = top_indices[:, i]  # Indices of i-th best bjet for each event
            selected_values = feature_values[np.arange(len(df)), bjet_indices]
            result_data[f'Bjet{i+1}_{feature_base}'] = selected_values

    return pd.DataFrame(result_data, index=df.index).fillna(method='bfill', axis=1)

def add_extra_features(df):
    """Add extra features to the dataframe for further analysis."""
    pattern = re.compile(r'Bjet(\d+)_.+')
    bjet_columns = [col for col in df.columns if pattern.match(col)]
    other_columns = [col for col in df.columns if col not in bjet_columns]
    
    other_df = df[other_columns].copy()
    
    bjet_df = get_top_bjets(df[bjet_columns])
    df_copy = pd.concat([other_df, bjet_df], axis=1)
    logging.info("Adding extra features to the dataframe.")
    MathUtil.add_system_4vec(df_copy, 'Bjet1', 'Bjet2', 'DiJet')
    MathUtil.add_system_4vec(df_copy, 'DiTau', 'DiJet', 'DiHiggs')
    df_copy['OS'] = ((df_copy['LDTau_charge'] * df_copy['SDTau_charge']) < 0)
    df_copy['HT'] = df_copy['Bjet1_pt'] + df_copy['Bjet2_pt'] + df_copy['LDTau_pt'] + df_copy['SDTau_pt'] + df_copy['MET_pt'] # Total transverse energy
    MathUtil.add_dR(df_copy, 'LDTau', 'SDTau', 'DiTau_dR')
    MathUtil.add_dR(df_copy, 'Bjet1', 'Bjet2', 'DiJet_dR')
    MathUtil.add_dR(df_copy, 'DiTau', 'DiJet', 'DiHiggs_dR')
    logging.info(f"features: {df_copy.columns}")
    return df_copy

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
        

def analyze_taus(df, groups=['TTbar']):
    """Combined method that provides tau analysis with counts table and separated dataframes.
    
    Args:
        df (pd.DataFrame): Input dataframe.
        groups (list): List of group names to filter for real taus.
        
    Returns:
        tuple: (real_tau_df, fake_tau_df) - DataFrames with real and fake tau events
    """
    copy = df.copy()
    data_events = copy[copy['group'] == 'Data']['weight'].sum()
    logging.info(f"Total number of events in data: {data_events}")
    
    # Exclude data for MC analysis
    mc_copy = copy[copy['group'] != 'Data']
    
    # Get real tau events
    real_tau_df = FakeUtil.keep_real_taus(mc_copy, groups)
    
    # Get fake tau events
    fake_tau_df = FakeUtil.keep_fakes(mc_copy)
    
    # Calculate totals
    real_tau_events = real_tau_df['weight'].sum()
    fake_tau_events = fake_tau_df['weight'].sum()
    
    logging.info(f"Total Number of MC events with real taus: {real_tau_events}")
    logging.info(f"Total Number of MC events with fake taus: {fake_tau_events}")
    
    # Display results in a comprehensive table
    table = Table(title="Tau Analysis Summary")
    table.add_column("Category", justify="left", style="cyan", no_wrap=True)
    table.add_column("Event Count", justify="right", style="magenta")
    table.add_column("Percentage", justify="right", style="green")
    
    total_mc = real_tau_events + fake_tau_events
    real_percentage = (real_tau_events / total_mc * 100) if total_mc > 0 else 0
    fake_percentage = (fake_tau_events / total_mc * 100) if total_mc > 0 else 0
    
    table.add_row("Data Events", f"{data_events:.2f}", "N/A")
    table.add_row("MC Events with Real Taus", f"{real_tau_events:.2f}", f"{real_percentage:.1f}%")
    table.add_row("MC Events with Fake Taus", f"{fake_tau_events:.2f}", f"{fake_percentage:.1f}%")
    table.add_row("Total MC Events", f"{total_mc:.2f}", "100.0%")
    
    console = Console()
    console.print(table)

    real_tau_df = pd.concat(real_tau_df, copy[copy['group'] == 'Data'], axis=0)
    
    return real_tau_df, fake_tau_df

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

def load_and_select(input_name, mode, out_dir, root_plt_dir, **extra_kwargs):
    """Load the input dataframe, add extra features, split based on mode, and save the results."""
    input_df = pd.read_csv(input_name, low_memory=False)
    input_df = add_extra_features(input_df)
    input_prefix = input_name.split('/')[-1].replace('.csv', '')
    if mode == 'OSSS':
        os_df, ss_df, os_cutflow = ABCDUtil.split_dataframe(input_df, lambda df: df[df['OS'] == True])
        logging.info(f"OS events: {os_df['weight'].sum()}, SS events: {ss_df['weight'].sum()}")
        os_df.to_csv(pjoin(out_dir, f'{input_prefix}_OS.csv'), index=False)
        FileSysHelper.checkpath(pjoin(root_plt_dir, 'OS'))
        plot_histograms(os_df, pjoin(root_plt_dir, 'OS'), region_name='OS Region')
        ss_df.to_csv(pjoin(out_dir, f'{input_prefix}_SS.csv'), index=False)
        os_cutflow.to_csv(pjoin(out_dir, f'{input_prefix}_OS_cutflow.csv'), index=False)
        FileSysHelper.checkpath(pjoin(root_plt_dir, 'SS'))
        plot_histograms(ss_df, pjoin(root_plt_dir, 'SS'), region_name='SS Region')
        logging.info(f"OS dataframe saved to {pjoin(out_dir, f'{input_prefix}_OS.csv')}")
        logging.info(f"SS dataframe saved to {pjoin(out_dir, f'{input_prefix}_SS.csv')}")
        logging.info(f"OS cutflow saved to {pjoin(out_dir, f'{input_prefix}_OS_cutflow.csv')}")
    elif mode == 'MBB':
        mbb_cut = extra_kwargs.get('mbb_cut', 90)
        filter_func = lambda df: df.copy()[df['DiJet_mass'] > mbb_cut]
        high_mbb, low_mbb, high_cutflow = ABCDUtil.split_dataframe(input_df, filter_func)
        high_mbb.to_csv(pjoin(out_dir, f'{input_prefix}_highMbb.csv'), index=False)
        FileSysHelper.checkpath(pjoin(root_plt_dir, 'HIGH_MBB'))
        plot_histograms(high_mbb, pjoin(root_plt_dir, 'HIGH_MBB'), region_name='High Mbb Region')
        low_mbb.to_csv(pjoin(out_dir, f'{input_prefix}_lowMbb.csv'), index=False)
        high_cutflow.to_csv(pjoin(out_dir, f'{input_prefix}_highMbb_cutflow.csv'), index=False)
        FileSysHelper.checkpath(pjoin(root_plt_dir, 'LOW_MBB'))
        plot_histograms(low_mbb, pjoin(root_plt_dir, 'LOW_MBB'), region_name='Low Mbb Region')
        logging.info(f"High Mbb dataframe saved to {pjoin(out_dir, f'{input_prefix}_highMbb.csv')}")
        logging.info(f"Low Mbb dataframe saved to {pjoin(out_dir, f'{input_prefix}_lowMbb.csv')}")
        logging.info(f"High Mbb cutflow saved to {pjoin(out_dir, f'{input_prefix}_highMbb_cutflow.csv')}")
    elif mode == 'REALTAUS':
        real_taus, fake_taus = analyze_taus(input_df)
        real_taus.to_csv(pjoin(out_dir, f'{input_prefix}_realTaus.csv'), index=False)
        fake_taus.to_csv(pjoin(out_dir, f'{input_prefix}_fakeTaus.csv'), index=False)
        plot_histograms(real_taus, pjoin(root_plt_dir, 'REAL_TAUS'), region_name='Real Taus Region')
        plot_histograms(fake_taus, pjoin(root_plt_dir, 'FAKE_TAUS'), region_name='Fake Taus Region')
    else:
        raise ValueError(f"Unsupported mode: {mode}. Choose either 'OSSS' or 'MBB'.")

if __name__ == "__main__":
    parser = RichArgumentParser()
    parser.add_argument('mode', choices=['OSSS', 'MBB', 'REALTAUS'], help="Mode of operation: OSSS for OS/SS analysis, MBB for DiJet-mass-based analysis, REALTAUS for real/fake tau analysis.")
    parser.add_argument('-i', '--input', required=True, help="Input filename containing data after HH-btag inference.")
    parser.add_argument('-o', '--output', required=True, help="Output directory to save the processed data.")
    parser.add_argument('-p', '--plot_dir', default=None, help="Directory to save plots. If not provided, no plots will be saved.")
    parser.add_argument('--mbb_cut', type=float, default=120, help="Mbb cut value for MBB mode. Default is 120 GeV.")
    parser.add_argument('--quiet', action='store_true', help="Run in quiet mode without logging output to console.")
    args = parser.parse_args()

    if args.quiet:
        setup_logging(console_level=logging.WARNING, file_level=logging.INFO)
    else:
        setup_logging(console_level=logging.INFO, file_level=logging.DEBUG)
    
    FileSysHelper.checkpath(args.output, createdir=True)
    if not os.path.isfile(args.input):
        raise FileNotFoundError(f"Input file {args.input} does not exist.")
    
    load_and_select(args.input, args.mode, args.output, args.plot_dir, mbb_cut=args.mbb_cut)
    