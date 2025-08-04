import os, logging
import pandas as pd

from hep_rewgt_tk.reweight_nn import SingleMLPRwgter

from src.plotting.visutil import CSVPlotter
from src.plotting.summary import PostPreselProcessor
from src.utils.mathutil import MathUtil, ABCDUtil
from src.utils.ioutil import setup_logging
from src.utils.datautil import CutflowProcessor
from src.utils.displayutil import RichArgumentParser

luminosity = {"2022PreEE": (5.0104+2.9700) * 1000, "2022PostEE": (5.8070+17.7819+3.0828) * 1000, "2023Summer": 32.7 * 1000, "2022": 1}
regroup_dict = {"Others": ['WJets', 'WZ', 'WW', 'WWW', 'ZZZ', 'WZZ', 'WWZ'], 'HH': ['ggF']}

drop_kwds = ['Gen', 'weight_values', 'Weight_values', 'OS', 'group', 'gen', 'dataset', 'label', 'id', 'year', 'Tau_charge', 'X_num'] 

pjoin = os.path.join

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

df_base_dir = "/Users/yuntongzhou/Desktop/Dihiggszztt/output"
plt_base_dir = "/Users/yuntongzhou/Desktop/Dihiggszztt/plots"
raw_base_dir = "/Users/yuntongzhou/Desktop/Dihiggszztt/preprocessed"
meta_data_dir = "/Users/yuntongzhou/Desktop/Dihiggszztt/HHtobbtautau/data"

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



def add_inv_mass_dR(df):
    """Prepare the invariant mass and dR for the given dataframe."""
    # List of particle pairs for system 4-vectors
    system_pairs = [('LDTau', 'SDTau', 'DiTau'),
        ('LDBjet', 'SDBjet', 'DiJet')]

    # List of particle pairs for dR calculations
    dr_pairs = [('LDTau', 'SDTau', 'Tau_dR'),
        ('LDBjet', 'SDBjet', 'Bjet_dR'),
        ('DiTau', 'DiJet', 'RecoH_dR')]

    # Add system 4-vectors
    for p1, p2, sys in system_pairs:
        MathUtil.add_system_4vec(df, p1, p2, sys)

    # Add dR values
    for p1, p2, name in dr_pairs:
        MathUtil.add_dR(df, p1, p2, name)

    # Add HT
    MathUtil.add_HT(df, ['LDTau', 'SDTau', 'LDBjet', 'SDBjet'], 'HT')

    # Add momentum for all particles
    particles = ['LDTau', 'SDTau', 'LDBjet', 'SDBjet', 'DiTau', 'DiJet']
    for particle in particles:
        MathUtil.add_f_momentum(df, particle)

    # Calculate OS
    df['OS'] = df['LDTau_charge']*df['SDTau_charge'] < 0

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
    def get_os_ss(data_path, channel_name, output_dir) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Process return OS and SS dataframes."""
        def select_sign(df, sign='OS'):
            add_inv_mass_dR(df)
            if sign == 'OS':
                return df[df['OS']]
            elif sign == 'SS':
                return df[~df['OS']]
            else:
                raise ValueError("sign must be 'OS' or 'SS'")

        selOS = lambda df: select_sign(df, 'OS')
        selSS = lambda df: select_sign(df, 'SS')

        cwd = os.getcwd()
        meta_dir = pjoin(cwd, 'data/weightedMC')

        base_config = {'wgt_name': 'Generator_weight',
            'meta_dir': meta_dir,
            'output_base': output_dir}

        if not os.path.exists(base_config['output_base']):
            os.makedirs(base_config['output_base'])
            
        os_dfs = []
        ss_dfs = []
        cp = CSVPlotter(outdir=plt_base_dir)
        for year in os.listdir(data_path):
            if year.startswith('.'):
                continue

            print(f"Processing year: {year}")

            lumi = luminosity[year]
            args = {'metadata_path': pjoin(base_config['meta_dir'], f'{year}.json'), 'postp_output': pjoin(base_config['output_base'], channel_name, year),
                'per_evt_wgt': base_config['wgt_name'], 'luminosity': lumi, 'datasource': pjoin(data_path, year)}
            
            if not os.path.exists(args['postp_output']):
                os.makedirs(args['postp_output'])

            # Process OS events
            os_df = cp.process_datasets(**args, extraprocess=selOS, selname='OS Tau')
            os_dfs.append(os_df)

            # Process SS events
            ss_df = cp.process_datasets(**args, extraprocess=selSS, selname='SS Tau')
            ss_dfs.append(ss_df)

        return pd.concat(os_dfs), pd.concat(ss_dfs)
    
    @staticmethod
    def get_total_df(data_relPath, output_relPath):
        """Get the total dataframe (OS/SS) by combining all preprocessed data of all MC groups and actual data."""
        data_path = pjoin(raw_base_dir, data_relPath)
        output_path = pjoin(df_base_dir, output_relPath)

        if not os.path.exists(output_path):
            os.makedirs(output_path)
        
        os_df, ss_df = OSSSUtil.get_os_ss(data_path, output_relPath)

        os_df.to_csv(pjoin(output_path, 'OS.csv'), index=False)
        ss_df.to_csv(pjoin(output_path, 'SS.csv'), index=False)

        oscf_df = CutflowProcessor.cutflow_by_sum(os_df, 'zerob_os', None)
        sscf_df = CutflowProcessor.cutflow_by_sum(ss_df, 'zerob_ss', None)

        oscf_df = CutflowProcessor.categorize_processes(oscf_df, regroup_dict)
        sscf_df = CutflowProcessor.categorize_processes(sscf_df, regroup_dict)
        oscf_df.to_csv(pjoin(output_path, 'OS_cutflow.csv'))
        sscf_df.to_csv(pjoin(output_path, 'SS_cutflow.csv'))
        logging.info(f"Processed OS and SS dataframes saved to {output_path}")
        
        return os_df, ss_df

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

def read_two_channels_df(output_dir, filter_func=None):
    ABCD_dir = f"/Users/yuntongzhou/Desktop/Dihiggszztt/output/ABCD/{output_dir}"

    Res1b_SS = pd.read_csv(f'{ABCD_dir}/SS1b.csv', index_col=0)
    Res2b_SS = pd.read_csv(f'{ABCD_dir}/SS2b.csv', index_col=0)
    Res1b_OS = pd.read_csv(f'{ABCD_dir}/OS1b.csv', index_col=0)
    Res2b_OS = pd.read_csv(f'{ABCD_dir}/OS2b.csv', index_col=0)
    
    if filter_func:
        Res1b_SS = filter_func(Res1b_SS)
        Res2b_SS = filter_func(Res2b_SS)
        Res1b_OS = filter_func(Res1b_OS)
        Res2b_OS = filter_func(Res2b_OS)

    return Res1b_SS, Res2b_SS, Res1b_OS, Res2b_OS

if __name__ == "__main__":
    parser = RichArgumentParser()
    
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('-i1', '--input1', required=True, type=str, default=None, help='Relative input file path 1 for one b')
    parser.add_argument('-i2', '--input2', required=True, type=str, default=None, help='Relative input file path 2 for two b')
    parser.add_argument('-o', '--output', required=True, type=str, default=None, help='Relative output file path')
    parser.add_argument('-m', '--mode', type=str, default='hadd', help='Mode of operation: hadd, train, infer, plot')

    args = parser.parse_args()

    console_level = logging.INFO if not args.debug else logging.DEBUG
    setup_logging(console_level=console_level)
    logging.info(f"Starting ABCD analysis in {output_home} ...")

    oneb_path = f'{output_home}/{args.input1}'
    twob_path = f'{output_home}/{args.input2}'
    
    OS_2b, OS_1b, SS_2b, SS_1b = getABCDdf(oneb_path, twob_path, args.output, args.output)
 
    post_process_base = f'/Users/yuntongzhou/Desktop/Dihiggszztt/output/{args.output}'

    local_cutflow_base = '/Users/yuntongzhou/Desktop/Dihiggszztt/output/ABCD_Cutflow'

    cfg_1b = {"DIRNAME": None, "DATA_DIR": data_dir, 
            "INPUTDIR": pjoin(post_process_base, 'oneb'),  
            "LOCALOUTPUT": pjoin(local_cutflow_base, args.output, 'oneb'),
            "TRANSFERPATH": None, 
            "IS_MC": True}

    cfg_2b = cfg_1b.copy()
    cfg_2b["INPUTDIR"] = pjoin(post_process_base, 'twob')
    cfg_2b["LOCALOUTPUT"] = pjoin(local_cutflow_base, args.output, 'twob')

    for cfg in [cfg_1b, cfg_2b]:
        logging.info(f"Processing {cfg['INPUTDIR']} ...")
        pp = PostPreselProcessor(cfg, luminosity)
        pp.get_yield('OS')
        pp.get_yield('SS')

    # ABCDTable()

    # prep_training(dfA, 'Res2b_OS.csv')
    # prep_training(dfB, 'Res1b_OS.csv')
    # prep_training(dfC, 'Res2b_SS.csv')
    # prep_training(dfD, 'Res1b_SS.csv')