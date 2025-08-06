from src.utils.mathutil import MathUtil
from src.utils.ioutil import setup_logging
from src.plotting.visutil import CSVPlotter
from src.utils.filesysutil import FileSysHelper
import logging, os
import pandas as pd
from src.utils.displayutil import RichArgumentParser

pjoin = os.path.join
PARENT_DIR = os.path.dirname(os.path.realpath(__file__))
SRC_DIR = os.path.dirname(PARENT_DIR)
META_DIR = os.path.join(SRC_DIR, 'data')

luminosity = {"2022PreEE": (5.0104+2.9700) * 1000, "2022PostEE": (5.8070+17.7819+3.0828) * 1000, "2023Summer": 32.7 * 1000, "2022": 1}

def load_dfs(data_dir, output_dir):
    """Load all output csv files from all eras, all groups dataframes from the specified directory and return a concatenated dataframe."""
    FileSysHelper.checkpath(output_dir, createdir=True)
    FileSysHelper.checkpath(data_dir, createdir=False, raiseError=True)

    meta_data_dir = pjoin(META_DIR, 'weightedMC') 

    plt_base_dir = pjoin(output_dir, 'plots')

    cp = CSVPlotter(outdir=plt_base_dir)
    dfs = []

    for year in os.listdir(data_dir):
        if year.startswith('.') or not os.path.isdir(pjoin(data_dir, year)):
            continue
        elif year not in luminosity:
            continue
        
        logging.info(f"Processing year: {year}")
        lumi_yr = luminosity.get(year, 1)
        args = {'metadata_path': pjoin(meta_data_dir, f'{year}.json'), 
                'postp_output': pjoin(output_dir, year),
                'per_evt_wgt': 'Generator_weight', 'luminosity': lumi_yr, 
                'datasource': pjoin(data_dir, year)}
        FileSysHelper.checkpath(args['postp_output'], createdir=True)

        df = cp.process_datasets(**args, extraprocess=False, sig_factor=1)
        df['year'] = year
        dfs.append(df)
    
    return pd.concat(dfs, ignore_index=True)

def prep_infer_input(df):
    """Prepare the input dataframe for HH-btag inference by adding necessary columns."""
    SDBjet_names = [f'Bjet{i}' for i in range(2, 11)]
    LDBjet_names = ['Bjet1']
    total_names = SDBjet_names + LDBjet_names
    
    MathUtil.add_system_4vec(df, 'LDTau', 'SDTau', 'DiTau')

    for name in total_names:
        MathUtil.add_rel_E_pt(df, name)
        MathUtil.add_rel_M_pt(df, name)
        df[f'{name}_htt_dEta'] = df[f'{name}_eta'] - df['DiTau_eta']
        df[f'{name}_htt_dPhi'] = df[f'{name}_phi'] - df['DiTau_phi']
    
    df['DiTau_scalar_pt'] = df['LDTau_pt'] + df['SDTau_pt']
    df['DiTau_MET_dPhi'] = df['DiTau_phi'] - df['MET_phi']
    df['MET_DiTau_rel_pt'] = df['MET_pt'] / df['DiTau_pt']

if __name__ == "__main__":
    setup_logging()
    parser = RichArgumentParser(description="Prepare data for HH-btag inference")
    parser.add_argument('data_dir', type=str, help='Directory containing the csv output files')
    parser.add_argument('output_dir', type=str, help='Directory to save the processed data')
    args = parser.parse_args()
    df = load_dfs(args.data_dir, args.output_dir)
    prep_infer_input(df)
    df.to_csv(pjoin(args.output_dir, 'prepared_data.csv'), index=False)
    logging.info(f"Prepared data saved to {pjoin(args.output_dir, 'prepared_data.csv')}")
