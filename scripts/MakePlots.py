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
matplotlib.use('Agg')

from scripts.PrepABCD import plot_histograms

def plot_Rwgt_DataMinusMC(src_df, rwgt_df, out_dir):
    from config.plotsetting import H_mass, tau_pt, tau_eta, bjet_pt, bjet_mass, dR, HT, H_pt
    att_dicts = H_mass | tau_pt |bjet_pt | bjet_mass | dR | HT | H_pt

    cp = CSVPlotter(outdir=out_dir)
    
    oneD_df = src_df.copy()
    compare_df = src_df[src_df['group'] == 'Data'].copy()
    oneD_df.loc[oneD_df['group'] != 'Data', 'weight'] *= -1
    rwgt_df['weight'] = rwgt_df['weight_reco_p3'].copy()
    compare_df = compare_df[compare_df['group'] == 'Data'].copy()
    renorm_fac = oneD_df['weight'].sum() / compare_df['weight'].sum()
    compare_df['weight'] *= renorm_fac

    logging.info("Plotting 1D subtraction vs multi-D reweighting results.")
    logging.info(f"Number of events in QCD: {oneD_df['weight'].sum()}")

    cp.plot_shape([oneD_df, rwgt_df, compare_df], labels=['1D subtraction', 'multi-D reweighting', 'Total Data'], 
                  attridict=att_dicts, ratio_ylabel='Pred/Actual', outdir=out_dir,
                  normalize=False, title='Multijet Background', save_suffix='QCD')
    
def plot_Rwgt_SSvsOS(ss_df, os_df, out_dir):
    from config.plotsetting import H_mass, tau_pt, tau_eta, bjet_pt, bjet_mass, dR, HT, H_pt
    att_dicts = H_mass | tau_pt | tau_eta | bjet_pt | bjet_mass | dR | HT | H_pt

    cp = CSVPlotter(outdir=out_dir)
    ss_df = ss_df[ss_df['group'] == 'Data']
    reco_os_df = ss_df.copy()
    reco_os_df['weight'] = ss_df['weight_reco_os'].copy()
    os_df = os_df[os_df['group'] == 'Data']
    renorm_fac = os_df['weight'].sum() / ss_df['weight'].sum()
    ss_df['weight'] *= renorm_fac
    logging.info("Plotting SS reweighted to OS vs actual OS distributions.")
    cp.plot_shape([os_df, ss_df, reco_os_df], labels=['OS data', 'SS data', 'SS reweighted to OS'], 
                  attridict=att_dicts, ratio_ylabel='Pred/Actual', outdir=out_dir,
                  normalize=False, title='Total Background', save_suffix='SSvsOS')
    
    
if __name__ == "__main__":
    setup_logging()
    parser = RichArgumentParser()
    mode_description = """Mode of operation for the script. 
    DataMinusMC: Compare 1D subtration versus MD subtraction via NN density estimation.
    OriVSRwgt: Compare original versus reweighted distributions."""

    parser.add_argument("mode", choices=['DataMinusMC', 'OriVSRwgt'], help=mode_description)
    parser.add_argument("-i", "--input", nargs='+', required=True, help="Input CSV file(s)")
    parser.add_argument("-o", "--output", required=True, help="Output directory")
    
    args = parser.parse_args()

    if args.mode == "DataMinusMC":
        if len(args.input) == 2:
            src_df = pd.read_csv(args.input[0])
            tar_df = pd.read_csv(args.input[1])
            plot_Rwgt_DataMinusMC(src_df, tar_df, args.output)
        else:
            logging.error("DataMinusMC mode requires exactly 2 input CSV files.")
            exit(1)

    elif args.mode == "OriVSRwgt":
        if len(args.input) == 2:
            ss_df = pd.read_csv(args.input[0])
            os_df = pd.read_csv(args.input[1])
            plot_Rwgt_SSvsOS(ss_df, os_df, args.output)
        else:
            logging.error("OriVSRwgt mode requires exactly 2 input CSV files.")
            exit(1)
