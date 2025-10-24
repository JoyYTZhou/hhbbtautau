import os, logging, glob, json
import pandas as pd
from rich.table import Table
from rich.console import Console
from rich.prompt import Prompt, Confirm
from src.plotting.visutil import CSVPlotter
from src.utils.ioutil import setup_logging
from src.utils.displayutil import RichArgumentParser
import matplotlib
from src.utils.statsutil import normalize_mc

pjoin = os.path.join
matplotlib.use('Agg')

def plot_Rwgt_DataMinusMC(src_df, rwgt_df, out_dir):
    from config.plotsetting import H_mass, tau_pt, bjet_pt, bjet_mass, dR, HT, H_pt
    att_dicts = H_mass | tau_pt |bjet_pt | bjet_mass | dR | HT | H_pt

    cp = CSVPlotter(outdir=out_dir)
    
    oneD_df = src_df.copy()
    compare_df = src_df[src_df['group'] == 'Data'].copy()
    rwgt_df['weight'] = rwgt_df['weight_reco_os_fakes'].copy()
    
    
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
    from src.utils.inferutil import data_subtract_mc
    att_dicts = H_mass | tau_pt | tau_eta | bjet_pt | bjet_mass | dR | HT | H_pt

    cp = CSVPlotter(outdir=out_dir)
    ss_df = ss_df[ss_df['group'] == 'Data']
    reco_os_df = ss_df.copy()
    reco_os_df['weight'] = ss_df['weight_reco_os_fakes'].copy()
    logging.info(f"reco_os_df['weight'].sum(): {reco_os_df['weight'].sum()}")

    # Find events with weights less than -20
    negative_events = reco_os_df[reco_os_df['weight'] < -20]
    logging.info(f"Number of events with weight < -20: {len(negative_events)}")
    if len(negative_events) > 0:
        logging.info(f"Weights of events < -20: {negative_events['weight'].tolist()}")
        if len(negative_events) > 10:
            logging.warning("More than 10 events have weight < -20. Check the reweighting procedure carefully.")
            logging.warning("Exiting...")
            exit(1)
        # Remove events with weight < -20
        reco_os_df = reco_os_df[reco_os_df['weight'] >= -10]
        logging.info(f"Number of events in reco_os_df after filtering: {len(reco_os_df)}")
        logging.info(f"Total weight in reco_os_df after filtering: {reco_os_df['weight'].sum()}")

    os_data = os_df[os_df['group'] == 'Data'].copy()
    os_mc = os_df[os_df['group'] != 'Data'].copy()
    
    os_mc, renorm_fac = normalize_mc(os_data, os_mc, feature='DiTau_dR')
    
    os_df = data_subtract_mc(pd.concat([os_data, os_mc], ignore_index=True))
    logging.info(f"Number of events in OS QCD from subtraction: {os_df['weight'].sum()}")

    renorm_fac = os_df['weight'].sum() / ss_df['weight'].sum()
    ss_df['weight'] *= renorm_fac
    logging.info(f"Number of events in SS Data after renormalization: {ss_df['weight'].sum()}")
    
    renorm_fac = os_df['weight'].sum() / reco_os_df['weight'].sum()
    reco_os_df['weight'] *= renorm_fac
    logging.info(f"Number of events in QCD Prediction from SS Data after renormalization: {reco_os_df['weight'].sum()}")
    
    logging.info("Plotting SS reweighted to OS vs actual OS distributions.")
    cp.plot_shape([os_df, ss_df, reco_os_df], labels=['OS QCD from subtraction', 'SS Data', 'QCD Prediction from SS Data'], 
                   attridict=att_dicts, ratio_ylabel='Pred/Actual', outdir=out_dir,
                   normalize=False, title='Total Background', save_suffix='SSvsOS_QCD')

def compare(df1, df2, out_dir, df1_label, df2_label):
    from config.plotsetting import H_mass, tau_pt, tau_eta, bjet_pt, bjet_mass, dR, HT, H_pt
    att_dicts = H_mass | tau_pt | tau_eta | bjet_pt | bjet_mass | dR | HT | H_pt

    cp = CSVPlotter(outdir=out_dir)
    logging.info("Comparing two datasets.")
    df1[df1['group'] != 'Data'].weight *= -1
    logging.info(f"Number of events in {df1_label}: {df1['weight'].sum()}")
    df2[df2['group'] != 'Data'].weight *= -1
    logging.info(f"Number of events in {df2_label}: {df2['weight'].sum()}")
    cp.plot_shape([df1, df2], labels=[df1_label, df2_label], 
                  attridict=att_dicts, ratio_ylabel='ratio', outdir=out_dir,
                  normalize=True, title='Dataset Comparison', save_suffix='Compare')
    
if __name__ == "__main__":
    setup_logging()
    program_description = """Script to generate plots for different analysis methods
    Mode of operation for the script. 
    DataMinusMC: Compare 1D subtration versus MD subtraction via NN density estimation.
    OriVSRwgt: Compare original versus reweighted distributions.
    Compare: Compare two datasets with given labels."""

    parser = RichArgumentParser(description=program_description)
    parser.add_argument("--config", help="Path to JSON config file. See an example training_example.json", default=None)

    json_args = parser.parse_args()

    with open(json_args.config, 'r') as f:
        args = json.load(f)

    if args['mode'] == "DataMinusMC":
        src_df = args['src_df']
        tar_df = args['tar_df']
        plot_Rwgt_DataMinusMC(src_df, tar_df, args['output_dir'])

    elif args['mode'] == "OriVSRwgt":
        ss_df = pd.read_csv(args['ss_df'])
        os_df = pd.read_csv(args['os_df'])
        plot_Rwgt_SSvsOS(ss_df, os_df, args['output_dir'])

    elif args['mode'] == "Compare":
        df1 = args['df1']
        df2 = args['df2']
        label1 = args['label1']
        label2 = args['label2']
        compare(df1, df2, args['output_dir'], label1, label2)
