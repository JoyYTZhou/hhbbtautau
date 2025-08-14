from src.utils.mathutil import MathUtil
from src.utils.ioutil import setup_logging
from src.plotting.visutil import CSVPlotter
from src.utils.filesysutil import FileSysHelper
import logging, os
import pandas as pd
from src.utils.displayutil import RichArgumentParser
from hep_rewgt_tk.reweight_nn import SingleMLPRwgter


drop_kwds = ['Gen', 'weight_values', 'Weight_values', 'OS', 'group', 'gen', 'dataset', 'label', 'id', 'year', 'Tau_charge', 'X_num', 'weight'] 

def dataMinusMC(data_df, mc_df, out_dir, num_epochs=100, session_name=''):
    rwgter = SingleMLPRwgter(data_df, mc_df, w_col='weight', out_dir=f'{out_dir}/DataMinusMC/{session_name}', drop_kwd=drop_kwds)
    rwgter.prep_data(drop_neg_wgts=True)
    shallow_args= {
        'num_epochs': num_epochs,
        'hidden_arch': 'high_dim',
        'batch_size': 256,
        'lr': 0.01,
        'save': True,
        'savename': f'{session_name}.pth',
        'save_interval': 50}
    rwgter.train(**shallow_args)
    norm_fac = data_df['weight'].sum() - mc_df['weight'].sum()
    rwgter.reweight(data_df, norm_factor=norm_fac, save=True, filename='QCD_by_Rwgt', method='subtraction')
    

if __name__ == "__main__":
    setup_logging()
    parser = RichArgumentParser()
    parser.add_argument("input_csv", help="Path to input CSV file")
    parser.add_argument("output_dir", help="Path to output directory")
    parser.add_argument("mode", choices=["dataMinusMC"], help="Reweighting mode")
    parser.add_argument("--session_name", default='', help="Session name for training")
    parser.add_argument("--num_epochs", default=100, type=int, help="Number of training epochs")

    args = parser.parse_args()

    if args.mode == 'dataMinusMC':
        total_df = pd.read_csv(args.input_csv)
        data_df = total_df[total_df['group'] == 'Data'].copy()
        logging.info(f"Number of total events: {len(data_df)}")
        mc_df = total_df[total_df['group'] != 'Data'].copy()
        logging.info(f"Number of MC modelled events: {len(mc_df)}")
        dataMinusMC(data_df, mc_df, args.output_dir, session_name=args.session_name)

    

