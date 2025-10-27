from src.utils.ioutil import setup_logging
import logging, os
import pandas as pd
import json
from src.utils.displayutil import RichArgumentParser
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import TensorDataset, DataLoader
from src.utils.inferutil import infer_ss_to_os, infer_multiclass
from src.utils.trainutil import train_and_reweight_multiclass, train_model, train_and_reweight
from src.utils.statsutil import normalize_mc

pjoin = os.path.join

drop_kwds = ['Gen', 'weight_values', 'Weight_values', 'OS', 'group', 'gen', 'dataset', 'label', 'id', 'year', 'Tau_charge', 'X_num', 'weight', 'Tau', 'btag'] 
features_train = ['Bjet1_pt', 'Bjet2_pt', 'Bjet1_mass', 'Bjet2_mass',
                 'LDTau_pt', 'LDTau_mass', 'SDTau_pt', 'SDTau_mass', 
                 'DiTau_pt', 'DiTau_eta', 'DiTau_phi', 'DiTau_mass', 'DiTau_dR', 
                 'DiJet_dR', 'DiJet_mass', 'DiJet_pt', 'DiJet_eta', 'DiJet_phi',
                 'MET_pt', 'MET_phi']

# -----------------------
# Define NN classifier
# -----------------------
class SimpleNN(nn.Module):
    def __init__(self, d, p_dropout=0.3, num_classes=1):  # Add num_classes parameter
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(d, 64),
            nn.LeakyReLU(),
            nn.BatchNorm1d(64),
            nn.Dropout(p_dropout),
            nn.Linear(64, 32),
            nn.LeakyReLU(),
            nn.BatchNorm1d(32),
            nn.Dropout(p_dropout),
            nn.Linear(32, 16),
            nn.LeakyReLU(),
            nn.Dropout(p_dropout),
            nn.Linear(16, num_classes)   # no Sigmoid
        )
    def forward(self, x): 
        return self.net(x)

# ----------- Training routine
# -----------------------

def dataMinusMC(data_df, mc_df, out_dir, training_args, session_name=''):
    results_dict = train_and_reweight(data_df, mc_df, features_train, **training_args)
    w_data_reco_p3 = results_dict['w_data_reco_p3']
    w_data_reco_p2 = results_dict['w_data_reco_p2']
    model = results_dict['model']
    torch.save(model.state_dict(), os.path.join(out_dir, f"{session_name}_model.pth"))
    data_df['weight_reco_p3'] = w_data_reco_p3
    data_df['weight_reco_MC'] = w_data_reco_p2
    data_df.to_csv(os.path.join(out_dir, f"{session_name}_data_new_weights.csv"), index=False)
    logging.info(f"Data weights saved to {os.path.join(out_dir, f'{session_name}_data_new_weights.csv')}")
    
def train_and_reweight_ss_os(ss_df, os_df, features, n_epochs=80, batch_size=1024, lr=1e-3):
    """
    Train a NN classifier to separate SS vs OS events,
    then compute reweighted OS weights for the SS sample.
    
    Returns:
        dict with:
            "model"           : trained NN model
            "r_data"          : np.array of local ratios (OS/SS)
            "w_data_reco_OS"  : np.array of normalized OS weights for SS events
    """

    # -----------------------
    # 0) Prepare inputs
    # -----------------------
    p1 = ss_df[features].to_numpy().astype(np.float32)   # SS = 0
    p2 = os_df[features].to_numpy().astype(np.float32)   # OS = 1

    X = np.vstack([p1, p2])
    y = np.hstack([
        np.zeros(len(p1), dtype=np.float32),   # SS = 0
        np.ones(len(p2),  dtype=np.float32)    # OS = 1
    ])
    w = np.hstack([
        np.ones(len(p1), dtype=np.float32),
        os_df['weight'].to_numpy().astype(np.float32)
    ])

    # Shuffle
    perm = np.random.permutation(len(X))
    X, y, w = X[perm], y[perm], w[perm]

    # Torch tensors
    X_tensor = torch.tensor(X, dtype=torch.float32)
    y_tensor = torch.tensor(y, dtype=torch.float32).unsqueeze(1)
    w_tensor = torch.tensor(w, dtype=torch.float32).unsqueeze(1)

    dataset = TensorDataset(X_tensor, y_tensor, w_tensor)

    # -----------------------
    # 1) Train the model
    # -----------------------
    model = SimpleNN(len(features))
    model = train_model(model, dataset, n_epochs=n_epochs, batch_size=batch_size, lr=lr)
    # -----------------------
    results_dict = infer_ss_to_os(model, ss_df, os_df, features)
    results_dict["model"] = model

    return results_dict

def SSToOS(ss_df, os_df, out_dir, training_args, session_name=''):
    ss_df = ss_df[ss_df['group'] == 'Data'].copy()
    os_df = os_df[os_df['group'] == 'Data'].copy()
    results_dict = train_and_reweight_ss_os(ss_df, os_df, features_train, **training_args)
    w_data_reco_os = results_dict['w_data_reco_os']
    model = results_dict['model']
    torch.save(model.state_dict(), os.path.join(out_dir, f"{session_name}_ss_to_os_model.pth"))
    ss_df['weight_reco_os'] = w_data_reco_os
    ss_df.to_csv(os.path.join(out_dir, f"{session_name}_ss_data.csv"), index=False)
    logging.info(f"SS Data with new OS weights saved to {os.path.join(out_dir, f'{session_name}_ss_data.csv')}")

def SSDataToOSQCD(ss_df, os_df, out_dir, training_args, session_name='', save_interval=None):
    ss_data = ss_df[ss_df['group'] == 'Data'].copy()
    os_data = os_df[os_df['group'] == 'Data'].copy()
    os_mc = os_df[os_df['group'] != 'Data'].copy()
    logging.info(f"Number of SS Data events: {len(ss_data)}")
    logging.info(f"Number of OS MC events: {os_mc['weight'].sum()}")
    logging.info(f"Number of OS Data events: {len(os_data)}")
    # os_mc, renorm_fac = normalize_mc(os_data, os_mc, feature='DiTau_pt')

    results_dict = train_and_reweight_multiclass(SimpleNN, ss_data, os_mc, os_data, features_train, save_interval=save_interval, **training_args)

    model = results_dict['model']
    w_reco_qcd = results_dict['w_reco_qcd']
    if results_dict['saved_models']:
        for idx, state_dict in enumerate(results_dict['saved_models']):
            torch.save(state_dict, os.path.join(out_dir, f"{session_name}_ss_to_os_qcd_model_epoch{idx*save_interval}.pth"))
            logging.info(f"Saved intermediate model at epoch {idx*save_interval} to {os.path.join(out_dir, f'{session_name}_ss_to_os_qcd_model_epoch{idx*save_interval}.pth')}")

    torch.save(model.state_dict(), os.path.join(out_dir, f"{session_name}_ss_to_os_qcd_model.pth"))
    ss_data['weight_reco_os_fakes'] = w_reco_qcd
    ss_data.to_csv(os.path.join(out_dir, f"{session_name}_ss_data_qcd.csv"), index=False)
    logging.info(f"SS Data with new OS Fake weights saved to {os.path.join(out_dir, f'{session_name}_ss_data_qcd.csv')}")

def load_model(model_path, input_dim, num_classes=1):
    model = SimpleNN(input_dim, num_classes=num_classes)
    model.load_state_dict(torch.load(model_path))
    model.eval()
    return model

if __name__ == "__main__":
    setup_logging()
    parser = RichArgumentParser()
    parser.add_argument("--config", help="Path to JSON config file. See an example training_example.json", default=None)

    json_args = parser.parse_args()

    with open(json_args.config, 'r') as f:
        args = json.load(f)

    if args['mode'] == 'dataMinusMC':
        total_df = pd.read_csv(args['input_csv'])
        data_df = total_df[total_df['group'] == 'Data'].copy()
        logging.info(f"Number of total events: {len(data_df)}")
        mc_df = total_df[total_df['group'] != 'Data'].copy()
        logging.info(f"Number of MC modelled events: {len(mc_df)}")
        dataMinusMC(data_df, mc_df, args['output_dir'], training_args=args['training_args'], session_name=args['session_name'])
    elif args['mode'] == 'SStoOS':
        ss_df = pd.read_csv(args['ss_input_csv'])
        os_df = pd.read_csv(args['os_input_csv'])
        SSToOS(ss_df, os_df, args['output_dir'], training_args=args['training_args'], session_name=args['session_name'])
    elif args['mode'] == 'SStoFakes':
        ss_df = pd.read_csv(args['ss_input_csv'])
        if args['inference_only']:
            if args.get("model_epoch", None) is not None:
                model_path = pjoin(args['output_dir'], f"{args['session_name']}_ss_to_os_qcd_model_epoch{args['model_epoch']}.pth")
            else:
                model_path = pjoin(args['output_dir'], f"{args['session_name']}_ss_to_os_qcd_model.pth")
                
            model = load_model(model_path, input_dim=len(features_train), num_classes=3)
            results_dict = infer_multiclass(model, ss_df[ss_df['group'] == 'Data'], features_train)
            w_reco_qcd = results_dict['w_reco_qcd']
            ss_df.loc[ss_df['group'] == 'Data', 'weight_reco_os_fakes'] = w_reco_qcd
            new_ss_name = f"{args['session_name']}_ss_data_qcd.csv"
            ss_df.to_csv(pjoin(args['output_dir'], new_ss_name), index=False)
            logging.info(f"SS Data with new OS Fake weights saved to {pjoin(args['output_dir'], new_ss_name)}")
        else:
            os_df = pd.read_csv(args['os_input_csv'])
            SSDataToOSQCD(ss_df, os_df, args['output_dir'], training_args=args['training_args'], session_name=args['session_name'], 
                          save_interval=args.get('save_interval', None))
    else:
        raise ValueError(f"Unknown mode {args['mode']}. Supported modes: dataMinusMC, SStoOS, SStoFakes")

    

