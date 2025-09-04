from src.utils.mathutil import MathUtil
from src.utils.ioutil import setup_logging
from src.plotting.visutil import CSVPlotter
from src.utils.filesysutil import FileSysHelper
import logging, os
import pandas as pd
import json
from src.utils.displayutil import RichArgumentParser
from hep_rewgt_tk.reweight_nn import SingleMLPRwgter
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import TensorDataset, DataLoader

drop_kwds = ['Gen', 'weight_values', 'Weight_values', 'OS', 'group', 'gen', 'dataset', 'label', 'id', 'year', 'Tau_charge', 'X_num', 'weight', 'Tau', 'btag'] 
features_train = ['Bjet1_pt', 'Bjet2_pt', 'Bjet1_eta', 'Bjet2_eta', 'Bjet1_phi', 'Bjet2_phi', 'Bjet1_mass', 'Bjet2_mass',
                 'LDTau_pt', 'LDTau_mass', 'LDTau_eta', 'LDTau_phi', 'SDTau_pt', 'SDTau_eta', 'SDTau_phi', 'SDTau_mass', 
                 'MET_pt', 'MET_phi']

def train_and_reweight(data_df, mc_df, features, n_epochs=80, batch_size=1024, lr=1e-3):
    """
    Train a NN classifier to separate Data vs MC, 
    then compute reweighted event weights for Reco p3 and Reco p2.
    
    Args:
        data_df (pd.DataFrame): Data events
        mc_df   (pd.DataFrame): MC events
        features (list[str]): list of feature names
        n_epochs (int): training epochs
        batch_size (int): batch size
        lr (float): learning rate
    
    Returns:
        dict with:
            "model" : trained NN model
            "w_data_reco_p3" : np.array of reco p3 weights for data
            "w_data_reco_p2" : np.array of reco p2 weights for data
    """

    # -----------------------
    # 0) Prepare inputs
    # -----------------------
    p1 = data_df[features].to_numpy().astype(np.float32)   # Data
    p2 = mc_df[features].to_numpy().astype(np.float32)     # MC

    X = np.vstack([p1, p2])
    y = np.hstack([
        np.zeros(len(p1), dtype=np.float32),   # Data = 0
        np.ones(len(p2),  dtype=np.float32)    # MC   = 1
    ])
    w = np.hstack([
        np.ones(len(p1), dtype=np.float32),
        mc_df['weight'].to_numpy().astype(np.float32)
    ])

    # Shuffle
    perm = np.random.permutation(len(X))
    X, y, w = X[perm], y[perm], w[perm]

    # Torch tensors
    X_tensor = torch.tensor(X, dtype=torch.float32)
    y_tensor = torch.tensor(y, dtype=torch.float32).unsqueeze(1)
    w_tensor = torch.tensor(w, dtype=torch.float32).unsqueeze(1)

    dataset = TensorDataset(X_tensor, y_tensor, w_tensor)
    loader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    # -----------------------
    # 1) Define NN classifier
    # -----------------------
    class SimpleNN(nn.Module):
        def __init__(self, d):
            super().__init__()
            self.net = nn.Sequential(
                nn.Linear(d, 64),
                nn.ReLU(),
                nn.Linear(64, 32),
                nn.ReLU(),
                nn.Linear(32, 16),
                nn.ReLU(),
                nn.Linear(16, 1)   # no Sigmoid
            )
        def forward(self, x): return self.net(x)

    model = SimpleNN(len(features))
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    criterion = nn.BCEWithLogitsLoss(reduction="none")

    # -----------------------
    # 2) Training loop with validation
    # -----------------------
    # Split into train/validation
    train_size = int(0.8 * len(dataset))
    val_size = len(dataset) - train_size
    train_dataset, val_dataset = torch.utils.data.random_split(dataset, [train_size, val_size])
    
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False)
    
    for epoch in range(n_epochs):
        # Training
        model.train()
        total_train_loss = 0.0
        for batch_X, batch_y, batch_w in train_loader:
            optimizer.zero_grad()
            logits = model(batch_X)
            loss = criterion(logits, batch_y)
            loss = (loss * batch_w).mean()
            loss.backward()
            optimizer.step()
            total_train_loss += loss.item() * batch_X.size(0)
        
        # Validation
        model.eval()
        total_val_loss = 0.0
        with torch.no_grad():
            for batch_X, batch_y, batch_w in val_loader:
                logits = model(batch_X)
                loss = criterion(logits, batch_y)
                loss = (loss * batch_w).mean()
                total_val_loss += loss.item() * batch_X.size(0)

        if epoch % 10 == 0:
            avg_train_loss = total_train_loss / len(train_dataset)
            avg_val_loss = total_val_loss / len(val_dataset)
            logging.info(f"Epoch {epoch+1}/{n_epochs}, Train Loss = {avg_train_loss:.4f}, Val Loss = {avg_val_loss:.4f}")

    # -----------------------
    # 3) Prediction on DATA only
    # -----------------------
    X_data_tensor = torch.from_numpy(p1)
    with torch.no_grad():
        s_data = torch.sigmoid(model(X_data_tensor)).numpy().ravel()

    eps = 1e-6
    r_data = s_data / (1.0 - s_data + eps)   # local ratio p2/p1

    logging.info(f"Max probability for belonging to MC only: {s_data.max():.4f}")
    logging.info(f"Min probability for belonging to MC only: {s_data.min():.4f}")
    
    logging.info(f"Max ratio of QCD/Data: {r_data.max():.4f}")
    logging.info(f"Min ratio of QCD/Data: {r_data.min():.4f}")
    
    
    # Base weights from data
    w_data_base = data_df["weight"].to_numpy() if "weight" in data_df.columns else np.ones(len(p1), dtype=np.float32)
    w_data_reco_p3 = (1.0 - r_data) * w_data_base
    w_data_reco_p2 = r_data * w_data_base

    # -----------------------
    # 4) Normalization
    # -----------------------
    target_sum = data_df["weight"].sum() - mc_df['weight'].sum()
    norm_factor_p3 = target_sum / (w_data_reco_p3.sum() + 1e-12)
    norm_factor_p2 = target_sum / (w_data_reco_p2.sum() + 1e-12)

    w_data_reco_p3 *= norm_factor_p3
    w_data_reco_p2 *= norm_factor_p2

    return {
        "model": model,
        "w_data_reco_p3": w_data_reco_p3,
        "w_data_reco_p2": w_data_reco_p2
    }

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

    

