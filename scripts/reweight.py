from src.utils.ioutil import setup_logging
import logging, os
import pandas as pd
import json
from src.utils.displayutil import RichArgumentParser
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import TensorDataset, DataLoader

drop_kwds = ['Gen', 'weight_values', 'Weight_values', 'OS', 'group', 'gen', 'dataset', 'label', 'id', 'year', 'Tau_charge', 'X_num', 'weight', 'Tau', 'btag'] 
features_train = ['Bjet1_pt', 'Bjet2_pt', 'Bjet1_mass', 'Bjet2_mass',
                 'LDTau_pt', 'LDTau_mass', 'SDTau_pt', 'SDTau_mass', 
                 'DiTau_pt', 'DiTau_eta', 'DiTau_phi', 'DiTau_mass', 'DiTau_dR', 
                 'DiJet_dR', 'DiJet_mass', 'DiJet_pt', 'DiJet_eta', 'DiJet_phi',
                 'MET_pt', 'MET_phi']

def smooth_labels(y, eps=0.05):
    """Smoothing of binary labels. eps belonging to [0.01, 0.1] is typical."""
    return y * (1 - eps) + 0.5 * eps

# -----------------------
# Define NN classifier
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
    def forward(self, x): 
        return self.net(x)


# ----------- Training routine
# -----------------------
def train_model(model, dataset, n_epochs=80, batch_size=1024, lr=1e-3):
    """Train a given PyTorch model with BCEWithLogitsLoss and weights"""
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=1e-5)

    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode='min', factor=0.5, patience=5, verbose=True)
    criterion = nn.BCEWithLogitsLoss(reduction="none")

    # Split into train/validation
    train_size = int(0.8 * len(dataset))
    val_size = len(dataset) - train_size
    train_dataset, val_dataset = torch.utils.data.random_split(dataset, [train_size, val_size])
    
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    val_loader   = DataLoader(val_dataset,   batch_size=batch_size, shuffle=False)
    
    for epoch in range(n_epochs):
        # Training
        model.train()
        total_train_loss = 0.0
        for batch_X, batch_y, batch_w in train_loader:
            optimizer.zero_grad()
            logits = model(batch_X)
            batch_y_smooth = smooth_labels(batch_y)
            loss = criterion(logits, batch_y_smooth)
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

        avg_train_loss = total_train_loss / len(train_dataset)
        avg_val_loss   = total_val_loss / len(val_dataset)
        scheduler.step(avg_val_loss)
        
        if epoch % 10 == 0:
            logging.info(f"Epoch {epoch+1}/{n_epochs}, Train Loss: {avg_train_loss:.4f}, Val Loss: {avg_val_loss:.4f}, LR: {optimizer.param_groups[0]['lr']:.6f}")

    return model


# -----------------------
# Main wrapper
# -----------------------
def train_and_reweight(data_df, mc_df, features, n_epochs=80, batch_size=1024, lr=1e-3):
    """
    Train a NN classifier to separate Data vs MC, 
    then compute reweighted event weights for Reco p3 and Reco p2.
    """

    # 0) Prepare inputs
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

    # 1) Train the model
    model = SimpleNN(len(features))
    model = train_model(model, dataset, n_epochs=n_epochs, batch_size=batch_size, lr=lr)

    # 2) Prediction on DATA only
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
    
def train_and_reweight_ss_os(ss_df, os_df, features, n_epochs=80, batch_size=1024, lr=1e-3):
    """
    Train a NN classifier to separate SS vs OS events,
    then compute reweighted OS weights for the SS sample.
    
    Args:
        ss_df (pd.DataFrame): Same-sign events (label 0)
        os_df (pd.DataFrame): Opposite-sign events (label 1)
    
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
    # 2) Prediction on SS only
    # -----------------------
    X_ss_tensor = torch.from_numpy(p1)
    with torch.no_grad():
        s_data = torch.sigmoid(model(X_ss_tensor)).numpy().ravel()

    eps = 1e-6
    r_data = s_data / (1.0 - s_data + eps)   # local ratio OS/SS

    logging.info(f"Max probability for OS: {s_data.max():.4f}")
    logging.info(f"Min probability for OS: {s_data.min():.4f}")
    logging.info(f"Max ratio of OS/SS: {r_data.max():.4f}")
    logging.info(f"Min ratio of OS/SS: {r_data.min():.4f}")

    # -----------------------
    # 3) Compute OS weights for SS events
    # -----------------------
    w_data_base = ss_df["weight"].to_numpy() if "weight" in ss_df.columns else np.ones(len(p1), dtype=np.float32)
    w_data_reco_OS = r_data * w_data_base

    # Normalize to match OS total weight
    target_sum = os_df["weight"].sum()
    norm_factor = target_sum / (w_data_reco_OS.sum() + 1e-12)
    w_data_reco_OS *= norm_factor

    return {
        "model": model,
        "r_data": r_data,
        "w_data_reco_os": w_data_reco_OS
    }

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
    else:
        raise ValueError(f"Unknown mode {args['mode']}. Supported modes: dataMinusMC, SStoOS")

    

