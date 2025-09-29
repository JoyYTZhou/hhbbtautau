from src.utils.ioutil import setup_logging
import logging, os
import pandas as pd
import json
from src.utils.displayutil import RichArgumentParser
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import TensorDataset, DataLoader
from src.utils.inferutil import infer_ss_to_os, infer_data_to_mc, infer_multiclass
from src.utils.statsutil import normalize_mc

pjoin = os.path.join

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
    def __init__(self, d, p_dropout=0.3, num_classes=1):  # Add num_classes parameter
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(d, 64),
            nn.ReLU(),
            nn.Dropout(p_dropout),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Dropout(p_dropout),
            nn.Linear(32, 16),
            nn.ReLU(),
            nn.Dropout(p_dropout),
            nn.Linear(16, num_classes)   # no Sigmoid
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

def train_multiclass_model(model, dataset, n_epochs=80, batch_size=1024, lr=1e-3):
    """Train a multi-class PyTorch model with CrossEntropyLoss and optional class weights"""
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=1e-5)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode='min', factor=0.5, patience=5, verbose=True)
    
    # For multi-class classification, use CrossEntropyLoss
    # If you have class imbalance, you can pass class weights here
    criterion = nn.CrossEntropyLoss(reduction="none")
    
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
        correct_train = 0
        total_train = 0
        
        for batch_X, batch_y, batch_w in train_loader:
            optimizer.zero_grad()
            logits = model(batch_X)  # Shape: (batch_size, num_classes)
            
            # For multi-class, batch_y should be class indices (not one-hot)
            # If batch_y is one-hot, convert it: batch_y = batch_y.argmax(dim=1)
            if len(batch_y.shape) > 1 and batch_y.shape[1] > 1:
                batch_y = batch_y.argmax(dim=1)
            
            loss = criterion(logits, batch_y)  # CrossEntropyLoss expects class indices
            loss = (loss * batch_w).mean()     # Apply sample weights
            
            loss.backward()
            optimizer.step()
            
            total_train_loss += loss.item() * batch_X.size(0)
            
            # Calculate accuracy
            _, predicted = torch.max(logits.data, 1)
            total_train += batch_y.size(0)
            correct_train += (predicted == batch_y).sum().item()
        
        # Validation
        model.eval()
        total_val_loss = 0.0
        correct_val = 0
        total_val = 0
        
        with torch.no_grad():
            for batch_X, batch_y, batch_w in val_loader:
                logits = model(batch_X)
                
                # Convert one-hot to class indices if needed
                if len(batch_y.shape) > 1 and batch_y.shape[1] > 1:
                    batch_y = batch_y.argmax(dim=1)
                
                loss = criterion(logits, batch_y)
                loss = (loss * batch_w).mean()
                total_val_loss += loss.item() * batch_X.size(0)
                
                # Calculate accuracy
                _, predicted = torch.max(logits.data, 1)
                total_val += batch_y.size(0)
                correct_val += (predicted == batch_y).sum().item()
        
        avg_train_loss = total_train_loss / len(train_dataset)
        avg_val_loss   = total_val_loss / len(val_dataset)
        train_acc = 100 * correct_train / total_train
        val_acc = 100 * correct_val / total_val
        
        scheduler.step(avg_val_loss)
        
        if epoch % 10 == 0:
            logging.info(f"Epoch {epoch+1}/{n_epochs}, Train Loss: {avg_train_loss:.4f}, Val Loss: {avg_val_loss:.4f}, "
                        f"Train Acc: {train_acc:.2f}%, Val Acc: {val_acc:.2f}%, LR: {optimizer.param_groups[0]['lr']:.6f}")
    
    return model

def train_and_reweight_multiclass(data_df_0, mc_df, data_df_1, features, n_epochs=80, batch_size=1024, lr=1e-3):
    """
    Train a NN classifier for multi-class classification (Data vs MC1 vs MC2), 
    then compute reweighted event weights.
    """
    # 0) Prepare inputs for 3-class problem
    p0 = data_df_0[features].to_numpy().astype(np.float32)  
    p1 = mc_df[features].to_numpy().astype(np.float32)    # MC type 1
    p2 = data_df_1[features].to_numpy().astype(np.float32)    # Data type 2

    X = np.vstack([p0, p1, p2])
    y = np.hstack([
        np.zeros(len(p0), dtype=np.int64),  
        np.ones(len(p1), dtype=np.int64),     # MC  = class 1
        np.full(len(p2), 2, dtype=np.int64)  # Data type 2 = class 2
    ])
    
    w = np.hstack([
        np.ones(len(p0), dtype=np.float32),                       
        mc_df['weight'].to_numpy().astype(np.float32),               # MC1 weights
        np.ones(len(p2), dtype=np.float32)                            # Data type 2 weights
    ])
    
    # Shuffle
    perm = np.random.permutation(len(X))
    X, y, w = X[perm], y[perm], w[perm]
    
    # Torch tensors - NOTE: key changes here
    X_tensor = torch.tensor(X, dtype=torch.float32)
    y_tensor = torch.tensor(y, dtype=torch.long)        # Must be long for CrossEntropy, NO unsqueeze
    w_tensor = torch.tensor(w, dtype=torch.float32)     # NO unsqueeze for weights either
    
    dataset = TensorDataset(X_tensor, y_tensor, w_tensor)
    
    # 1) Train the model - specify num_classes
    num_classes = 3
    model = SimpleNN(len(features), num_classes=num_classes)  # Updated constructor
    model = train_multiclass_model(model, dataset, n_epochs=n_epochs, batch_size=batch_size, lr=lr)
    # 2) Prediction on DATA only
    results_dict = infer_multiclass(model, data_df_0, features)
    results_dict["model"] = model

    return results_dict

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
    results_dict = infer_data_to_mc(model, data_df, mc_df, features)
    results_dict["model"] = model
    
    return results_dict
    
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

def SSDataToOSQCD(ss_df, os_df, out_dir, training_args, session_name=''):
    ss_data = ss_df[ss_df['group'] == 'Data'].copy()
    os_data = os_df[os_df['group'] == 'Data'].copy()
    os_mc = os_df[os_df['group'] != 'Data'].copy()
    logging.info(f"Number of SS Data events: {len(ss_data)}")
    logging.info(f"Number of OS MC events: {os_mc['weight'].sum()}")
    logging.info(f"Number of OS Data events: {len(os_data)}")
    os_mc, renorm_fac = normalize_mc(os_data, os_mc, feature='DiTau_mass')

    results_dict = train_and_reweight_multiclass(ss_data, os_mc, os_data, features_train, **training_args)

    model = results_dict['model']
    w_reco_qcd = results_dict['w_reco_qcd']

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
        os_df = pd.read_csv(args['os_input_csv'])
        if args['inference_only']:
            model_path = pjoin(args['output_dir'], f"{args['session_name']}_ss_to_os_qcd_model.pth")
            model = load_model(model_path, input_dim=len(features_train), num_classes=3)
            results_dict = infer_multiclass(model, ss_df[ss_df['group'] == 'Data'], features_train)
            w_reco_qcd = results_dict['w_reco_qcd']
            ss_df.loc[ss_df['group'] == 'Data', 'weight_reco_os_fakes'] = w_reco_qcd
            new_ss_name = f"{args['session_name']}_ss_data_qcd.csv"
            ss_df.to_csv(pjoin(args['output_dir'], new_ss_name), index=False)
            logging.info(f"SS Data with new OS Fake weights saved to {pjoin(args['output_dir'], new_ss_name)}")
        else:
            SSDataToOSQCD(ss_df, os_df, args['output_dir'], training_args=args['training_args'], session_name=args['session_name'])
    else:
        raise ValueError(f"Unknown mode {args['mode']}. Supported modes: dataMinusMC, SStoOS, SStoFakes")

    

