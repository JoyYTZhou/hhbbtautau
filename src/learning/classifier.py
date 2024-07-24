from sklearn.model_selection import train_test_split, GridSearchCV
import pandas as pd
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.utils.class_weight import compute_class_weight
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
from xgboost import XGBClassifier

class DataLoader():
    def __init__(self, file_path, target_column):
        self.target_column = target_column
        self.data = pd.read_csv(file_path)
        self.X = None
        self.y = None
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.scaler = StandardScaler()
       
    def preprocess_data(self, dropped: 'list[str]' = []):
        self.y = self.data[self.target_column]
        self.X = self.data.drop(columns=[self.target_column]+dropped)
        self.X = self.scaler.fit_transform(self.X)
        
    def split_data(self, test_size=0.3, random_state=None):
        self.X_train, self.X_test, self.y_train, self.y_test = \
            train_test_split(self.X, self.y, test_size=test_size, random_state=random_state)
            
    def get_train_data(self, if_torch=True):
        if if_torch: 
            return torch.tensor(self.X_train.to_numpy(), dtype=torch.float32), torch.tensor(self.y_train.to_numpy(), dtype=torch.long)
        else:
            return self.X_train, self.y_train
    
    def get_test_data(self, if_torch=False):
        if if_torch:
            return torch.tensor(self.X_test, dtype=torch.float32), torch.tensor(self.y_test, dtype=torch.long)
        else:
            return self.X_test, self.y_test

class SimpleClassifier(nn.Module):
    def __init__(self, hidden_size=128, num_classes=2):
        super().__init__()
        self.model = nn.Sequential(
            nn.LazyLinear(hidden_size),
            nn.ReLU(),
            nn.LazyLinear(num_classes)
        )
        self.criterion = nn.CrossEntropyLoss()
        self.optimizer = optim.Adam(self.model.parameters(), lr=0.001)
        self.train_loader = None
    
    def fit(self, X_train, y_train, batch_size=32, epochs=50):
        X_train_tensor = X_train
        y_train_tensor = y_train 

        train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
        self.train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
        
        for epoch in range(epochs):
            for inputs, labels in self.train_loader:
                self.optimizer.zero_grad()
                outputs = self.model(inputs)
                loss = self.criterion(outputs, labels)
                loss.backward()
                self.optimizer.step()

            print(f'Epoch [{epoch+1}/{epochs}], Loss: {loss.item():.4f}')

        print('Training finished!')
    
    def forward(self, x):
        return self.model(x)
    
    def predict(self, X_test):
        X_test_tensor = torch.tensor(X_test, dtype=torch.float32)
        
        with torch.no_grad():
            outputs = self.model(X_test_tensor)
            _, predicted = torch.max(outputs, 1)
        
        return predicted.numpy()
    
    def evaluate(self, X_test, y_test):
        predicted = self.predict(X_test)
        accuracy = (predicted == y_test).mean()
        print(f'Test Accuracy: {accuracy:.4f}')

def est_bkg_RegA(df, cri1, cri2, weight_column):
    """
    Estimate the background in region A using the ABCD method.

    Parameters:
    - df: pandas DataFrame containing the data.
    - cri1: String that defines the first criteria for splitting the data.
    - cri2: String that defines the second criteria for splitting the data.

    Returns:
    - Estimated background in region A.
    """
    region_A = df.query(cri1 + " and " + cri2)
    region_B = df.query(cri1 + " and not (" + cri2 + ")")
    region_C = df.query("not (" + cri1 + ") and " + cri2)
    region_D = df.query("not (" + cri1 + ") and not (" + cri2 + ")")
    
    N_A = region_A[weight_column].sum()
    N_B = region_B[weight_column].sum()
    N_C = region_C[weight_column].sum()
    N_D = region_D[weight_column].sum()
    
    if N_D == 0:
        raise Warning("No events in region D. Cannot estimate background for region A.")
        return None
    N_A_background = (N_B * N_C) / N_D
    
    return N_A_background

def binaryBDTReweighter(X_train, y_train, X_test):
    class_weight = (len(y_train) - y_train.sum()) / y_train.sum()
    param_grid = {
        'max_depth': [3, 4, 5],
        'learning_rate': [0.1, 0.01, 0.05],
        'n_estimators': [50, 100, 200],
        'subsample': [0.8, 1.0],
        'colsample_bytree': [0.8, 1.0]
    }
    xgb_clf = XGBClassifier(objective='binary:logistic', random_state=42, n_jobs=1, scale_pos_weight=class_weight)
    grid_search = GridSearchCV(xgb_clf, param_grid, scoring='roc_auc', cv=5, n_jobs=-1)
    grid_search.fit(X_train, y_train)

    labels = xgb_clf.predict(X_test)

    probabilities = xgb_clf.predict_proba(X_test)
    