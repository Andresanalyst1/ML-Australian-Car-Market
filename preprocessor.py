import numpy as np
import pandas as pd
from sklearn.preprocessing import OneHotEncoder, TargetEncoder
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, MinMaxScaler, OrdinalEncoder
from sklearn.impute import SimpleImputer
import category_encoders as ce 

df = pd.read_csv("data/cleaned_car_dataset_final.csv")

#Cleaning 'quattro'
df.loc[df['drivetrain'] == 'quattro', 'drivetrain'] = '4WD'

# Target Log Transform due to the righ-skewed distribution in the target variable
df["log_price"] = np.log1p(df["price"])

#Since 'marketplace_listing_title' is only the merge of diff columns, keeping it is redundant.
#The price column is not necessary anymore since the log_price is already calculated.
#Other columns like 'vehicle_model_display_name_0' and 'description' are no longer necessary
cols_to_remove = ["marketplace_listing_title","price","vehicle_model_display_name_0",'description']
df.drop(cols_to_remove,axis=1,inplace=True)

#Splitting data in Features and Target
X_features = df.drop('log_price', axis=1)
Y = df['log_price']

#Splitting in training and test sets
x_train_raw,x_test_raw,y_train,y_test = train_test_split(X_features,Y,train_size=.8,random_state=42)


# 1. Define column groups
cat_ohe_cols = ['state', 'vehicle_transmission_type','vehicle_fuel_type','drivetrain']   
cat_target_cols = ['vehicle_make_display_name', 'vehicle_model_display_name',
                   'vehicle_exterior_color', 'body_style'] 
num_cols = ['year', 'kms','size', 'performance_metrics','luxury']

# 2. Define pipelines
# Pipeline for numerical: Impute missing -> Scale
num_pipe = Pipeline([
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', MinMaxScaler())
])

# Pipeline for OneHot: Impute -> Encode
ohe_pipe = Pipeline([
    ('imputer', SimpleImputer(strategy='most_frequent')),
    ('encoder', OneHotEncoder(handle_unknown='ignore', drop='first'))
])

# Pipeline for Target Encoding: Impute -> Encode
# TargetEncoder handles new categories in test data automatically
target_pipe = Pipeline([
    ('imputer', SimpleImputer(strategy='most_frequent')),
    ('encoder', ce.TargetEncoder()),
    ('scaler',MinMaxScaler()) 
])

# 3. Combine into a preprocessor
preprocessor = ColumnTransformer(transformers=[
    ('num', num_pipe, num_cols),
    ('ohe', ohe_pipe, cat_ohe_cols),
    ('target', target_pipe, cat_target_cols)
], remainder='drop')

# 4. Apply

# Fit only on TRAIN, transform TRAIN AND TEST
x_train = preprocessor.fit_transform(x_train_raw, y_train)
x_test = preprocessor.transform(x_test_raw)

print("Pipeline complete. Shapes:", x_train.shape, x_test.shape)

feature_names = preprocessor.get_feature_names_out()

target_mapping = {
    f'target__{i}': f'target__{col}' 
    for i, col in enumerate(cat_target_cols)
}

# Apply mapping
feature_names_readable = [
    target_mapping.get(name, name) for name in feature_names
]
