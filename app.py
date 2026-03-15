import streamlit as st
import pandas as pd
import numpy as np
import joblib
from scipy.sparse import hstack, csr_matrix
import preprocessor as prep

df = pd.read_csv("data/cleaned_car_dataset_final.csv")

st.set_page_config(layout="wide", page_title="Australian Car Price Prediction")

st.title('🚗 Australian Car Price Prediction')
st.markdown('### This interactive app helps you analyze the Australian car market. ' \
'You can filter and explore car prices, specifications, and more.')


st.header('Car Details Input')
brand_array = sorted(df['vehicle_make_display_name'].unique())
brand= st.selectbox('Brand 👨🏻‍🔧',brand_array) #Brand input

models_array = np.sort(df[df['vehicle_make_display_name'] == brand]['vehicle_model_display_name'].unique())
model = st.selectbox('Model 🛠️',sorted(models_array)) #Model input

fuel_types_array = sorted(df['vehicle_fuel_type'].unique())
fuel_type = st.selectbox('Fuel type ⛽',sorted(fuel_types_array),index=fuel_types_array.index('PETROL')) #Fueltype input
        
state_array = sorted(df['state'].unique())
state = st.selectbox('State 📌',state_array,index=state_array.index('QLD')) #State input

transmission = st.radio('Select transmission: ',['Automatic','Manual']) #Transmission input
year = st.number_input('Year ⌛', min_value=df['year'].min(), max_value=df['year'].max(), value=2018) #Year input
kilometers = st.number_input('Mileage (kms)', min_value=0, max_value=350000, value= 100000,step = 20000) #KMS input

color_array = sorted(df['vehicle_exterior_color'].unique()) 
color = st.selectbox('Color ',color_array,index = color_array.index('white')) #Color input


clicked = st.button("Submit",type = "primary")
st.markdown(
    """
    ---
 
    """
)

#Load the model
loaded_model = joblib.load('best_model.joblib')

def predicting(input_vector):
    input_df = pd.DataFrame(input_vector,columns =
                            ['state', 'vehicle_transmission_type','vehicle_fuel_type','drivetrain',
                             'vehicle_make_display_name', 'vehicle_model_display_name',
                             'vehicle_exterior_color', 'body_style',
                             'year', 'kms', 'performance_metrics','luxury', 'size'])
    transformed_input_vector = prep.preprocessor.transform(input_df)
    y_pred_output = loaded_model.predict(transformed_input_vector)
    return y_pred_output

if clicked:
    drivetrain = df[df['vehicle_model_display_name']== model]['drivetrain'].mode()[0]
    body_style = df[df['vehicle_model_display_name']== model]['body_style'].mode()[0]
    performance_metrics = df[df['vehicle_model_display_name']== model]['performance_metrics'].mean().round(2)
    luxury = df[df['vehicle_model_display_name']== model]['luxury'].mean().round(2)
    size = df[df['vehicle_model_display_name']== model]['size'].mean().round(2)
    input_vector = [[state,transmission,fuel_type,drivetrain,brand,model,color,body_style,year,kilometers,
                     performance_metrics,luxury,size]]
    
    predicted_price = np.exp(predicting(input_vector))[0]
    
    st.markdown(f"""
    <div style='text-align: center; padding: 30px; background-color: #f0f2f6; border-radius: 10px;'>
        <p style='font-size: 18px; color: gray; margin: 0;'>Estimated Vehicle Price</p>
        <p style='font-size: 48px; font-weight: bold; color: #1f77b4; margin: 0;'>${predicted_price:,.0f}</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(
    """
    ---
 
    """
    )

