# Car Price Prediction Project (Web Scraping → ML → Streamlit)

## Project Overview

![Pipeline Architecture](images/Readme-Workflow.png)


This project focuses on building a **real-world car price prediction system** using **real market data** scraped from Facebook Marketplace in Australia.

The goal is to: <br/>
- Collect **real car prices** from online sources.  
- Analyse and clean the data.
- Build a **machine learning model** to predict car prices.  
- Deploy the final solution as an **interactive Streamlit app**. <br/>

This project follows an **end-to-end data science workflow**, from data collection to deployment.

---

## Project Objectives

- Scrape car listings data from Facebook Marketplace  
- Perform **Exploratory Data Analysis (EDA)**  
- Apply data cleaning and preprocessing using AI agents to reduce the process ensuring efficiency.
- Perform **feature engineering**  
- Train and evaluate multiple **machine learning models**
- Select the best-performing model  
- Deploy the model using **Streamlit**

---

## Workflow
1. **Web Scraping**
- Extract car prices and features such as:
   - Brand
   - Model
   - Year
   - Mileage
   - Fuel type
   - Transmission
   - Location
   - Price
   - Color

2. **Data Cleaning & Preprocessing**
   - Handle missing values  
   - Remove duplicates
   - Cleaning model cars variations.  
   - Normalise and format variables  

3. **Exploratory Data Analysis (EDA)**
   - Price distribution analysis  
   - Feature correlations  
   - Market trends and insights  

4. **Feature Engineering**
   - Encoding categorical variables  
   - Creating new meaningful features  
   - Feature selection  

5. **Machine Learning**
   - Train multiple models (e.g. Linear Regression, Random Forest, XGBoost, LGBoost)  
   - Model evaluation using appropriate metrics  
   - Hyperparameter tuning  

6. **Deployment**
   - Build an interactive **Streamlit app**  
   - Users can input car features and get a predicted price  

---

## Tech Stack
- **Python**
- **Apify** (Web Scraping)
- **Pandas, NumPy** (Data manipulation)
- **Matplotlib, Seaborn** (EDA & Visualization)
- **Scikit-learn, scipy, category_encoders** (Machine Learning)
- **Streamlit** (Deployment)

---

## 📊 Expected Output
- Clean and structured dataset with real car prices  
- Insights into car market pricing trends  
- A trained ML model capable of predicting
- Stream link [here](https://ml-australian-car-market.streamlit.app/)


