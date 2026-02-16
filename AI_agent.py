from google import genai
from google.genai import types
import pandas as pd
import os
import json
from dotenv import load_dotenv


load_dotenv()

gemini_token = os.getenv("GEMINI_TOKEN")

client = genai.Client(api_key=gemini_token)

def rate_car_model(marketplace_listing_title,brand, year, vehicle_fuel_type):
    prompt = f"""
    Act as an Australian automotive market expert. 
    Research and rate the car specified below using Australian-market 
    specifications and data (e.g., RedBook, Carsales, and the Green Vehicle Guide).

    CAR IDENTITY:
    marketplace_listing_title: {marketplace_listing_title}
    Brand: {brand}
    Year: {year}
    Fuel Type: {vehicle_fuel_type}

    TASK:
    
    1. Conduct a search for this specific model variant as sold in Australia.
    
    Features:

    Performance_metrics - Rate from 1 to 5 (1=poor, 5 = Excellent) in terms of
    horsepower, engine size, 0-60 time. It can be float.

    luxury - Rate from 1 to 5 (1=poor, 5 = Excellent) in terms of
    brand positioning and trim level. Consider interior quality, features, and market segment. 
    Trim keywords: Base (1-2), Mid/Sport (3), Premium/Elite (4-5). It can be float.

    body_style - Choose ONE: Hatchback, Sedan, Wagon, Liftback, Fastback, Coupe, Convertible,
    SUV, Compact SUV, Medium SUV, Large SUV, Crossover, UTE, Dual-Cab UTE, Tray back, 
    Van, Cargo Van, Passenger van, People mover, Minivan, Limousine, Truck, Bus, Minibus.

    size - size: Rate from 1 to 5 (1=poor, 5 = Excellent) in terms of vehicle size 
    within its category:
    1=subcompact, 2=compact, 3=midsize, 4=large, 5=extra-large
    Examples: Yaris=1, Corolla=2, Camry=3, LandCruiser=4

    drivetrain - AWD, FWD, RWD or 4WD. 

    vehicle_model_display_name - From the marketing_listing_title, keep only the name of the model in upper cases and drop the rest of the strings. In other words, 
    clean the strings to get the proper model name. If you are not sure how to proceed simply leave the space blank.
    For example: 'Ford Falcon FG II G6E Ecolpi - Black', return 'FALCON',
    'Mitsubishi Mitsubishi Express' return 'EXPRESS'
    '2016 Mazda Mazda3', return '3',
    where the model name has more than one string, replace the space character for dash. For example,
    '2015 TOYOTA LAND CRUISER', return 'LAND-CRUISER'.

    2. Return ONLY valid JSON. Do not include introductory text or markdown code blocks.

    REQUIRED JSON STRUCTURE AND CONSTRAINTS:
    {{
      "performance_metrics": 1-5,
      "luxury": 1-5,
      "size": 1-5,
      "body_style": "sedan/SUV/truck/etc",
      "drivetrain": "FWD/RWD/AWD/4WD"
      "vehicle_model_display_name": the proper model name
    }}

    3. In case the info given don't mention nothing about the actual matter, leave your outputs blank.
    """
    
    response = client.models.generate_content(
        model = 'gemini-2.5-flash-lite',
        contents = prompt,
        config = {
            "response_mime_type": "application/json"
            
        }
        )
    return json.loads(response.text)



print("Loading dataset...")
df = pd.read_csv("data/cleaned_car_data.csv")
print(f"Total rows in dataset: {len(df)}")


# Create car_id for deduplication
df['car_id'] = df['marketplace_listing_title']

# Step 2: Extract unique combinations
unique_cars = df.drop_duplicates(subset='marketplace_listing_title', keep='first').copy()
print(f"Unique cars to rate: {len(unique_cars)}")
print(f"Estimated API calls: {len(unique_cars)} (instead of {len(df)})")
print("\nStarting Gemini API calls on unique cars only...\n")

# Step 3: Rate only unique cars
ratings_dict = {}  # Store results keyed by car_id

for idx, row in unique_cars.iterrows():
    try:
        ratings = rate_car_model(
            row['marketplace_listing_title'], 
            row['vehicle_make_display_name'], 
            row.get('year'), 
             
            row.get('vehicle_fuel_type')
        )
        
        # Store in dictionary with car_id as key
        ratings_dict[row['car_id']] = ratings
        
        print(f"[{len(ratings_dict)}/{len(unique_cars)}] Rated: {row['year']} {row['marketplace_listing_title']}")
        
    except Exception as e:
        print(f"ERROR with {row['marketplace_listing_title']}: {e}")
        # Store None for failed ratings so we can identify them later
        ratings_dict[row['car_id']] = None

print("\n" + "="*60)
print(f"Gemini API calls completed: {len(ratings_dict)} unique cars rated")
print("="*60 + "\n")

# Step 4: Map ratings back to ALL rows in original dataframe
print("Mapping ratings back to all rows...")

# Initialize new columns
df['performance_metrics'] = None
df['luxury'] = None
df['size'] = None
df['body_style'] = None
df['drivetrain'] = None
df['vehicle_model_display_name'] = None

# Apply ratings to all rows based on car_id lookup
for idx, row in df.iterrows():
    car_id = row['car_id']
    
    if car_id in ratings_dict and ratings_dict[car_id] is not None:
        ratings = ratings_dict[car_id]
        df.at[idx, 'performance_metrics'] = ratings.get('performance_metrics')
        df.at[idx, 'luxury'] = ratings.get('luxury')
        df.at[idx, 'size'] = ratings.get('size')
        df.at[idx, 'body_style'] = ratings.get('body_style')
        df.at[idx, 'drivetrain'] = ratings.get('drivetrain')
        df.at[idx, 'vehicle_model_display_name'] = ratings.get('vehicle_model_display_name')

# Step 5: Drop the temporary car_id column (you don't need it anymore)
df.drop(columns=['car_id'], inplace=True)

# Step 6: Save enriched dataset
output_path = 'data/cleaned_dataset_LLM.csv'
df.to_csv(output_path, index=False)

print(f"\n Success! Enriched dataset saved to: {output_path}")
print(f"Total rows processed: {len(df)}")
print(f"Rows with Gemini ratings: {df['performance_metrics'].notna().sum()}")
print(f"Rows with missing ratings: {df['performance_metrics'].isna().sum()}")



