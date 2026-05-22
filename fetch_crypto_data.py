import requests
import pandas as pd 
from sqlalchemy import create_engine
from datetime import datetime

#Binance API endpoints
url="https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"

#fetch data
response = requests.get(url)

#convert Json response
data=response.json()
print("fetched data:")
print(data)

#create dataframe
df =pd.dataFrame([{
    "symbol":data["symbol"],
    "price":float(data["price"]),
    "timestamp": datetime.now()
}])
print(df)

#Postgresql connection
engine = create_engine("postgresql+psycopg2://postgres:12345@localhost:5432/crypto_pipeline")

#store into PostgreSQL
df.to_sql(
    "prices",
    engine,
    if_exists="append",
    index =False
)
print("Data inserted into postgresql")
