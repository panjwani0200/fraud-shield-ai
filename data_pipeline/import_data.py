import pandas as pd
import sqlalchemy
import os

csv_file = "data/ecommerce_fraud.csv"

if not os.path.exists(csv_file):
    raise FileNotFoundError(f"❌ CSV file not found at {csv_file}. "
                            f"Please download from Kaggle and put it in the data/ folder.")

df = pd.read_csv(csv_file)
print("✅ Data Loaded! Shape:", df.shape)
print(df.head())

db_url = import os
db_url = os.getenv("DB_URL")
engine = sqlalchemy.create_engine(db_url)

df.to_sql("transactions", engine, if_exists="replace", index=False)

print("🎉 Data successfully imported into database (table: transactions)!")
