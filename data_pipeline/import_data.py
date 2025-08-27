import os
import glob
import pandas as pd
import sqlalchemy
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

# Force load .env from repo root
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")

# Debug print
print("DEBUG: DB_HOST =", DB_HOST)
print("DEBUG: DB_PORT =", DB_PORT)

# Auto-detect CSV inside /data
csv_files = glob.glob("data/*.csv")
if not csv_files:
    raise FileNotFoundError(
        "❌ No CSV found in data/. Please download from Kaggle and place in data/ folder."
    )

csv_file = next((f for f in csv_files if "Fraudulent_E-Commerce_Transaction_Data.csv" in f), csv_files[0])
print(f"📂 Using dataset: {csv_file}")

# Load CSV
df = pd.read_csv(csv_file)
print("✅ Data Loaded! Shape:", df.shape)
print(df.head())

# Clean column names
print("🧹 Cleaning column names...")
df.columns = (
    df.columns
      .str.strip()
      .str.lower()
      .str.replace(" ", "_")
      .str.replace(r"[()]", "", regex=True)
)

# Build PostgreSQL URL
if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS]):
    raise ValueError("❌ One or more DB environment variables are missing!")

db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{int(DB_PORT)}/{DB_NAME}?sslmode={DB_SSLMODE}"

# Function to log which DB was used
def log_db_usage(db_type):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_line = f"[{timestamp}] Data imported using {db_type}\n"
    print(log_line.strip())
    with open("data_import_log.txt", "a") as f:
        f.write(log_line)

# Try PostgreSQL first, fallback to SQLite
try:
    print("⬆️ Importing data into PostgreSQL 'transactions' table...")
    engine = sqlalchemy.create_engine(db_url)
    df.to_sql("transactions", engine, if_exists="replace", index=False)
    log_db_usage("PostgreSQL")
except OperationalError as e:
    print("⚠️ PostgreSQL connection failed:", e)
    print("🟢 Falling back to local SQLite database...")
    engine = sqlalchemy.create_engine("sqlite:///local_test.db")
    df.to_sql("transactions", engine, if_exists="replace", index=False)
    log_db_usage("SQLite")
