import pandas as pd
from sqlalchemy import create_engine
import os

# Crée un dossier si nécessaire
export_path = '/home/salma_eddib'
os.makedirs(export_path, exist_ok=True)

# Connexion à PostgreSQL
engine = create_engine('postgresql://airflow_user:airflow_pass@localhost:5432/airflow_db')

# Export des avis enrichis
df_sentiment = pd.read_sql("SELECT * FROM fct_sentiment_analysis", engine)
df_sentiment.to_csv(f'{export_path}/fct_sentiment_analysis.csv', index=False)

# Export des sujets LDA
df_topics = pd.read_sql("SELECT * FROM review_topics", engine)
df_topics.to_csv(f'{export_path}/review_topics.csv', index=False)

# Export des fact LDA
df_fact = pd.read_sql("SELECT * FROM fact_reviews", engine)
df_fact.to_csv(f'{export_path}/fact_reviews.csv', index=False)

# Export des bank LDA
df_bank = pd.read_sql("SELECT * FROM dim_bank", engine)
df_bank.to_csv(f'{export_path}/dim_bank.csv', index=False)

# Export des branche LDA
df_branch = pd.read_sql("SELECT * FROM dim_branch", engine)
df_branch.to_csv(f'{export_path}/dim_branch.csv', index=False)

# Export des location LDA
df_location = pd.read_sql("SELECT * FROM dim_location", engine)
df_location.to_csv(f'{export_path}/dim_location.csv', index=False)

# Export des sentiment LDA
df_sentiment = pd.read_sql("SELECT * FROM dim_sentiment", engine)
df_sentiment.to_csv(f'{export_path}/dim_sentiment.csv', index=False)



print("Export CSV terminé avec succès.")
