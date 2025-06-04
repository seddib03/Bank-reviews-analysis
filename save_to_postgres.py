
import json
import psycopg2

def save_data_postgres():
    # Charger les données depuis le fichier JSON
    with open('/home/salma_eddib/airflow/dags/full_reviews.json', 'r') as f:
        reviews = json.load(f)

    # Connexion à PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        dbname="airflow_db",
        user="airflow_user",
        password="airflow_pass"  # Remplace par ton mot de passe réel
    )
    cur = conn.cursor()

    # Création de la table staging si elle n'existe pas
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stg_customer_reviews (
            id SERIAL PRIMARY KEY,
            bank TEXT,
            city TEXT,
            branch TEXT,
            location TEXT,
            text TEXT,
            rating INTEGER,
            date TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Insertion des données dans la table
    for review in reviews:
        bank = review.get("bank")
        city = review.get("city")
        branch = review.get("branch")
        location = review.get("location")
        text = review.get("text")
        rating = review.get("rating")
        date = review.get("date")  # Assure-toi que cette clé existe dans ton JSON

        cur.execute("""
            INSERT INTO stg_customer_reviews (bank, city, branch, location, text, rating, date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (bank, city, branch, location, text, rating, date))

    # Commit & fermer
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Données insérées avec succès dans stg_customer_reviews.")

if __name__ == "__main__":
    save_data_postgres()
