import time
import json
import os
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# Connexion à la base de données (pour plus tard)
DB_URI = "postgresql://airflow_user:airflow_pass@localhost:5432/airflow_db"

# Liste des banques et villes à scraper
BANKS = ["Attijariwafa Bank", "CIH"]
CITIES = ["Rabat", "Salé"]

# Fichier JSON pour stocker les avis
RAW_DATA_FILE = "/home/salma_eddib/airflow/dags/full_reviews.json"

# Vérifier si le fichier JSON existe et le charger
def load_existing_data():
    """Charge les avis existants depuis le fichier JSON."""
    if os.path.exists(RAW_DATA_FILE) and os.path.getsize(RAW_DATA_FILE) > 0:
        try:
            with open(RAW_DATA_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print("⚠️ Fichier JSON corrompu. Réinitialisation...")
            return []
    return []

# Enregistrer les avis dans le fichier JSON
def save_data(new_reviews):
    """Enregistre les nouveaux avis sans écraser les anciens."""
    existing_reviews = load_existing_data()
    all_reviews = existing_reviews + new_reviews

    try:
        with open(RAW_DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(all_reviews, f, indent=4, ensure_ascii=False)
        print(f"✅ Données enregistrées dans {RAW_DATA_FILE}")
    except Exception as e:
        print(f"❌ Erreur lors de l'enregistrement du fichier JSON : {e}")

# Fonction principale pour scraper les avis
def scrape_reviews(bank_name, city):
    """Scrape les avis Google Maps pour une banque et une ville donnée."""
    print(f"🚀 Démarrage du scraping pour {bank_name} à {city}...")

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True, slow_mo=300)
            page = browser.new_page()

            search_url = f"https://www.google.com/maps/search/{bank_name}+{city}+Maroc"
            print(f"🔍 Accès à {search_url}")
            page.goto(search_url)
            time.sleep(5)

            if not page.locator('a.hfpxzc').count():
                print(f"❌ Aucune agence trouvée pour {bank_name} à {city}")
                browser.close()
                return

            branches = page.locator('a.hfpxzc').all()
            print(f"🏦 {len(branches)} agences trouvées pour {bank_name} à {city}")

            reviews_data = []

            for branch in branches[:3]:  # On limite à 3 agences pour éviter d'être bloqué
                branch.click()
                time.sleep(5)

                branch_name = page.locator('h1.DUwDvf').text_content().strip() if page.locator('h1.DUwDvf').count() > 0 else "Inconnu"
                location = page.locator('div.rogA2c').nth(0).text_content().strip() if page.locator('div.rogA2c').count() > 0 else "Inconnue"

                if page.locator('button:has-text("Avis")').count() == 0:
                    print(f"❌ Aucun avis pour {branch_name}")
                    continue

                page.locator('button:has-text("Avis")').first.click()
                time.sleep(5)

                for _ in range(5):  # Scroll pour charger plus d'avis
                    page.keyboard.press("PageDown")
                    time.sleep(2)

                html = page.content()
                soup = BeautifulSoup(html, "html.parser")

                for review in soup.select('.jftiEf'):
                    text = review.select_one('.wiI7pd').text if review.select_one('.wiI7pd') else "Aucun texte"
                    rating = review.select_one('.kvMYJc')["aria-label"][0] if review.select_one('.kvMYJc') else "0"
                    date = review.select_one('.rsqaWe').text if review.select_one('.rsqaWe') else "Date inconnue"

                    reviews_data.append({
                        "bank": bank_name,
                        "city": city,
                        "branch": branch_name,
                        "location": location,
                        "text": text,
                        "rating": rating,
                        "date": date
                    })

            save_data(reviews_data)
            browser.close()

        except Exception as e:
            print(f"❌ Erreur lors du scraping : {e}")

# Vérification et exécution
if __name__ == "__main__":
    print("🚀 Script lancé !")

    # Vérifier si Playwright est installé
    try:
        with sync_playwright() as p:
            p.chromium.launch().close()
        print("✅ Playwright fonctionne correctement.")
    except Exception as e:
        print(f"❌ Erreur avec Playwright : {e}")
        print("👉 Exécute `playwright install` et `playwright install-deps` avant de relancer.")
        exit(1)

    # Vérifier si le fichier JSON existe, sinon le créer
    if not os.path.exists(RAW_DATA_FILE):
        print(f"📝 Création du fichier {RAW_DATA_FILE}...")
        with open(RAW_DATA_FILE, "w", encoding="utf-8") as f:
            json.dump([], f)

    # Lancer le scraping pour chaque banque et ville
    for bank in BANKS:
        for city in CITIES:
            scrape_reviews(bank, city)

    print("✅ Scraping terminé !")
