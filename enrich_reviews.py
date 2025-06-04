import psycopg2
import pandas as pd
from textblob import TextBlob
from langdetect import detect
from gensim import corpora
from gensim.models import LdaModel
from nltk.tokenize import word_tokenize
import nltk

# Téléchargement du modèle nltk si nécessaire
nltk.download('punkt')

# Connexion à PostgreSQL
conn = psycopg2.connect(
    dbname="airflow_db", user="airflow_user", password="airflow_pass", host="localhost", port="5432"
)
cursor = conn.cursor()

# Fonction d'extraction de la langue
def extract_language(text):
    try:
        return detect(text)
    except:
        return "unknown"

# Fonction d'analyse du sentiment
def classify_sentiment(text):
    analysis = TextBlob(text)
    polarity = analysis.sentiment.polarity
    if polarity > 0:
        return "Positive"
    elif polarity < 0:
        return "Negative"
    else:
        return "Neutral"

# Fonction d'extraction des sujets avec LDA
def extract_topics(texts):
    tokenized_texts = [word_tokenize(text.lower()) for text in texts]
    dictionary = corpora.Dictionary(tokenized_texts)
    corpus = [dictionary.doc2bow(text) for text in tokenized_texts]
    
    lda = LdaModel(corpus, num_topics=3, id2word=dictionary, passes=15)
    topics = lda.print_topics(num_words=4)
    
    # Retourner une représentation des sujets comme chaîne de caractères
    return str(topics)

# Lire les données de la table stg_reviews
cursor.execute("SELECT * FROM stg_reviews")
rows = cursor.fetchall()

# Convertir les données en DataFrame pandas
df = pd.DataFrame(rows, columns=["bank", "city", "branch", "location", "clean_text", "rating", "date", "row_num"])

# Appliquer l'enrichissement des données
df['language'] = df['clean_text'].apply(extract_language)
df['sentiment'] = df['clean_text'].apply(classify_sentiment)

# Extraire les sujets (LDA) sur l'ensemble des avis
topics = extract_topics(df['clean_text'])

# Insérer les données enrichies dans la nouvelle table enriched_reviews
for index, row in df.iterrows():
    cursor.execute(
        """
        INSERT INTO enriched_reviews (bank, city, branch, location, clean_text, rating, date, language, sentiment, topics)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (row['bank'], row['city'], row['branch'], row['location'], row['clean_text'], row['rating'], row['date'], row['language'], row['sentiment'], topics)
    )

# Valider les changements
conn.commit()

# Fermer la connexion
cursor.close()
conn.close()

print("Données enrichies et insérées dans la table enriched_reviews.")



