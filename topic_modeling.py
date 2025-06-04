# dags/topic_modeling.py
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
import pandas as pd

def perform_lda_analysis(df):
    # Vectorisation du texte
    vectorizer = CountVectorizer(max_df=0.95, min_df=2, stop_words='english')
    X = vectorizer.fit_transform(df['clean_text'])
    
    # Application de LDA
    lda = LatentDirichletAllocation(n_components=5, random_state=42)
    lda.fit(X)
    
    # Extraction des topics
    feature_names = vectorizer.get_feature_names_out()
    topics = []
    for topic_idx, topic in enumerate(lda.components_):
        topics.append({
            "topic_id": topic_idx,
            "top_terms": [feature_names[i] for i in topic.argsort()[-10:]]
        })
    
    return pd.DataFrame(topics)