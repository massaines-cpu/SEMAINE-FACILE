import streamlit as st
import requests
import pandas as pd

url = "http://127.0.0.1:8000"

st.title("pipeline ETL")

# récupérer les sources disponibles
sources = requests.get(f"{url}/sources").json()
print(sources)

# afficher un sélecteur
noms_sources = [source["nom"] for source in sources]
choix = st.selectbox("choisir une source", noms_sources)

# trouver l'id de la source choisie
id_source = [source["id"] for source in sources if source["nom"] == choix][0]

# récupérer les données de cette source
reponse = requests.get(f"{url}/sources/{id_source}/data").json()
colonnes = reponse["colonnes"]
donnees = reponse["donnees"]

# reconstruire un tableau lisible
noms_colonnes = [colonne["label"] for colonne in colonnes]
ids_colonnes = {colonne["id"]: colonne["label"] for colonne in colonnes}

lignes = {}
for d in donnees:
    if d["row_id"] not in lignes:
        lignes[d["row_id"]] = {"_ids": {}}
    lignes[d["row_id"]][ids_colonnes[d["column_id"]]] = d["value"]
    lignes[d["row_id"]]["_ids"][ids_colonnes[d["column_id"]]] = d["id"]

df = pd.DataFrame([{k: v for k, v in l.items() if k != "_ids"} for l in lignes.values()])

# afficher le tableau
st.dataframe(df)