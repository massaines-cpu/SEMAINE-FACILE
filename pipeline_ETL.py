import pandas as pd
import sqlite3

# Faire un pipeline ETL pour des données numériques.
#
# 1. en entrée du pipeline il y a l’url d’un fichier csv.
# 2. le système analyse et extrait les données.
# 3. le système les enregistre dans une base SQLite

fichier_csv = 'Data1.csv'

data = pd.read_csv(fichier_csv)
colonnes = data.columns
print(colonnes)

connexion = sqlite3.connect("egg_base.db")

connexion.execute('''
    CREATE TABLE IF NOT EXISTS source (
        id    INTEGER PRIMARY KEY,
        nom   TEXT,
        url   TEXT)
''')

connexion.execute('''
    CREATE TABLE IF NOT EXISTS colonne (
        id        INTEGER PRIMARY KEY,
        label     TEXT,
        type      TEXT,
        source_id INTEGER)
''')

connexion.execute('''
    CREATE TABLE IF NOT EXISTS data (
        id        INTEGER PRIMARY KEY,
        value     TEXT,
        column_id INTEGER,
        row_id    INTEGER,
        ordre     INTEGER)
''')
connexion.commit()

print('ok')

connexion.execute(
    'INSERT INTO source (nom, url) VALUES (?, ?)',('Data1.csv', 'Data1.csv'))
connexion.commit()

print('ok2')

for colonne in colonnes:
    pass
# ouvrir le fichier
# ajoute la source nom du fichier etc
# recup la liste des col avec leur types
# ajoute chaque col dans la bdd
# pareil avec les data