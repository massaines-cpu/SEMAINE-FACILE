import pandas as pd
import sqlite3

fichier_csv = 'Data1.csv'

data = pd.read_csv(fichier_csv)
colonnes = data.columns
print(colonnes)

connexion = sqlite3.connect("egg_base.db")

connexion.execute('''CREATE TABLE IF NOT EXISTS source (
        id    INTEGER PRIMARY KEY,
        nom   TEXT,
        url   TEXT)
''')

connexion.execute('''CREATE TABLE IF NOT EXISTS colonne (
        id        INTEGER PRIMARY KEY,
        label     TEXT,
        type      TEXT,
        source_id INTEGER)
''')

connexion.execute(''' CREATE TABLE IF NOT EXISTS data (
        id        INTEGER PRIMARY KEY,
        value     TEXT,
        column_id INTEGER,
        row_id    INTEGER,
        ordre     INTEGER)
''')
connexion.commit()

print('ok')

curseur = connexion.execute('INSERT INTO source (nom, url) VALUES (?, ?)',('Data1.csv', 'Data1.csv'))
connexion.commit()

id_source = curseur.lastrowid

print('ok2')
col_id = {}
for colonne in colonnes:
    type_colonne = str(colonnes.dtype)
    c = connexion.execute('INSERT INTO colonne (label, type, source_id) VALUES (?, ?, ?)',
                      (colonne, type_colonne, id_source))
    connexion.commit()
    col_id[colonne] = c.lastrowid

print('ok3')

for row_id, ligne in enumerate(data.iterrows()):
    for ordre, colonne in enumerate(colonnes):
        valeur = str(ligne[1][colonne])
        connexion.execute('INSERT INTO data (value, column_id, row_id, ordre) VALUES (?, ?, ?, ?)',
                          (valeur, col_id[colonne], row_id, ordre))

print('fini 1')

# ouvrir le fichier
# ajoute la source nom du fichier etc
# recup la liste des col avec leur types
# ajoute chaque col dans la bdd
# pareil avec les data