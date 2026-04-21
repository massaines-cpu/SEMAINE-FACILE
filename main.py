from fastapi import FastAPI, UploadFile
import pandas as pd
import sqlite3

app = FastAPI()

@app.post('/upload')
def charger_csv(fichier: UploadFile):
    chemin = f'{fichier.filename}'
    with open(chemin, 'wb') as f:
        f.write(fichier.file.read())

    data = pd.read_csv(chemin)
    colonnes = data.columns
    connexion = sqlite3.connect('egg_base.db')

    curseur = connexion.execute(
        'INSERT INTO source (nom, url) VALUES (?, ?)',
        (fichier.filename, fichier.filename)
    )
    connexion.commit()
    id_source = curseur.lastrowid

    col_ids = {}
    for colonne in colonnes:
        type_colonne = str(data[colonne].dtype)
        curseur_col = connexion.execute(
            'INSERT INTO colonne (label, type, source_id) VALUES (?, ?, ?)',
            (colonne, type_colonne, id_source)
        )
        col_ids[colonne] = curseur_col.lastrowid
    connexion.commit()

    for row_id, ligne in enumerate(data.iterrows()):
        for ordre, colonne in enumerate(colonnes):
            valeur = str(ligne[1][colonne])
            connexion.execute(
                'INSERT INTO data (value, column_id, row_id, ordre) VALUES (?, ?, ?, ?)',
                (valeur, col_ids[colonne], row_id, ordre)
            )
    connexion.commit()
    connexion.close()

    return {'message': 'ok', 'source_id': id_source}

@app.get('/data/{id}')
def recup_data(id: int):
    connexion = sqlite3.connect('egg_base.db')
    curseur = connexion.execute('SELECT * FROM data WHERE id = ?', (id,))
    ligne = curseur.fetchone()
    connexion.close()

    if ligne is None:
        return {'erreur': 'pas de données'}

    return {
        'id': ligne[0],
        'value': ligne[1],
        'column_id': ligne[2],
        'row_id': ligne[3],
        'ordre': ligne[4]
    }

@app.put('/data/{id}')
def modif_data(id: int, nouvelle_valeur: str):
    connexion = sqlite3.connect('egg_base.db')
    connexion.execute('UPDATE data SET value = ? WHERE id = ?', (nouvelle_valeur, id))
    connexion.commit()
    connexion.close()
    return {'message': 'valeur maj'}

@app.post('/data')
def ajout_data(value: str, column_id: int, row_id: int, ordre: int):
    connexion = sqlite3.connect('egg_base.db')
    curseur = connexion.execute(
        'INSERT INTO data (value, column_id, row_id, ordre) VALUES (?, ?, ?, ?)',
        (value, column_id, row_id, ordre)
    )
    connexion.commit()
    connexion.close()
    return {'message': 'ligne ajoutée', 'id': curseur.lastrowid}

@app.delete('/data/{id}')
def delete_data(id: int):
    connexion = sqlite3.connect('egg_base.db')
    connexion.execute('DELETE FROM data WHERE id = ?', (id,))
    connexion.commit()
    connexion.close()
    return {'message': 'ligne supp'}