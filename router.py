from fastapi import APIRouter
import sqlite3

router = APIRouter()

@router.get('/sources')
def recup_source():
    connexion = sqlite3.connect('egg_base.db')
    sources = connexion.execute('SELECT * FROM source').fetchall()
    connexion.close()
    liste = []
    for source in sources:
        liste.append({
            'id': source[0],
            'nom': source[1],
            'url': source[2]
        })
    return liste

@router.get('/sources/{id_source}/data')
def recup_source_data(id_source: int):
    connexion = sqlite3.connect('egg_base.db')
    colonnes = connexion.execute('SELECT id, label FROM colonne WHERE source_id = ?', (id_source,)).fetchall()
    data = connexion.execute(
        'SELECT * FROM data WHERE column_id IN ({})'.format(
            ','.join('?' * len(colonnes))), [colonne[0] for colonne in colonnes]).fetchall()
    connexion.close()
    return {
        'colonnes': [{'id': colonne[0], 'label': colonne[1]} for colonne in colonnes],
        'donnees': [{'id': data_au_singulier[0], 'value': data_au_singulier[1],
                     'column_id': data_au_singulier[2], 'row_id': data_au_singulier[3],
                     'ordre': data_au_singulier[4]} for data_au_singulier in data]
    }
@router.post('/sources')
def create_source(nom: str, colonnes: str):
    connexion = sqlite3.connect('egg_base.db')
    curseur = connexion.execute(
        'INSERT INTO source (nom, url) VALUES (?, ?)', (nom, nom)
    )
    connexion.commit()
    id_source = curseur.lastrowid

    for col in colonnes.split(','):
        label, type_col = col.strip().split(':')
        connexion.execute(
            'INSERT INTO colonne (label, type, source_id) VALUES (?, ?, ?)',
            (label.strip(), type_col.strip(), id_source)
        )
    connexion.commit()
    connexion.close()
    return {'message': 'source créée', 'id': id_source}