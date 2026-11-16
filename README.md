# Détection de fraude bancaire avec Hadoop, PySpark et Streamlit

Projet du cours Écosystème Hadoop (IABD), réalisé à quatre. L'objectif est de construire un pipeline de données de bout en bout : stockage sur HDFS, analyse et modélisation avec PySpark dans un notebook Jupyter, puis présentation des résultats dans un dashboard Streamlit.

## Avancement

| Étape du sujet | Description | État |
|---|---|---|
| 1 | Choix du dataset et de la colonne à prédire | Fait |
| 2 | Installation de Hadoop | À faire |
| 3 | Installation de PySpark et Jupyter | À faire |
| 4 | Chargement du CSV dans HDFS | À faire |
| 5 | Notebook PySpark | À faire |
| 6 | Dashboard Streamlit | À faire |

## Dataset

Source : [Bank Transaction Fraud Detection](https://www.kaggle.com/datasets/marusagar/bank-transaction-fraud-detection), publié sur Kaggle par marusagar. Le jeu de données décrit des transactions de la banque LOL Bank Pvt. Ltd.

Le fichier à placer dans `data/` s'appelle `Bank_Transaction_Fraud_Detection.csv` (68,3 Mo).

| Caractéristique | Valeur |
|---|---|
| Lignes | Environ 200 001 |
| Colonnes | 24 |
| Cible | `Is_Fraud` : 1 si la transaction est frauduleuse, 0 sinon |


Les colonnes couvrent trois blocs : les informations du client (identifiant, genre, âge, état, ville), la banque (agence, type de compte, solde) et la transaction (montant, date, heure, marchand, appareil, localisation).

Les colonnes `Customer_Name`, `Customer_Contact` et `Customer_Email` contiennent des données personnelles. Leur exclusion, comme toute autre modification des colonnes, sera faite dans la partie analyse du notebook.


## Structure du projet

```
projet-fraude-hadoop/
├── .gitignore            
├── README.md
├── requirements.txt      # Dépendances Python
├── notebook.ipynb        # Pipeline PySpark
├── app.py                # Dashboard Streamlit
├── data/                 
└── outputs/              # Exports du notebook, non versionné
```

## Mise en place de l'environnement Python

À compléter une fois l'installation validée sur la machine.

## Installation de Hadoop

À compléter à l'étape 2.