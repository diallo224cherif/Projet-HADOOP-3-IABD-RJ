# Détection de fraude bancaire avec Hadoop, PySpark et Streamlit



Projet du cours Écosystème Hadoop (IABD), réalisé à quatre. L'objectif est de construire un pipeline de données de bout en bout : stockage distribué sur HDFS, analyse exploratoire, profilage comportemental et modélisation avec PySpark dans un notebook Jupyter, puis restitution interactive des résultats dans un dashboard Streamlit.



## Avancement



| Étape du sujet | Description | État |

|---|---|---|

| 1 | Choix du dataset et de la colonne à prédire | Fait |

| 2 | Installation et configuration de Hadoop (HDFS) | Fait |

| 3 | Installation de PySpark, Jupyter et Streamlit | Fait |

| 4 | Chargement du CSV dans HDFS | Fait |

| 5 | Notebook PySpark (ETL, EDA, profilage, MLlib) | Fait |

| 6 | Dashboard Streamlit (`app.py`) | Fait |



## Dataset



Source : [Credit Card Transactions Fraud Detection Dataset (Sparkov)](https://www.kaggle.com/datasets/kartik2112/fraud-detection), publié sur Kaggle par `kartik2112`. Initialement prévu sur *Bank Transaction Fraud Detection* (données synthétiques sans signal exploitable), le projet a basculé sur le jeu de données **Sparkov**, qui simule des comportements bancaires réalistes avec des schémas de fraude identifiables (horaires nocturnes, montants atypiques, catégories à risque).



Le fichier utilisé s'appelle `fraudTest.csv` (~150 Mo).



| Caractéristique | Valeur |

|---|---|

| Lignes | 555 719 |

| Colonnes | 23 brutes (22 utiles après suppression de l'index `_c0`) |

| Cible | `is_fraud` (renommée `est_fraude`) : `1` si la transaction est frauduleuse, `0` sinon |

| Taux de fraude | Environ 0,39 % (fort déséquilibre de classes) |



Les colonnes couvrent trois blocs : les informations du client (`first`, `last`, `gender`, `dob`, `street`, `city`, `state`, `zip`, `lat`, `long`, `city_pop`, `job`), la carte (`cc_num`) et la transaction (`trans_num`, `trans_date_trans_time`, `unix_time`, `amt`, `merchant`, `category`, `merch_lat`, `merch_long`).



**Traitement des données personnelles :** Les colonnes identifiantes (`prenom_client`, `nom_client`, `date_naissance`) sont utilisées temporairement pour construire l'historique comportemental de chaque porteur via des *Window functions*, puis toutes les données personnelles (`prenom_client`, `nom_client`, `rue_client`, `num_carte`, `id_transaction`) sont supprimées avant l'étape d'apprentissage (anonymisation et prévention du surapprentissage).



## Pipeline PySpark (`notebook.ipynb`)



1. **Chargement (4.1) :** Lecture du fichier CSV brut depuis HDFS avec `inferSchema=False` et paramètres optimisés pour WSL (`local[2]`, `spark.driver.memory=3g`, `spark.sql.shuffle.partitions=16`).

2. **Renommage et typage (4.2) :** Normalisation des noms de colonnes en français, conversion explicite des types (dates, timestamps, numériques) et contrôle des valeurs non converties.

3. **Analyse exploratoire (4.3) :** Statistiques descriptives, répartition des classes, montants moyens par classe, taux de fraude par catégorie et export d'une table croisée `categorie_marchand × heure` (`outputs/heatmap_fraude.csv`).

4. **Nettoyage et profilage comportemental (4.4) :** Suppression des doublons et valeurs incohérentes, puis calcul de 5 variables d'habitudes client strictement tournées vers le passé (sans *data leakage*) via `Window` :

   - `montant_moyen_client` et `ratio_montant_habituel`

   - `ecart_heure_habituelle`

   - `nb_trans_24h` (fréquence sur les 24 heures précédentes)

   - `delai_derniere_trans_min` (temps écoulé depuis la transaction précédente)



   Les données personnelles sont ensuite supprimées et le jeu intermédiaire est persisté en Parquet (`outputs/df_prof.parquet`) pour libérer la mémoire RAM.

5. **Feature engineering et prétraitement (4.5) :** Calcul de `heure`, `jour_semaine`, `age` et `distance_km` (formule sphérique de Haversine), export de la matrice de corrélation de Pearson (`outputs/correlation.csv`), pondération de la classe minoritaire (`poids`), split train/test (80 % / 20 %) et pipeline MLlib (`StringIndexer` + `OneHotEncoder` sur `categorie_marchand`, `genre`, `etat` + `VectorAssembler` + `StandardScaler`, soit 77 *features* en entrée).

6. **Modélisation et évaluation (4.6) :** Entraînement et comparaison de trois algorithmes sur le même jeu de test non modifié :

   - **LogisticRegression** (modèle linéaire pondéré via `weightCol`)

   - **RandomForestClassifier** (arbres de décision pondérés via `weightCol`)

   - **MultilayerPerceptronClassifier (MLP)** (réseau de neurones `[nb_features, 32, 16, 2]`, entraîné sur un jeu d'entraînement rééquilibré par `sampleBy`)



   L'évaluation privilégie **AUPRC** (adapté aux classes très déséquilibrées), complété par **AUROC**, **F1_Fraude**, le rappel, la précision et la matrice de confusion, exportés dans `outputs/resultats_modeles.csv`.



## Structure du projet



```text

projet-fraude-hadoop/

├── .gitignore

├── README.md

├── requirements.txt             # Dépendances Python (pyspark, pandas, streamlit, jupyter)

├── notebook.ipynb               # Pipeline complet PySpark (étapes 4.1 à 4.6)

├── app.py                       # Dashboard interactif Streamlit

├── data/                        # Dataset local (non versionné)

└── outputs/                     # Exports générés par le notebook (non versionné)

    ├── df_prof.parquet/         # Checkpoint Parquet après profilage comportemental

    ├── statistiques.csv         # Statistiques descriptives

    ├── heatmap_fraude.csv       # Taux de fraude (%) par catégorie et par heure

    ├── correlation.csv          # Matrice de corrélation (Pearson)

    └── resultats_modeles.csv    # Comparaison des performances des 3 modèles

```



## Mise en place de l'environnement Python



Prérequis : Linux / WSL2, Java (JDK 11 ou 17) pour Hadoop et Spark, Python 3.10+.



```bash

# Création et activation de l'environnement virtuel

python3 -m venv .venv

source .venv/bin/activate



# Installation des dépendances

pip install -r requirements.txt

```



**Note WSL2 (mémoire) :** Le calcul des *Window functions* sur ~555 000 lignes peut saturer la RAM par défaut de WSL2. Si nécessaire, configurer `%USERPROFILE%\.wslconfig` sous Windows (`memory=8GB`, `swap=6GB`) puis relancer WSL avec `wsl --shutdown`.



## Installation et chargement dans Hadoop (HDFS)



Une fois les démons Hadoop démarrés (`start-dfs.sh`), créer le répertoire cible sur HDFS et y déposer le jeu de données :



```bash

# Création du dossier sur HDFS

hdfs dfs -mkdir -p /user/$USER/data



# Envoi du fichier CSV dans HDFS

hdfs dfs -put -f data/fraudTest.csv /user/$USER/data/



# Vérification du fichier sur HDFS

hdfs dfs -ls -h /user/$USER/data/

```



Le chemin HDFS utilisé par le notebook peut être surchargé via la variable d'environnement `DATA_PATH` (par défaut : `hdfs://localhost:9000/user/<utilisateur>/data/fraudTest.csv`).



## Exécution du projet



1. **Lancer le notebook PySpark** pour exécuter le pipeline de bout en bout et générer les fichiers dans `outputs/` :

   ```bash

   jupyter notebook notebook.ipynb

   ```

2. **Lancer le dashboard Streamlit** pour visualiser les KPIs, le comparatif des trois modèles, la matrice de corrélation, la carte thermique (heatmap) et le diagnostic automatique :

   ```bash

   streamlit run app.p