# Détection de fraude bancaire avec Hadoop, PySpark et Streamlit

Projet du cours Écosystème Hadoop (IABD), réalisé à quatre. L'objectif est de construire un pipeline de données de bout en bout : stockage sur HDFS, analyse et modélisation avec PySpark dans un notebook Jupyter, puis présentation des résultats dans une application Streamlit.

## Avancement

| Étape du sujet | Description | État |
|---|---|---|
| 1 | Choix du dataset et de la colonne à prédire | Fait |
| 2 | Installation de Hadoop (un nœud) | Fait |
| 3 | Installation de PySpark et Jupyter | Fait |
| 4 | Chargement du CSV dans HDFS | Fait |
| 4.1 à 4.4 | Lecture, typage, statistiques, nettoyage | Fait |
| 4.5 | Features engineering (variables pour le modèle) | Fait |
| 4.6 | Machine learning (découpage, modèles, évaluation) | Fait |
| 4.7 | Data viz : export des résultats et application Streamlit (`app.py`) | Export fait, `app.py` à faire |

## Dataset (étape 1)

Source : [Bank Transaction Fraud Detection](https://www.kaggle.com/datasets/marusagar/bank-transaction-fraud-detection), publié sur Kaggle par marusagar. Le jeu de données décrit des transactions de la banque LOL Bank Pvt. Ltd.

Le sujet impose un jeu de plus de 10 000 lignes, avec une colonne à prédire. Le fichier doit être placé dans le dossier `data/` : `Bank_Transaction_Fraud_Detection.csv` (68,3 Mo). Ce dossier n'est pas versionné.

| Caractéristique | Valeur |
|---|---|
| Lignes | Environ 200 000 |
| Colonnes | 24 |
| Cible | `Is_Fraud` (renommée `est_fraude`) : 1 si la transaction est frauduleuse, 0 sinon |

Le jeu est très déséquilibré : environ 5 % des transactions sont frauduleuses. C'est pourquoi on évalue les modèles avec l'AUPRC et le rappel, et non avec l'accuracy.

Les colonnes `Customer_Name`, `Customer_Contact` et `Customer_Email` contiennent des données personnelles. Elles sont retirées dans le notebook (étape 4.4).

## Structure du projet

```
Projet-HADOOP-3-IABD-RJ/
├── .gitignore
├── README.md
├── requirements.txt          # Dépendances Python
├── traitement_donnes.ipynb   # Pipeline PySpark (étapes 4.1 à 4.7)
├── app.py                    # Application Streamlit (à venir)
├── data/                     # Le CSV, non versionné
└── outputs/                  # Exports du notebook, non versionnés
```

## Système d'exploitation

Le projet tourne sous **Linux (Ubuntu 22.04)**. Sous **Windows**, il faut passer par **WSL 2** (Windows Subsystem for Linux) : Hadoop et Spark ne fonctionnent pas proprement en Windows natif sans fichiers supplémentaires (`winutils.exe`).

**Sous Windows**, ouvrir PowerShell en administrateur et lancer :

```powershell
wsl --install -d Ubuntu
```

Redémarrer l'ordinateur, ouvrir **Ubuntu** depuis le menu Démarrer, puis suivre **toutes** les étapes ci-dessous dans le terminal Ubuntu. Le notebook s'ouvre ensuite dans le navigateur Windows habituel, à l'adresse `localhost:8888`.

## Prérequis

- Ubuntu 22.04 (natif, ou dans WSL 2 sous Windows), avec 8 Go de RAM recommandés. Hadoop, Spark et Jupyter tournent ensemble : fermer les applications inutiles pendant l'exécution.
- Python 3.11.

Installer Java 11 et le serveur SSH :

```bash
sudo apt update
sudo apt install -y openjdk-11-jdk openssh-server
```

Vérifier Java 11 :

```bash
ls /usr/lib/jvm/java-11-openjdk-amd64/bin/java
```

Le chemin doit exister. Sur ce projet, c'est Java 11 qui est utilisé, même si Java 21 est installé par défaut. Hadoop reçoit ce chemin explicitement (voir ci-dessous).

## Installation de Hadoop (étape 2)

Hadoop est installé dans votre dossier personnel (`~/hadoop`), et non dans le dossier du projet.

1. **Télécharger Hadoop 3.3.6** depuis [les archives Apache](https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz) (environ 700 Mo). Le téléchargement peut être long : c'est le serveur qui limite la vitesse, pas votre connexion.

   ```bash
   cd ~
   wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
   tar -xzf hadoop-3.3.6.tar.gz
   mv hadoop-3.3.6 hadoop
   ls ~/hadoop
   ```

   `ls` doit afficher `bin`, `etc`, `sbin`, `share`...

2. **Définir les variables d'environnement** à la fin de `~/.bashrc`. Ce fichier est lu à chaque ouverture de terminal :

   ```bash
   cat >> ~/.bashrc << 'EOF'

   # Hadoop
   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
   export HADOOP_HOME=$HOME/hadoop
   export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
   EOF
   source ~/.bashrc
   hadoop version
   ```

   Ce que fait chaque ligne :
   - `JAVA_HOME` indique à Hadoop où est Java.
   - `HADOOP_HOME` indique où est installé Hadoop.
   - `PATH` permet de taper `hdfs`, `hadoop` et `start-dfs.sh` depuis n'importe quel dossier.

   `hadoop version` doit afficher `Hadoop 3.3.6`.

3. **Indiquer `JAVA_HOME` à Hadoop**. Les démons Hadoop sont lancés via SSH, qui ne lit pas `~/.bashrc` : Hadoop doit donc connaître Java par son propre fichier. Ajouter à la fin de `~/hadoop/etc/hadoop/hadoop-env.sh` :

   ```bash
   echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/hadoop/etc/hadoop/hadoop-env.sh
   tail -n 1 ~/hadoop/etc/hadoop/hadoop-env.sh
   ```

   La ligne d'origine `# export JAVA_HOME=` reste commentée : ce n'est pas un problème, la nouvelle ligne active est lue après.

4. **Configurer HDFS**. Deux fichiers XML décrivent le fonctionnement de HDFS. Chacun contient des propriétés, placées entre `<configuration>` et `</configuration>`.

   `core-site.xml` indique l'adresse du système de fichiers HDFS :

   ```xml
   <property>
     <name>fs.defaultFS</name>
     <value>hdfs://localhost:9000</value>
   </property>
   ```

   `hdfs-site.xml` indique le nombre de copies de chaque bloc. Sur un cluster à un seul nœud, il n'y a qu'une copie possible, d'où la valeur 1 (la valeur par défaut, 3, ne pourrait pas être respectée) :

   ```xml
   <property>
     <name>dfs.replication</name>
     <value>1</value>
   </property>
   ```

   Vérifier les deux propriétés :

   ```bash
   grep -A2 "<name>" ~/hadoop/etc/hadoop/core-site.xml ~/hadoop/etc/hadoop/hdfs-site.xml
   ```

   **Stockage permanent** : par défaut, Hadoop écrit ses données dans `/tmp`, qui est vidé au redémarrage. Les données HDFS seraient alors perdues. Pour les garder, indiquer des dossiers permanents dans `hdfs-site.xml` :

   ```bash
   mkdir -p ~/hadoop_store/hdfs/namenode ~/hadoop_store/hdfs/datanode
   ```

   ```xml
   <property>
     <name>dfs.namenode.name.dir</name>
     <value>file:/home/VOTRE_NOM/hadoop_store/hdfs/namenode</value>
   </property>
   <property>
     <name>dfs.datanode.data.dir</name>
     <value>file:/home/VOTRE_NOM/hadoop_store/hdfs/datanode</value>
   </property>
   ```

   Remplacer `VOTRE_NOM` par votre nom d'utilisateur (`echo $USER`). Ces réglages ne prennent effet qu'après le formatage de l'étape 5.

5. **Autoriser SSH sans mot de passe vers localhost**. Hadoop se connecte à lui-même par SSH pour lancer ses démons :

   ```bash
   mkdir -p ~/.ssh && chmod 700 ~/.ssh
   ls ~/.ssh
   ```

   Si `id_rsa` n'existe pas, créer la clé. Si elle existe déjà, ne pas la recréer :

   ```bash
   ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
   ```

   Puis autoriser cette clé et tester :

   ```bash
   cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
   chmod 600 ~/.ssh/authorized_keys
   ssh localhost 'echo SSH_OK'
   ```

   La première connexion demande de confirmer l'empreinte : taper `yes`. La sortie doit être `SSH_OK`, sans demande de mot de passe ni de passphrase.

6. **Formater le NameNode et lancer HDFS**. Le formatage crée les métadonnées de HDFS (dans les dossiers indiqués à l'étape 4). Il ne se fait **qu'une seule fois** pour un cluster neuf : il efface toutes les données existantes.

   ```bash
   hdfs namenode -format
   start-dfs.sh
   jps
   ```

   Dans la sortie du formatage, la ligne `Storage directory ... has been successfully formatted` confirme le succès. `jps` doit afficher `NameNode`, `DataNode` et `SecondaryNameNode`.

   Vérifier l'interface web : `http://localhost:9870`, puis le menu **Datanodes** doit montrer un nœud « In service ».

## Environnement Python (étape 3)

Depuis la racine du projet :

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install "pyspark==3.5.*"
```

- `python3 -m venv venv` crée un environnement Python isolé : les paquets du projet n'interfèrent pas avec le reste du système.
- `source venv/bin/activate` active cet environnement. Le terminal affiche alors `(venv)`. Il faut le refaire à chaque nouveau terminal.

Sous Linux et WSL, retirer la ligne `pywinpty` de `requirements.txt`, car ce paquet est réservé à Windows natif :

```bash
pip install -r <(grep -v '^pywinpty' requirements.txt)
pip install "pyspark==3.5.*"
```

Vérifier PySpark :

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

La version doit être `3.5.9`. PySpark 3.5 est utilisé avec Java 11 : ne pas installer PySpark 4, qui exige Java 17.

## Chargement du CSV dans HDFS (étape 4)

Une fois HDFS lancé (`start-dfs.sh`), créer le dossier et y copier le fichier :

```bash
hdfs dfs -mkdir -p /data/raw
hdfs dfs -put data/Bank_Transaction_Fraud_Detection.csv /data/raw/
hdfs dfs -ls /data/raw
```

Le notebook lit le fichier à `hdfs://localhost:9000/data/raw/Bank_Transaction_Fraud_Detection.csv`. Si la commande `-ls` affiche une erreur, le fichier n'est pas à cet emplacement : relancer les commandes ci-dessus.

## Lancer le projet après un redémarrage

Un redémarrage arrête Hadoop. Dans cet ordre :

1. Ouvrir un terminal et vérifier qu'aucun service ne tourne : `jps` ne doit afficher que `Jps`.
2. Lancer HDFS : `start-dfs.sh`, puis `jps` (trois processus attendus).
3. Vérifier les données : `hdfs dfs -ls /data/raw`.
4. **Ensuite seulement**, ouvrir le notebook. Le noyau Jupyter ne doit pas démarrer avant HDFS, car il peut prendre le port 9000 utilisé par le NameNode.

## Exécution du notebook

Depuis la racine du projet, avec le venv activé et HDFS lancé :

```bash
jupyter notebook
```

Ouvrir `traitement_donnes.ipynb`, puis lancer **Restart** du noyau et **Run All**. Le noyau doit être le Python du venv (`venv/bin/python`). Le notebook crée le dossier `outputs/` et y enregistre `resultats_modeles.csv`, `statistiques.csv` et `repartition_fraude.csv`.

## Problèmes courants

| Message | Cause probable | Solution |
|---|---|---|
| `Adresse déjà utilisée` pour le port 9000 | Un noyau Jupyter occupe le port | Fermer le notebook ou arrêter le noyau, puis `stop-dfs.sh` puis `start-dfs.sh` |
| `RPC response has invalid length` | Le NameNode n'est pas lancé | Vérifier `jps`, relancer `start-dfs.sh` |
| `PATH_NOT_FOUND` à la lecture du CSV | Le fichier n'est pas dans `/data/raw` | Relancer la copie de l'étape 4 |
| `namenode is running as process ...` | Le service tourne déjà | Rien à faire, vérifier avec `jps` |
| Données HDFS disparues | Stockage dans `/tmp` | Configurer le stockage permanent (étape 4 de l'installation) |

## Application Streamlit

À compléter une fois `app.py` écrit. Elle lira les fichiers de `outputs/`, qui doivent donc exister : lancer le notebook avant.

```bash
streamlit run app.py
```

## Résultats

Deux modèles sont évalués sur le jeu de test (39 782 transactions, dont 1 976 frauduleuses) :

| Modèle | AUPRC | AUROC |
|---|---|---|
| Régression logistique | 0,0494 | 0,4994 |
| Random forest | 0,0506 | 0,5071 |

L'AUROC proche de 0,5 indique que les modèles ne séparent pas nettement les fraudes des transactions normales. Ils détectent une partie des fraudes (28 à 48 % selon le modèle), mais avec une précision d'environ 5 %, donc beaucoup de fausses alertes. Les variables disponibles ne suffisent donc pas à distinguer les fraudes avec ces modèles.

## Équipe

À compléter avec les noms des membres.
