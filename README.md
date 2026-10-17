# EcommerceAnalytics

Pipeline Spark/Scala d'analyse de données e-commerce : ingestion de 4 sources (CSV, JSON, Parquet),
validation, enrichissement des transactions, KPI marchands, cohortes et écriture des résultats.

## Prérequis

| Outil | Version utilisée |
|-------|------------------|
| Java (JDK) | 17 |
| Scala | 2.13 |
| SBT | 1.10.x |
| Spark | 4.x (pour spark-submit) |

Installation rapide :

- **Java 17** : https://adoptium.net (penser à définir `JAVA_HOME`)
- **Scala + SBT** : le plus simple est de passer par Coursier (https://get-coursier.io), puis
  ```
  cs setup
  ```
  qui installe `scala` et `sbt`.
- **Spark** : télécharger l'archive sur https://spark.apache.org/downloads.html, la décompresser
  et ajouter le dossier `bin/` au `PATH` (variable `SPARK_HOME`).

Sous Windows, Spark peut demander `winutils.exe` (variable `HADOOP_HOME`) pour l'écriture des fichiers.

## Données

Les fichiers sources sont à placer dans `src/main/resources/data/` :

```
transactions.csv
users.json
products.parquet/   (répertoire)
merchants.csv
```

Les chemins se règlent dans `src/main/resources/application.conf` (section `app.data.input`).

## Compilation

```
sbt compile
```

Générer le JAR exécutable :

```
sbt assembly
```

Le JAR est créé dans `target/scala-2.13/ecommerce-analytics.jar`.
Spark est déclaré en `provided` : il n'est pas inclus dans le JAR, c'est `spark-submit` qui le fournit.

## Exécution locale

Depuis la racine du projet :

```
sbt run
```

Les résultats sont écrits dans le dossier `output/` (CSV et Parquet).

## Déploiement avec spark-submit

En local :

```
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master local[*] \
  target/scala-2.13/ecommerce-analytics.jar
```

Sur un cluster (exemple YARN) :

```
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-memory 4g \
  --files application.conf \
  --conf spark.driver.extraJavaOptions=-Dconfig.file=application.conf \
  target/scala-2.13/ecommerce-analytics.jar
```

`-Dconfig.file` permet d'utiliser un autre fichier de configuration que celui embarqué dans le JAR
(chemins HDFS, nombre de partitions...).

## Configuration

Tous les paramètres sont dans `application.conf` : chemins, master Spark, `shuffle.partitions`,
activation du cache / broadcast, seuils de validation. Si une clé manque, `ConfigLoader` utilise
une valeur par défaut.

## Structure

```
src/main/scala/com/ecommerce/
├── analytics/   ingestion, validation, transformations, analytique, MainApp
├── models/      case classes Transaction, User, Product, Merchant
└── utils/       ConfigLoader, SparkSessionBuilder
```
