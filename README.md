# EcommerceAnalytics

Pipeline Spark/Scala d'analyse de données e-commerce : ingestion de 4 sources (CSV, JSON, Parquet),
validation, enrichissement des transactions, KPI marchands, cohortes et écriture des résultats.

## Prérequis

| Outil | Version utilisée |
|-------|------------------|
| Java (JDK) | 17 |
| Scala | 2.13.18 |
| SBT | 1.10.x |
| Spark | 4.2.0 |

Installation rapide :

- **Java 17** : https://adoptium.net (penser à définir `JAVA_HOME`)
- **Scala + SBT** : le plus simple est de passer par Coursier (https://get-coursier.io), puis
  ```
  cs setup
  ```
  qui installe `scala` et `sbt`.
- **Spark** : télécharger l'archive sur https://spark.apache.org/downloads.html, la décompresser
  et ajouter le dossier `bin/` au `PATH` (variable `SPARK_HOME`).

Sous Windows, l'écriture des fichiers de sortie demande `winutils.exe` et `hadoop.dll` dans
`%HADOOP_HOME%in`, avec ce dossier dans le `PATH`. Ils doivent correspondre à la version Hadoop de
Spark (3.5 pour Spark 4.2.0), sinon on obtient une erreur `UnsatisfiedLinkError ... NativeIO$Windows`.

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

## Tests

```
sbt test
```

| Fichier de test | Ce qui est vérifié |
|-----------------|--------------------|
| `DataValidationTest` | chaque règle de validation, le rapport de qualité, les références orphelines |
| `DataIngestionTest` | lecture des vrais fichiers (nombre de lignes, types), rejet non nul, écriture du rapport |
| `TimeFeaturesSpec` | UDF `extractTimeFeatures`, y compris les entrées nulles ou mal formées |
| `AnalyticsTest` | KPI marchands, cohortes, matrice de rétention, meilleure cohorte |
| `SparkOptimizationsTest` | cache, persist, unpersist, broadcast |

`DataIngestionTest` utilise les fichiers de `src/main/resources/data` : s'ils sont absents, ses tests
sont marqués « canceled » au lieu d'échouer.

## Exécution locale

Depuis la racine du projet :

```
sbt run
```

Le pipeline enchaîne ingestion → validation → transformation → analytique → écriture, affiche les
résultats dans la console et la durée de chaque étape (environ 2 minutes en local).

Résultats produits dans `output/` :

```
output/
├── quality_report/          rapport de qualité des données (CSV)
├── csv/                     un fichier CSV avec en-tête par résultat
│   ├── kpi_marchands/
│   ├── retention_cohortes/
│   ├── matrice_retention/
│   └── meilleure_cohorte_3_mois/
├── parquet/                 les mêmes résultats en Parquet
└── rejected/                lignes rejetées par la validation, avec rejection_reason (Parquet)
    ├── transactions/  users/  products/  merchants/
```

Le rapport de qualité donne, par dataset : lignes lues / valides / rejetées, taux de rejet, valeurs
nulles, et pour les transactions le nombre de références orphelines (user_id, product_id,
merchant_id absents du référentiel).

Les logs de Spark sont limités aux avertissements (`src/main/resources/log4j2.properties`).

## Déploiement avec spark-submit

En local, depuis la racine du projet (les chemins des données sont relatifs) :

```
spark-submit \
  --class com.ecommerce.analytics.MainApp \
  --master local[*] \
  target/scala-2.13/ecommerce-analytics.jar
```

Le `--master` passé à spark-submit est prioritaire sur celui de `application.conf`.

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
