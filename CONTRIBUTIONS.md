# Journal de contribution

## Répartition

| Question | Responsable | Relecteur |
|----------|-------------|-----------|
| 0.1 – 0.3 Organisation | Tous | Tous |
| 1.1 Structure SBT | A | C |
| 1.2 build.sbt | A | C |
| 1.3 README | A | C |
| 2.1 Ingestion | A | B |
| 2.2 Validation | A | B |
| 2.3 Gestion d'erreurs | A | B |
| 2.4 Rapport qualité | A | B |
| 2.5 Intégrité référentielle (bonus) | A | B |
| 3.1 UDF extractTimeFeatures | B | A |
| 3.2 enrichTransactionData | B | A |
| 3.3 Fenêtrage | B | A |
| 4.1 KPI marchands | C | B |
| 4.2 Cohortes | C | B |
| 5.1 Stockage | C | A et B |
| 5.2 Jointures | C | A et B |
| 6.1 MainApp | C | A, B, C |
| 7.1 application.conf | A | C |

## Charge de travail et difficultés

### Membre A – COLY Ousmane
- Temps passé : environ 6 heures (lecture du sujet, installation, organisation Git/GitHub, ingestion et validation, tests, relecture de la partie 3, documentation)
- Difficultés :
  - `users.json` : Spark infère `age` en `Long` alors que la case class attend un `Int`, et `.as[User]`
    refuse la conversion. Résolu en imposant un schéma explicite à la lecture du JSON.
  - `merchants.csv` : avec `inferSchema`, `establishment_date` (yyyyMMdd) est lu comme un entier.
    Il a fallu le recaster en `String` avant de passer en `Dataset[Merchant]`.
  - Valeurs nulles dans les colonnes numériques : la conversion en `Dataset[T]` échouait avec des
    `Double` / `Int` simples, d'où le passage en `Option`.
  - Écriture du rapport CSV impossible sous Windows (`UnsatisfiedLinkError NativeIO$Windows`) :
    le `hadoop.dll` installé ne correspondait pas à la version Hadoop tirée par Spark 4.0.1.
    Corrigé en alignant `build.sbt` sur Spark 4.2.0, la version installée sur le poste.

### Membre B – CISSOKO Djely Kanny
- Temps passé : environ 10 heures (UDF, jointures, fenêtrage, tests, Git)
- Difficultés :
  - Installer et configurer Git, Java et sbt sous Windows (PATH, PowerShell).
  - Comprendre les fonctions de fenêtrage (rangeBetween sur 7 jours, lag).
  - Gérer les valeurs nulles et les timestamps invalides dans l'UDF.

### Membre C
- Temps passé : environ 7 heures (lecture du sujet, SparkOptimizations, Analytics, MainApp, tests, intégration, documentation)
- Points d'attention identifiés dès la conception :
  - Le CSV ne supporte pas les colonnes de type tableau (ex. `preferred_categories`) : les lignes rejetées sont donc écrites en Parquet, et le CSV est réservé aux résultats agrégés
  - Le cache est paresseux : il ne se remplit qu'au premier « action ». Résolu avec un `count()` qui force la matérialisation à un endroit connu
  - Cohortes : calcul du `period_index` en mois et cas des cohortes trop récentes qui n'ont pas encore atteint la période 3.

## Décisions techniques du groupe

1. **Spark 4.2.0 / Scala 2.13 / Java 17.**
   Spark 4 n'est plus publié pour Scala 2.12, donc on est passé en 2.13. On compile avec la même
   version de Spark que celle utilisée par spark-submit pour éviter les écarts de dépendances
   (on a eu un problème avec Hadoop sous Windows en 4.0.1). Java 17 est la LTS supportée par Spark 4.

2. **JAR construit avec sbt-assembly.**
   `sbt package` ne contient que nos classes, il manquerait Typesafe Config au lancement.
   Avec assembly on a un seul JAR autonome ; Spark reste en `provided` car il est fourni par spark-submit.

3. **Configuration avec Typesafe Config + valeurs par défaut.**
   Tous les paramètres sont dans application.conf et lus via `ConfigLoader`. Chaque lecture
   passe par `hasPath` pour retomber sur une valeur par défaut si la clé est absente.

4. **Champs numériques en `Option` dans les case classes.**
   Les fichiers contiennent des valeurs nulles : avec un `Double` simple, la conversion en
   `Dataset[T]` plante. `Option` permet de garder ces lignes et de les rejeter proprement à la validation.

5. **Lignes rejetées conservées avec toutes les règles violées.**
   Chaque fonction de validation renvoie un `ValidationResult` (valides + rejetées). La colonne
   `rejection_reason` liste toutes les règles non respectées (pas seulement la première), ce qui
   aide à comprendre les anomalies. Le rapport de qualité est calculé sur les données brutes.

6. **Stratégie de jointure (Q3.2) : LEFT JOIN depuis les transactions.**
   La table `transactions` est la table de faits. Elle reçoit trois LEFT JOIN (users, products, merchants),
   avec `broadcast()` sur les trois petites tables.
   Un INNER JOIN supprimerait les transactions dont le user_id, le product_id ou le merchant_id n'existe
   pas, donc du chiffre d'affaires. Avec LEFT JOIN on garde ces lignes : la tranche d'âge vaut « Inconnu »
   et les KPI marchands (Membre C) filtrent ensuite `merchant_name IS NOT NULL`.

   Autres décisions de la partie 3 :
   - L'âge de 25 ans n'est couvert par aucune tranche dans le sujet : on le range dans « Jeune ».
   - L'UDF renvoie `null` (sans exception) si le timestamp est nul, vide ou impossible (ex. mois 13).
   - « Utilisateur actif » : `collect_set` des jours sur 7 jours glissants, au moins 5 jours distincts.
   - « Dépasse de plus de 300 % » signifie : montant supérieur à 4 fois le panier moyen de l'utilisateur.

7. **Format de sortie : CSV et Parquet**
    Les résultats agrégés (KPI marchands, rétention, matrice, meilleure cohorte) sont écrits en CSV (un seul fichier avec en-tête, lisible par l'équipe métier) et en Parquet (typé et compact, pour une reprise par d'autres traitements Spark). Les lignes rejetées sont écrites en Parquet uniquement, car le CSV ne gère pas les colonnes de type tableau. Le rapport de qualité reste en CSV.

8. **Stratégie de cache (Q5.1)**
   `persist(MEMORY_AND_DISK_SER)` sur les transactions valides (la plus grosse table, environ 136 000 lignes : stockage sérialisé avec débordement sur disque) et `cache()` sur le DataFrame enrichi (lu par les KPI marchands, les cohortes et l'écriture). Le cache est matérialisé par un `count()` pour payer le calcul une seule fois, et libéré par `unpersist` dans le `finally` de `MainApp`, le tout pilotépar `enable-cache`.

9. **Broadcast et partitions (Q5.2)**
   `broadcast()` est appliqué dans `DataTransformation` sur `users`, `products` et `merchants` (Kany), et dans `Analytics` sur la table des tailles de cohorte via
   `SparkOptimizations.broadcastIfEnabled`, toujours selon `enable-broadcast`.
   `spark.sql.shuffle.partitions` (8) vient de `application.conf`, appliqué dans `SparkSessionBuilder`.

10. **Choix d'analytique (Partie 4)**
    Les marchands inconnus (`merchant_name` nul) sont exclus des KPI. Le classement par catégorie et par région est calculé sur le tableau déjà agrégé par marchand. La "meilleure cohorte à 3 mois" ne considère que les cohortes qui ont des actifs à la période 3 (les plus récentes sont donc exclues), avec la taille de cohorte comme critère de départage.

## Relectures croisées

| Date | Module relu | Auteur | Relecteur | Remarques |
|------|-------------|--------|-----------|-----------|
| 2026-10-18 | Partie 3 : TimeFeatures, DataTransformation (PR #1) | B – CISSOKO Djely Kanny | A – COLY Ousmane | Testé avec l'ingestion et la validation sur les données réelles (136 157 transactions enrichies, tests TimeFeatures OK). Correction demandée : `to_timestamp` plante en mode ANSI sur une date impossible, remplacer par `try_to_timestamp`. Remarques : clés `app.analytics.*` à ajouter dans application.conf, broadcast déjà fait ici (à signaler pour la 5.2). |
| 2026-10-20 | Partie 3 : correction (PR #1) | B – CISSOKO Djely Kanny | A – COLY Ousmane | Correction vérifiée : une date impossible ne fait plus échouer le job. PR fusionnée dans main. |
| 2026-10-20 | Parties 1, 2 et 7 : DataIngestion, DataValidation, rapport qualité, tests (PR #2) | A - COLY Ousmane | B - CISSOKO Djely Kanny | Approuvée et fusionnée. DataValidationTest 7/7 OK ; DataIngestionTest avorte sous Windows (winutils.exe et hadoop.dll manquants), limite d'environnement et non bug de logique. Remarques non bloquantes : la validation du timestamp ne vérifie que la longueur (14) ; safeLoad n'attrape pas UnsatisfiedLinkError ; les tests dépendent des vrais fichiers de données. |
| 2026-10-21 | Parties 5 et 6 : SparkOptimizations, MainApp (PR #5) | C – Bamba Fall | A – COLY Ousmane | Testé fusionné avec main : 31 tests OK, `sbt run` OK, JAR assembly + spark-submit OK. Cache / persist / unpersist en place et libérés dans le `finally`, broadcast piloté par `enable-broadcast`, `shuffle.partitions` lu dans la config. MainApp : gestion d'erreurs globale avec arrêt de la SparkSession, durée de chaque étape, sorties CSV + Parquet complètes. Rien de bloquant. Suite : logs Spark réduits (log4j2.properties) pour une console lisible pendant la démo. |
| 2026-10-21 | Parties 1 et 7 : structure SBT, build.sbt, README, application.conf | A – COLY Ousmane | C – Bamba Fall| Aucun point bloquant : structure conforme, `.gitignore` complet, assembly justifié, valeurs par défaut dans `ConfigLoader`, README complet. Remarque : `SparkSessionBuilder` appelle `.master(ConfigLoader.sparkMaster)`, ce qui l'emporte sur `--master` de `spark-submit` (l'exemple YARN du README s'exécuterait en local). Suggestion : ne définir le master que s'il est explicitement configuré. |
| 2026-10-22 | Partie 2.5 (bonus) : countOrphans dans DataValidation, rapport qualité, tests (PR #6) | A - COLY Ousmane | B - CISSOKO Djely Kanny | Relue après fusion dans main. Comparaison faite sur les données brutes avec un left_anti sur les identifiants distincts, clés nulles ignorées, broadcast selon la config ; résultat vérifié par les tests (users=2, products=2, merchants=1). sbt test sur main : 26 tests OK. Remarque non bloquante : les 3 comptages relancent chacun la lecture des données brutes (pas de cache à ce stade). |
| 2026-10-22 | Parties 4.1, 4.2, 5.1, 5.2 et 6.1 : Analytics, SparkOptimizations, MainApp (PR #5) | C - Bamba Fall | B - CISSOKO Djely Kanny | Relue après fusion dans main ; sbt test : 26 tests OK (AnalyticsTest, SparkOptimizationsTest). KPI marchands sans les marchands inconnus, rangs par catégorie et par région, rétention par cohorte et cache libéré dans le finally : conformes. Remarques non bloquantes : MainApp attrape NonFatal, qui n'attrape pas UnsatisfiedLinkError (Windows sans hadoop.dll) ; un avertissement Spark « join hint not part of a join relation » apparaît dans le test de broadcastIfEnabled, sans effet. sbt run non exécuté sur mon poste (hadoop.dll manquant). |