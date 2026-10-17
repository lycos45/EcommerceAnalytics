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
- Temps passé : à compléter
- Difficultés :
  - à compléter

### Membre B
- Temps passé : à compléter
- Difficultés :
  - à compléter

### Membre C
- Temps passé : à compléter
- Difficultés :
  - à compléter

## Décisions techniques du groupe

1. **Spark 4 / Scala 2.13 / Java 17.**
   Spark 4 n'est plus publié pour Scala 2.12, donc on est passé en 2.13. Java 17 est la version
   LTS supportée par Spark 4 et celle installée sur nos postes.

2. **JAR construit avec sbt-assembly.**
   `sbt package` ne contient que nos classes, il manquerait Typesafe Config au lancement.
   Avec assembly on a un seul JAR autonome ; Spark reste en `provided` car il est fourni par spark-submit.

3. **Configuration avec Typesafe Config + valeurs par défaut.**
   Tous les paramètres sont dans application.conf et lus via `ConfigLoader`. Chaque lecture
   passe par `hasPath` pour retomber sur une valeur par défaut si la clé est absente.

4. **Champs numériques en `Option` dans les case classes.**
   Les fichiers contiennent des valeurs nulles : avec un `Double` simple, la conversion en
   `Dataset[T]` plante. `Option` permet de garder ces lignes et de les rejeter proprement à la validation.

5. **Stratégie de jointure** : à compléter par B (question 3.2).

6. **Format de sortie** : à compléter par C.

## Relectures croisées

| Date | Module relu | Auteur | Relecteur | Remarques |
|------|-------------|--------|-----------|-----------|
| | | | | |
