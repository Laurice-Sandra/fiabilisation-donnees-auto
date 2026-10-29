# Usage de l'IA dans ce projet

Conformément à l'exigence ENF5 du cahier des charges, ce document trace
l'usage de l'IA (GitHub Copilot, agent conversationnel) sur ce projet.

## Principe suivi

L'IA n'a jamais pris de décision seule sur les points qui engagent le métier
(seuils de contrôle, niveau bloquant/alerte, type de jointure, traitement ou
non d'une anomalie) : elle a proposé, expliqué, et Laurice a tranché. Les
décisions actées sont tracées dans `config/parametres.yaml` (commentaires) et
dans `notebooks/data_analysis.ipynb` (cellules markdown taguées **Origine**).

## Ce que l'IA a fait

- **Diagnostic et correction de bugs** : erreurs d'import de données (chemin
  relatif dépendant du répertoire de travail), erreurs Spark (ANSI cast sur
  colonnes vides, erreurs de colonnes inexistantes).
- **Mise en place de l'architecture reproductible** : package `src/`
  installable (`pyproject.toml`, `pip install -e .`), pour que
  `from src.xxx import ...` fonctionne quel que soit le répertoire de lancement
  (ENF2).
- **Écriture de code Spark** : `src/telechargement.py`, `src/controles.py`,
  `src/jointure.py`, et les tests associés (`tests/`), à partir de règles
  métier discutées avec Laurice.
- **Propositions de contrôles de plausibilité** hors périmètre strict du
  cahier des charges (ex : bornes du `BonusMalus`, du `VehAge`, etc.),
  explicitement présentées comme des idées à valider, pas des vérités.
- **Vérification empirique d'hypothèses** formulées par Laurice sur les
  données (ex : la relation entre `ClaimNb` et `nb_sinistres_reel`), en
  recalculant sur l'ensemble du jeu de données plutôt que sur un seul exemple.
- **Explications pédagogiques** sur le domaine assurantiel (bonus-malus,
  Area/Density) quand demandées, toujours accompagnées d'une vérification
  sur les vraies données plutôt qu'une réponse uniquement théorique.

## Ce que Laurice a décidé seule ou tranché après proposition de l'IA

- Le type de jointure (LEFT) et comment rendre la clé sinistres unique avant
  la jointure (agrégation par somme et comptage).
- Les niveaux bloquant/alerte de chaque contrôle.
- Le traitement (ou l'absence de traitement) de chaque anomalie : ex. les
  lignes de sinistres identiques ne sont pas supprimées, les cas
  `jeune_au_plancher` et `BonusMalus` non multiple de 5 ne sont pas corrigés.
- Les seuils de plausibilité (ex. `VehAge`, `ClaimNb`, `ClaimAmount`), corrigés
  par Laurice après une première proposition erronée de l'IA sur le sens de
  certaines bornes (ex. un montant de sinistre négatif n'est pas une anomalie :
  cas de recours/subrogation).

## Traçabilité dans le notebook

Chaque cellule markdown du notebook qui contient une hypothèse, une
interprétation ou une idée de contrôle est taguée en première ligne :
- **Origine : Laurice** — idée, hypothèse ou décision de Laurice.
- **Origine : IA** — proposition de l'IA (contrôle, structure de code).
- **Origine : Laurice + IA** — décision prise ensemble après discussion.

## Limites connues

L'IA n'a pas pu exécuter certaines cellules du notebook directement (erreur
d'infrastructure récurrente de l'outil d'exécution). Toute la logique Spark a
donc été validée via des scripts Python autonomes exécutant le même code sur
les vraies données, puis Laurice a réexécuté les cellules elle-même dans son
propre kernel VS Code pour confirmation.
