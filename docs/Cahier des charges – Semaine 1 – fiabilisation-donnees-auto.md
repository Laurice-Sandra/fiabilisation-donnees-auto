# Cahier des charges – Semaine 1 – fiabilisation-donnees-auto

Oct 28, 2026 · @Laurice

Obtenir une base contrats × sinistres auto fiable, contrôlée et traçable, et prouver qu'aucun contrat, sinistre ni euro n'a été perdu ou dupliqué.

## 1. Contexte et problème

En assurance non-vie, contrats et sinistres viennent de systèmes différents et sont rapprochés pour tous les travaux actuariels (tarification, provisionnement, pilotage). Une jointure ou un filtre mal posé fausse silencieusement tous les chiffres en aval, même avec du code produit par l'IA. \[Laurice\]

Le projet reproduit ce problème sur le portefeuille auto public freMTPL2 (table contrats et table sinistres, via OpenML).

**Problème à résoudre** : produire une base contrats × sinistres fiable, et le **prouver** par des contrôles tracés.

## 2. Objectifs et utilisateurs

1. Construire la base commune aux projets suivants (GLM fréquence et coût moyen, semaines 3 et 4).
2. Détecter, classer et tracer automatiquement les anomalies.
3. Produire un reporting HTML lisible par un non-développeur.
4. Rendre le traitement exécutable par une autre personne, sans l'auteure.

| Utilisateur | Besoin |
| --- | --- |
| Actuaire (tarification, inventaire) | Base fiable pour modéliser |
| Responsable des données, contrôle interne | Preuve des contrôles, piste d'audit |
| Personne qui reprend le traitement | Note de procédure claire |

## 3. Périmètre

**Inclus**

- Téléchargement des données publiques
- Contrôles : doublons, clés orphelines, cohérence des nombres de sinistres, bornes de l'exposition, volumétries avant/après, réconciliation des totaux
- Jointure contrats × sinistres
- Journal d'anomalies (CSV) et reporting HTML

**Exclus**

- Modélisation tarifaire (semaines 3 et 4)
- Correction silencieuse des valeurs : on signale et on documente, on ne « répare » pas sans règle écrite
- Tableau de bord Power BI (projet ultérieur)

## 4. Exigences

**Fonctionnelles** (ce que le traitement fait)

| N° | Exigence |
| --- | --- |
| EF1 | Le traitement complet se lance en une seule commande |
| EF2 | Chaque contrôle a un niveau : bloquant (arrêt) ou alerte (continue + journal) |
| EF3 | Le journal liste chaque anomalie : contrôle, identifiant, valeur observée, niveau, date d'exécution |
| EF4 | Volumétries et totaux comparés avant et après chaque étape |
| EF5 | Reporting HTML des contrôles — contenu et forme décidés par Laurice \[Laurice\] |
| EF6 | Totaux recalculés indépendamment en pandas et comparés au pipeline |

**Non fonctionnelles** (comment il doit être construit)

| N° | Exigence |
| --- | --- |
| ENF1 | Aucune valeur en dur : chemins, clés, seuils dans `config/parametres.yaml` |
| ENF2 | Reproductible : un clone donne le même résultat en moins de 10 minutes |
| ENF3 | Tests automatiques (pytest) sur les contrôles |
| ENF4 | Données non publiées dans le dépôt |
| ENF5 | Usage de l'IA tracé dans `AI_USAGE.md` et le README |
| ENF6 | Pipeline en PySpark ; contrôle indépendant en pandas |

## 5. Livrables et critères d'acceptation

| Livrable | Emplacement |
| --- | --- |
| Code et tests | `src/`, `tests/` |
| Paramètres | `config/parametres.yaml` |
| Base fiabilisée (non publiée) | `data/sortie/` |
| Journal d'anomalies | `reports/journal_anomalies.csv` |
| Reporting HTML | `reports/rapport_controle.html` |
| Note de procédure | `docs/note_procedure.md` |
| README (Usage de l'IA, pitch) | racine du dépôt |

Le projet est terminé quand :

- [ ] la commande unique tourne sans erreur sur un clone propre
- [ ] totaux du pipeline = totaux du recalcul pandas (ou écart sous la tolérance du YAML)
- [ ] chaque anomalie est soit traitée par une règle écrite, soit journalisée
- [ ] tous les tests passent
- [ ] le reporting HTML est compris par un non-développeur
- [ ] il ne reste aucun « à définir » dans le YAML

## 6. Contraintes et décisions ouvertes

**Contraintes** : données publiques uniquement (freMTPL2, OpenML 41214 et 41215) ; Windows, Anaconda (`actuary`), VS Code, GitHub Copilot ; 5 jours × 3 h.

**Décisions ouvertes** — à trancher après l'exploration du J1, puis à reporter dans le YAML :

- [ ] Type de jointure (et pourquoi)
- [ ] Seuils minimum et maximum de l'exposition
- [ ] Niveau bloquant ou alerte de chaque contrôle
- [ ] Tolérance de réconciliation des totaux
- [ ] Contenu du reporting HTML \[Laurice\]
