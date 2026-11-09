# fiabilisation-donnees-auto
Pipeline de fiabilisation contrats × sinistres auto (freMTPL2)

## Pitch

Construire une base contrats × sinistres fiable à partir des données
publiques freMTPL2 (OpenML), avec des contrôles tracés et explicables
(doublons, clés orphelines, cohérence des sinistres, bornes de l'exposition),
pour servir de socle aux projets de tarification suivants (GLM fréquence et
coût moyen).

## Installation

```
pip install -e .
```

Installe le projet comme package local (`src/`), ce qui rend les imports
(`from src.xxx import ...`) indépendants du répertoire de lancement.

## Lancer le traitement

```
python -m src.pipeline
```

Une seule commande (EF1) : télécharge les données, applique les contrôles,
fait la jointure, écrit la base fiabilisée + le journal d'anomalies +
le reporting HTML. Détail de chaque étape et des livrables dans
[docs/note_procedure.md](./docs/note_procedure.md).

## Usage de l'IA

Ce projet a été construit avec l'aide d'un assistant IA (GitHub Copilot).
Le détail de ce qui a été proposé par l'IA, ce qui a été décidé par Laurice,
et comment les deux sont tracés dans le code et le notebook, est documenté
dans [AI_USAGE.md](./AI_USAGE.md).
