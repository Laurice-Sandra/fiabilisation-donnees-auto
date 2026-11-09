"""Génération du reporting HTML interactif (EF5).

Ce module assemble les données préparées par `src/preparation_rapport.py`
en graphiques Plotly (interactifs, aucun serveur requis : tout est embarqué
dans le fichier HTML final) puis en une page HTML unique. La palette de
couleurs s'inspire de celle de L'Olivier Assurance (demande explicite de
Laurice, relevée sur une capture d'écran de leur site, pas une charte
graphique officielle téléchargée). Le nom/logo affiché ("AutoFiable") est en
revanche original : les données traitées (freMTPL2, publiques) n'appartiennent
pas à L'Olivier Assurance, afficher leur nom aurait été trompeur.

Pas de tests automatiques sur ce module : il ne fait que du rendu visuel
(HTML/Plotly), qui ne se prête pas à une assertion pytest utile. Les calculs
qui l'alimentent (src/preparation_rapport.py, src/segmentation.py) sont eux
testés (ENF3).
"""

import json

import geopandas as gpd
import plotly.graph_objects as go
from pyspark.sql import DataFrame

from src.chargement import charger_contrats_sinistres
from src.jointure import joindre_contrats_sinistres
from src.preparation_rapport import (
    agreger_par_region,
    agreger_par_segment,
    calculer_kpis_globaux,
)
from src.segmentation import ajouter_classe
from src.telechargement import RACINE_PROJET, charger_parametres, telecharger_reference_geographique

# Palette de couleurs inspirée de L'Olivier Assurance (choix esthétique de
# Laurice, pas une charte graphique officielle téléchargée : à corriger si
# Laurice a les vraies valeurs).
COULEUR_VERT_OLIVE = "#3F4B23"
COULEUR_VERT_CLAIR = "#7A9A5B"
COULEUR_ORANGE = "#F05A28"
COULEUR_ORANGE_CLAIR = "#FFB092"
COULEUR_BLEU = "#4857D1"
COULEUR_BLEU_CLAIR = "#AEB6F2"
COULEUR_FOND = "#F2F3F8"
COULEUR_TEXTE = "#2B2F3A"

# Police commune a toute la page (CDN Google Fonts) et aux graphiques Plotly,
# pour que les deux se fondent visuellement au lieu de se juxtaposer.
POLICE = "Poppins, Arial, sans-serif"

# Dégradé utilisé sur les barres : plus lisible et moins "générique" qu'une
# couleur plate, et la couleur transmet aussi une info (plus la barre est
# haute, plus elle est foncée).
DEGRADE_ORANGE = [[0, COULEUR_ORANGE_CLAIR], [1, COULEUR_ORANGE]]
DEGRADE_BLEU = [[0, COULEUR_BLEU_CLAIR], [1, COULEUR_BLEU]]


def _mise_en_forme_commune(fig: go.Figure, titre: str) -> None:
    """Style Plotly partagé par tous les graphiques du rapport : même
    police que la page HTML, mêmes couleurs de fond, grille discrète.
    """
    fig.update_layout(
        title=dict(text=titre, font=dict(size=16, color=COULEUR_TEXTE, family=POLICE), x=0, xanchor="left"),
        font=dict(family=POLICE, color=COULEUR_TEXTE),
        plot_bgcolor="white", paper_bgcolor="white",
        bargap=0.35,
        margin=dict(t=110, l=60, r=40, b=50),
    )
    fig.update_xaxes(showgrid=False, linecolor="#DDD")
    fig.update_yaxes(showgrid=True, gridcolor="#EEE", zeroline=False)


def construire_df_analyse(spark, parametres: dict):
    """Reconstruit la base fiabilisée + les classes utilisées par le rapport
    (mêmes choix que `notebooks/analyse_descriptive.ipynb`, validés avec
    Laurice : DrivAge en 10 classes, BonusMalus en 5 — qui se réduisent
    naturellement à 3 car 56.7% des contrats sont au plancher légal).
    """
    df_contrats, df_sinistres = charger_contrats_sinistres(spark, parametres)

    cle = parametres["jointure"]["cle"]
    df_base = joindre_contrats_sinistres(df_contrats, df_sinistres, cle)

    df_base, ordre_drivage = ajouter_classe(df_base, "DrivAge", nb_classes=10)
    df_base, ordre_bonusmalus = ajouter_classe(df_base, "BonusMalus", nb_classes=5)

    return df_base, {"classe_DrivAge": ordre_drivage, "classe_BonusMalus": ordre_bonusmalus}


def figure_segment_contrats(df_analyse: DataFrame, ordres: dict) -> go.Figure:
    """Onglet Contrats : nb_contrats par classe_DrivAge, filtrable par
    classe_BonusMalus via un menu déroulant (une trace par classe BM,
    basculées en visible/invisible — pas de rechargement de page).
    """
    tableau = agreger_par_segment(df_analyse, "classe_DrivAge", "classe_BonusMalus")
    ordre_drivage = ordres["classe_DrivAge"]
    ordre_bm = ordres["classe_BonusMalus"]

    fig = go.Figure()
    # Trace 0 : total toutes classes BM confondues (vue par defaut). Le
    # degrade de couleur (pas un bloc plat) encode une 2e info (hauteur) et
    # evite le look "barres plates" trop generique.
    total = tableau.groupby("classe_DrivAge")["nb_contrats"].sum().reindex(ordre_drivage)
    fig.add_trace(go.Bar(
        x=ordre_drivage, y=total.values, name="Toutes classes BM",
        marker=dict(color=total.values, colorscale=DEGRADE_BLEU, line_width=0),
    ))

    # Une trace par classe BonusMalus, invisible par defaut
    for classe_bm in ordre_bm:
        sous_tableau = tableau[tableau["classe_BonusMalus"] == classe_bm].set_index("classe_DrivAge")
        y = sous_tableau["nb_contrats"].reindex(ordre_drivage).fillna(0)
        fig.add_trace(go.Bar(
            x=ordre_drivage, y=y.values, name=f"BonusMalus {classe_bm}", visible=False,
            marker=dict(color=y.values, colorscale=DEGRADE_ORANGE, line_width=0),
        ))

    boutons = [dict(label="Toutes classes BM", method="update",
                     args=[{"visible": [True] + [False] * len(ordre_bm)}])]
    for i, classe_bm in enumerate(ordre_bm):
        visible = [False] * (len(ordre_bm) + 1)
        visible[i + 1] = True
        boutons.append(dict(label=f"BonusMalus {classe_bm}", method="update", args=[{"visible": visible}]))

    fig.update_layout(
        xaxis_title="Tranche d'âge du conducteur", yaxis_title="Nombre de contrats",
        showlegend=False,
        updatemenus=[dict(
            buttons=boutons, x=1.0, xanchor="right", y=1.18, yanchor="top", active=0,
            bgcolor="white", bordercolor=COULEUR_BLEU, font=dict(family=POLICE),
        )],
    )
    _mise_en_forme_commune(fig, "Contrats par tranche d'âge du conducteur")
    return fig


def figure_declare_vs_reel(kpis: dict) -> go.Figure:
    """Onglet Sinistres : ClaimNb (déclaré) vs nb_sinistres_reel (coût
    connu), cf. échange sur l'hypothèse de Laurice dans analyse_descriptive.
    """
    valeurs = [kpis["claimnb_total"], kpis["nb_sinistres_reel_total"]]
    fig = go.Figure(go.Bar(
        x=["ClaimNb (déclaré)", "nb_sinistres_reel (coût connu)"],
        y=valeurs,
        marker=dict(color=[COULEUR_BLEU, COULEUR_ORANGE], line_width=0),
        text=[f"{v:,.0f}".replace(",", " ") for v in valeurs], textposition="outside",
    ))
    fig.update_layout(yaxis_title="Nombre de sinistres", showlegend=False)
    _mise_en_forme_commune(fig, "Sinistres déclarés vs sinistres à coût connu")
    return fig


def figure_frequence_expo(df_analyse: DataFrame, ordres: dict) -> go.Figure:
    """Onglet Sinistres : fréquence (ligne) et exposition (barres) par
    tranche d'âge du conducteur — reprend le graphique le plus parlant de
    `analyse_descriptive.ipynb` (effet jeune conducteur).
    """
    tableau = agreger_par_segment(df_analyse, "classe_DrivAge")
    ordre = ordres["classe_DrivAge"]
    tableau = tableau.set_index("classe_DrivAge").reindex(ordre).reset_index()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=tableau["classe_DrivAge"], y=tableau["exposition_totale"], name="Exposition totale",
        marker=dict(color=tableau["exposition_totale"], colorscale=[[0, "#EFF1FB"], [1, COULEUR_BLEU_CLAIR]], line_width=0),
        yaxis="y1",
    ))
    fig.add_trace(go.Scatter(
        x=tableau["classe_DrivAge"], y=tableau["frequence"], name="Fréquence",
        mode="lines+markers", line=dict(color=COULEUR_ORANGE, width=3),
        marker=dict(size=8, color=COULEUR_ORANGE), yaxis="y2",
    ))
    fig.update_layout(
        xaxis_title="Tranche d'âge du conducteur",
        yaxis=dict(title="Exposition totale"),
        yaxis2=dict(title="Fréquence", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", y=1.12, x=0),
    )
    _mise_en_forme_commune(fig, "Fréquence (nb sinistres / exposition) par tranche d'âge")
    return fig


def figure_carte_region(df_analyse: DataFrame, chemin_geojson) -> go.Figure:
    """Onglet Région : carte des contrats / exposition / sinistres / charge
    par région, avec un menu déroulant pour choisir la métrique affichée.

    Corrige le bug signalé par Laurice : les noms des régions (pas que les
    codes R..) sont maintenant affichés au survol ET directement sur la
    carte (étiquettes aux centroïdes, calculés avec geopandas).
    """
    agrege = agreger_par_region(df_analyse)
    with open(chemin_geojson, encoding="utf-8") as f:
        geojson = json.load(f)

    code_vers_nom = {f["properties"]["code"]: f["properties"]["nom"] for f in geojson["features"]}
    agrege["nom_region"] = agrege["code"].map(code_vers_nom)

    # Centroïdes pour les étiquettes de nom directement sur la carte
    # (reprojection en Lambert-93 pour un centroïde géométriquement correct,
    # puis retour en WGS84 pour les coordonnées lon/lat attendues par Plotly).
    regions_geo = gpd.read_file(chemin_geojson)
    centroides = regions_geo.to_crs(epsg=2154).geometry.centroid.to_crs(epsg=4326)
    regions_geo["lon"] = centroides.x
    regions_geo["lat"] = centroides.y
    regions_geo = regions_geo.merge(agrege, on="code", how="left")

    metriques = [
        ("nb_contrats", "Nombre de contrats", "{:,.0f}"),
        ("exposition_totale", "Exposition totale", "{:,.0f}"),
        ("nb_sinistres_reel", "Nombre de sinistres", "{:,.0f}"),
        ("montant_total_sinistres", "Charge totale (€)", "{:,.0f} €"),
    ]

    fig = go.Figure()
    for i, (colonne, titre, _fmt) in enumerate(metriques):
        fig.add_trace(go.Choropleth(
            geojson=geojson, locations=agrege["code"], z=agrege[colonne],
            featureidkey="properties.code",
            colorscale=[[0, "#FFF3EC"], [1, COULEUR_ORANGE]],
            marker_line_color="white", marker_line_width=1.2,
            visible=(i == 0), name=titre, colorbar_title=titre,
            customdata=agrege["nom_region"],
            hovertemplate="<b>%{customdata}</b><br>" + titre + " : %{z:,.0f}<extra></extra>",
        ))
    # Etiquettes de noms de region, visibles en permanence (pas liees au menu)
    fig.add_trace(go.Scattergeo(
        lon=regions_geo["lon"], lat=regions_geo["lat"], mode="text",
        text=regions_geo["nom"], textfont=dict(size=8, color=COULEUR_TEXTE, family=POLICE),
        showlegend=False, hoverinfo="skip",
    ))

    boutons = []
    for i, (colonne, titre, _fmt) in enumerate(metriques):
        visible = [False] * len(metriques) + [True]  # les etiquettes restent toujours visibles
        visible[i] = True
        boutons.append(dict(label=titre, method="update", args=[{"visible": visible}]))

    fig.update_layout(
        geo=dict(scope="europe", projection_type="mercator", showland=True, landcolor="#FAFAFA",
                  center=dict(lat=46.5, lon=2.5), lataxis_range=[41, 51], lonaxis_range=[-5, 10],
                  fitbounds=False),
        updatemenus=[dict(
            buttons=boutons, x=1.0, xanchor="right", y=1.08, yanchor="top", active=0,
            bgcolor="white", bordercolor=COULEUR_ORANGE, font=dict(family=POLICE),
        )],
        height=620,
    )
    _mise_en_forme_commune(fig, "Portefeuille par région")
    return fig


# Icônes (emoji, pas de dépendance réseau supplémentaire) associées à
# chaque KPI, pour un repérage visuel immédiat dans les cartes.
_ICONES_KPIS = {
    "Contrats": "📄", "Exposition totale": "⏱️", "Sinistres (coût connu)": "⚠️",
    "Charge totale": "💶", "Fréquence globale": "📈", "Coût moyen": "💰",
}


def construire_cartes_kpis(kpis: dict) -> str:
    """Petites cartes HTML (pas de Plotly ici : des chiffres bruts n'ont
    pas besoin d'un graphique, juste d'être lisibles d'un coup d'œil).

    Note : la "prime pure" globale (fréquence x coût moyen) a été retirée à
    la demande de Laurice — un seul chiffre agrégé sur tout le portefeuille
    n'a pas de sens actionnable en tarification (la prime pure n'est utile
    que comparée entre segments, pas en valeur absolue globale).
    """
    cartes = [
        ("Contrats", f"{kpis['nb_contrats']:,.0f}".replace(",", " ")),
        ("Exposition totale", f"{kpis['exposition_totale']:,.0f}".replace(",", " ")),
        ("Sinistres (coût connu)", f"{kpis['nb_sinistres_reel_total']:,.0f}".replace(",", " ")),
        ("Charge totale", f"{kpis['charge_totale']:,.0f} €".replace(",", " ")),
        ("Fréquence globale", f"{kpis['frequence_globale']:.1%}"),
        ("Coût moyen", f"{kpis['cout_moyen_global']:,.0f} €".replace(",", " ")),
    ]
    html_cartes = "".join(
        f'<div class="carte-kpi"><div class="carte-kpi-icone">{_ICONES_KPIS[label]}</div>'
        f'<div><div class="carte-kpi-valeur">{valeur}</div>'
        f'<div class="carte-kpi-label">{label}</div></div></div>'
        for label, valeur in cartes
    )
    return f'<div class="cartes-kpis">{html_cartes}</div>'


def construire_rapport_html(kpis: dict, figures_par_onglet: dict) -> str:
    """Assemble les figures Plotly et les cartes KPI en une seule page HTML.

    Navigation latérale (pas des onglets plats en haut) pour un rendu plus
    "dashboard" moderne ; transition en fondu au changement d'onglet.
    """
    premier_onglet = next(iter(figures_par_onglet))
    liens_nav = "".join(
        f'<a class="nav-lien{" actif" if nom == premier_onglet else ""}" '
        f'onclick="ouvrirOnglet(\'{nom}\')" id="bouton-{nom}">{nom}</a>'
        for nom in figures_par_onglet
    )

    premier_rendu_plotly = True
    onglets_contenu = ""
    for nom, figures in figures_par_onglet.items():
        divs = ""
        for fig in figures:
            divs += fig.to_html(full_html=False, include_plotlyjs=("cdn" if premier_rendu_plotly else False))
            premier_rendu_plotly = False
        style_affichage = "block" if nom == premier_onglet else "none"
        legende = (
            '<p class="legende-onglet">Anciennes régions françaises (avant le redécoupage de 2015).</p>'
            if nom == "Région" else ""
        )
        onglets_contenu += (
            f'<div class="onglet-contenu" id="onglet-{nom}" style="display:{style_affichage}">'
            f'<div class="graphe-carte">{divs}{legende}</div></div>'
        )

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8">
<title>Rapport de contrôle — fiabilisation-donnees-auto</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
* {{ box-sizing: border-box; }}
body {{ font-family: {POLICE}; margin: 0; background: {COULEUR_FOND}; color: {COULEUR_TEXTE}; display: flex; }}

/* Navigation laterale */
nav {{ width: 220px; min-height: 100vh; background: linear-gradient(180deg, {COULEUR_VERT_OLIVE}, #2B3419);
       color: white; padding: 28px 0; flex-shrink: 0; }}
nav .logo {{ font-size: 1.3em; font-weight: 700; padding: 0 24px 28px; display: flex; align-items: center; gap: 8px; }}
.nav-lien {{ display: block; padding: 14px 24px; cursor: pointer; color: #D9DEC8; font-weight: 500;
             border-left: 3px solid transparent; transition: all 0.2s ease; }}
.nav-lien:hover {{ background: rgba(255,255,255,0.06); color: white; }}
.nav-lien.actif {{ background: rgba(255,255,255,0.1); color: white; border-left-color: {COULEUR_ORANGE}; }}

/* Contenu principal */
main {{ flex: 1; padding: 28px 34px; }}
main h1 {{ margin: 0 0 20px; font-size: 1.3em; font-weight: 600; }}

.cartes-kpis {{ display: flex; flex-wrap: wrap; gap: 16px; margin-bottom: 28px; }}
.carte-kpi {{ background: white; border-radius: 12px; padding: 18px 22px; min-width: 170px;
              box-shadow: 0 2px 10px rgba(40,40,60,0.07); display: flex; align-items: center; gap: 14px;
              transition: transform 0.15s ease; }}
.carte-kpi:hover {{ transform: translateY(-2px); box-shadow: 0 6px 16px rgba(40,40,60,0.12); }}
.carte-kpi-icone {{ font-size: 1.6em; }}
.carte-kpi-valeur {{ font-size: 1.35em; font-weight: 700; color: {COULEUR_VERT_OLIVE}; line-height: 1.1; }}
.carte-kpi-label {{ color: #777; font-size: 0.8em; margin-top: 2px; }}

.onglet-contenu {{ animation: apparition 0.25s ease; }}
@keyframes apparition {{ from {{ opacity: 0; }} to {{ opacity: 1; }} }}
.graphe-carte {{ background: white; border-radius: 14px; padding: 10px 6px;
                  box-shadow: 0 2px 10px rgba(40,40,60,0.06); margin-bottom: 20px; }}
.legende-onglet {{ color: #888; font-size: 0.82em; text-align: center; margin: 0 0 10px; }}
</style>
</head>
<body>
<nav>
  <div class="logo">🛡️ AutoFiable</div>
  {liens_nav}
</nav>
<main>
<h1>Rapport de contrôle — fiabilisation-donnees-auto</h1>
{construire_cartes_kpis(kpis)}
{onglets_contenu}
</main>
<script>
function ouvrirOnglet(nom) {{
    document.querySelectorAll(".onglet-contenu").forEach(e => e.style.display = "none");
    document.querySelectorAll(".nav-lien").forEach(e => e.classList.remove("actif"));
    document.getElementById("onglet-" + nom).style.display = "block";
    document.getElementById("bouton-" + nom).classList.add("actif");
    window.dispatchEvent(new Event("resize"));
}}
</script>
</body>
</html>"""


def generer_rapport(spark, parametres: dict | None = None) -> str:
    """Point d'entrée : construit la base, les figures, écrit le HTML final
    à l'emplacement défini dans config/parametres.yaml (sorties.rapport_controle).
    Renvoie le chemin du fichier écrit.
    """
    parametres = parametres or charger_parametres()
    df_analyse, ordres = construire_df_analyse(spark, parametres)
    df_analyse = df_analyse.cache()
    df_analyse.count()

    kpis = calculer_kpis_globaux(df_analyse)
    chemin_geojson = telecharger_reference_geographique(parametres)

    figures_par_onglet = {
        "Contrats": [figure_segment_contrats(df_analyse, ordres)],
        "Sinistres": [figure_declare_vs_reel(kpis), figure_frequence_expo(df_analyse, ordres)],
        "Région": [figure_carte_region(df_analyse, chemin_geojson)],
    }

    html = construire_rapport_html(kpis, figures_par_onglet)

    chemin_sortie = RACINE_PROJET / parametres["sorties"]["rapport_controle"]
    chemin_sortie.parent.mkdir(parents=True, exist_ok=True)
    chemin_sortie.write_text(html, encoding="utf-8")
    return str(chemin_sortie)

