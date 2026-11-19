from pathlib import Path
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Détection de Fraude Bancaire — PySpark ML",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Chemins des exports générés par le notebook
RES_PATH = Path("outputs/resultats_modeles.csv")
HEATMAP_PATH = Path("outputs/heatmap_fraude.csv")
CORR_PATH = Path("outputs/correlation.csv")

# --- BARRE LATÉRALE (Contexte & état du pipeline) ---
with st.sidebar:
    st.title("🛡️ Fraude Bancaire")
    st.caption("Pipeline Hadoop / HDFS • PySpark MLlib • Streamlit")
    st.divider()

    st.markdown("**Jeu de données**")
    st.markdown(
        "- **Source :** Sparkov (`fraudTest.csv`)\n"
        "- **Volume :** ~555 719 transactions\n"
        "- **Taux de fraude :** ~0,39 %\n"
        "- **Profilage :** *Window functions* (passé uniquement) + anonymisation"
    )
    st.divider()

    st.markdown("**État des fichiers (`outputs/`)**")
    for label, path in [
        ("Résultats modèles", RES_PATH),
        ("Matrice de corrélation", CORR_PATH),
        ("Carte thermique (Heatmap)", HEATMAP_PATH),
    ]:
        if path.exists():
            st.markdown(f"✅ `{path.name}`")
        else:
            st.markdown(f"⚠️ `{path.name}` *(manquant)*")

    st.divider()
    if st.button("🔄 Actualiser les données", use_container_width=True):
        st.rerun()

# --- EN-TÊTE PRINCIPAL ---
st.title("Détection de Fraude Bancaire — Tableau de Bord ML")
st.caption(
    "Évaluation comparative des algorithmes sur données fortement déséquilibrées "
    "et analyse exploratoire des signaux comportementaux."
)

if not RES_PATH.exists():
    st.error(
        f"Fichier introuvable : `{RES_PATH}`. "
        "Exécutez d'abord la cellule **4.6** du notebook PySpark pour générer les résultats."
    )
    st.stop()

df = pd.read_csv(RES_PATH)
colonnes_metriques = [c for c in ["AUPRC", "AUROC", "F1_Fraude"] if c in df.columns]
has_cm_cols = {"Fraudes_detectees", "Fraudes_manquees", "Fausses_alarmes"}.issubset(df.columns)

# Alerte utile si le PMC/MLP n'est pas encore présent dans le CSV
has_mlp = df["modele"].str.contains("MLP|Perceptron|PMC", case=False, na=False).any()
if not has_mlp:
    st.warning(
        "**Perceptron Multicouches (MLP / PMC) absent de `outputs/resultats_modeles.csv`.** "
        "Exécutez la cellule **4.6 unifiée** (contenant les 3 modèles) dans votre notebook, "
        "puis cliquez sur **🔄 Actualiser les données** dans la barre latérale."
    )

# --- KPIs GLOBAUX (Meilleur modèle selon l'AUPRC) ---
best_row = df.loc[df["AUPRC"].idxmax()] if "AUPRC" in df.columns else df.iloc[0]

kpi_cols = st.columns(4)
with kpi_cols[0]:
    with st.container(border=True):
        st.metric("Meilleur modèle (AUPRC)", str(best_row["modele"]))
with kpi_cols[1]:
    with st.container(border=True):
        st.metric("Meilleur AUPRC", f"{best_row['AUPRC']:.4f}" if "AUPRC" in df.columns else "N/A")
with kpi_cols[2]:
    with st.container(border=True):
        st.metric("AUROC associé", f"{best_row['AUROC']:.4f}" if "AUROC" in df.columns else "N/A")
with kpi_cols[3]:
    with st.container(border=True):
        if "F1_Fraude" in df.columns:
            st.metric("F1 Fraude", f"{best_row['F1_Fraude']:.4f}")
        else:
            st.metric("Modèles évalués", len(df))

st.write("")

# --- NAVIGATION PAR ONGLETS ---
tab_modeles, tab_heatmap, tab_corr = st.tabs([
    "📊 Modèles & Performance",
    "🔥 Carte Thermique (Catégorie × Heure)",
    "🔗 Corrélations & Profilage",
])

# =====================================================================
# ONGLET 1 : MODÈLES & PERFORMANCE
# =====================================================================
with tab_modeles:
    # Cartes individuelles par modèle pour lecture rapide
    st.subheader("Synthèse par modèle")
    model_cols = st.columns(len(df))
    for ui_col, (_, row) in zip(model_cols, df.iterrows()):
        is_best = row["modele"] == best_row["modele"]
        badge = " 🏆" if is_best else ""
        with ui_col:
            with st.container(border=True):
                st.markdown(f"#### {row['modele']}{badge}")
                c1, c2 = st.columns(2)
                if "AUPRC" in df.columns:
                    c1.metric("AUPRC", f"{row['AUPRC']:.4f}")
                if "AUROC" in df.columns:
                    c2.metric("AUROC", f"{row['AUROC']:.4f}")
                if "F1_Fraude" in df.columns:
                    st.caption(f"**F1 Fraude :** `{row['F1_Fraude']:.4f}`")
                if has_cm_cols:
                    total_f = int(row["Fraudes_detectees"] + row["Fraudes_manquees"])
                    st.caption(
                        f"**Fraudes détectées :** `{int(row['Fraudes_detectees'])} / {total_f}` "
                        f"({row['Rappel_%']:.1f} %)" if "Rappel_%" in df.columns
                        else f"**Fraudes détectées :** `{int(row['Fraudes_detectees'])} / {total_f}`"
                    )
                    st.caption(f"**Fausses alarmes :** `{int(row['Fausses_alarmes']):,}`")

    st.divider()

    # Graphiques comparatifs côte à côte
    col_g1, col_g2 = st.columns(2)
    with col_g1:
        st.subheader("Métriques de classification")
        st.bar_chart(df.set_index("modele")[colonnes_metriques], height=320)
        st.caption("L'AUPRC est la métrique prioritaire face au déséquilibre (0,39 % de fraudes).")

    with col_g2:
        if has_cm_cols:
            st.subheader("Impact opérationnel (Fausses alarmes)")
            st.bar_chart(df.set_index("modele")[["Fausses_alarmes"]], height=320, color="#dc3545")
            st.caption("Plus ce chiffre est bas, moins de clients légitimes voient leur carte bloquée à tort.")
        else:
            st.subheader("Tableau détaillé")
            st.dataframe(
                df.style.format({c: "{:.4f}" for c in colonnes_metriques}),
                use_container_width=True,
                hide_index=True,
            )

    # Tableau complet détaillé
    if has_cm_cols:
        st.subheader("Tableau comparatif complet")
        format_dict = {c: "{:.4f}" for c in colonnes_metriques}
        for c_int in ["Fraudes_detectees", "Fraudes_manquees", "Fausses_alarmes"]:
            format_dict[c_int] = "{:,.0f}"
        for c_pct in ["Rappel_%", "Precision_%"]:
            if c_pct in df.columns:
                format_dict[c_pct] = "{:.2f}%"

        st.dataframe(
            df.style.format(format_dict),
            use_container_width=True,
            hide_index=True,
        )

    # Diagnostic dynamique
    diag = (
        f"**Conclusion :** **{best_row['modele']}** obtient le meilleur compromis global "
        f"(**AUPRC = {best_row['AUPRC']:.4f}**, **AUROC = {best_row['AUROC']:.4f}**, "
        f"**F1 = {best_row['F1_Fraude']:.4f}**)."
    )
    if has_cm_cols:
        tot_f = int(best_row["Fraudes_detectees"] + best_row["Fraudes_manquees"])
        diag += (
            f" Il intercepte **{int(best_row['Fraudes_detectees'])} fraudes sur {tot_f}** "
            f"tout en limitant les fausses alertes à **{int(best_row['Fausses_alarmes']):,}** transactions."
        )
    st.success(diag)

# =====================================================================
# ONGLET 2 : CARTE THERMIQUE (HEATMAP)
# =====================================================================
with tab_heatmap:
    st.subheader("Taux de fraude (%) par catégorie de marchand et heure de la journée")

    if HEATMAP_PATH.exists():
        hm = pd.read_csv(HEATMAP_PATH).set_index("categorie_marchand")
        hm_num = hm.apply(pd.to_numeric, errors="coerce")

        # Mini-indicateurs calculés automatiquement depuis le heatmap
        stacked = hm_num.stack()
        peak_cat, peak_hour = stacked.idxmax()
        peak_val = stacked.max()

        heures_nuit = [c for c in hm_num.columns if str(c).isdigit() and (int(c) >= 22 or int(c) <= 3)]
        heures_jour = [c for c in hm_num.columns if str(c).isdigit() and (4 <= int(c) <= 21)]
        moy_nuit = hm_num[heures_nuit].to_numpy().mean() if heures_nuit else 0.0
        moy_jour = hm_num[heures_jour].to_numpy().mean() if heures_jour else 0.0

        h1, h2, h3 = st.columns(3)
        with h1:
            with st.container(border=True):
                st.metric("Pic de fraude détecté", f"{peak_val:.2f}%", f"{peak_cat} à {peak_hour}h")
        with h2:
            with st.container(border=True):
                st.metric("Taux moyen nocturne (22h–03h)", f"{moy_nuit:.2f}%")
        with h3:
            with st.container(border=True):
                st.metric("Taux moyen en journée (04h–21h)", f"{moy_jour:.2f}%")

        st.write("")

        def color_heatmap(data):
            max_val = data.to_numpy().max()
            if max_val == 0:
                return pd.DataFrame("", index=data.index, columns=data.columns)

            def style_cell(v):
                if pd.isna(v) or v == 0:
                    return ""
                ratio = v / max_val
                alpha = (ratio ** 0.5) * 0.85
                if ratio > 0.35:
                    return f"background-color: rgba(220, 53, 69, {alpha:.2f}); color: white;"
                return f"background-color: rgba(220, 53, 69, {alpha:.2f});"

            return data.apply(lambda col: col.map(style_cell))

        st.dataframe(
            hm_num.style.format("{:.2f}%").apply(color_heatmap, axis=None),
            use_container_width=True,
            height=520,
        )
        st.info(
            "**Interprétation :** La fraude se concentre fortement sur les créneaux nocturnes (22h–03h) "
            "et sur quelques catégories précises (`grocery_pos`, `shopping_net`, `misc_net`). "
            "Cette interaction non linéaire explique pourquoi les arbres de décision et le MLP surclassent la régression logistique."
        )
    else:
        st.info("Exécutez l'Étape **4.3** du notebook pour générer `outputs/heatmap_fraude.csv`.")

# =====================================================================
# ONGLET 3 : CORRÉLATIONS & PROFILAGE
# =====================================================================
with tab_corr:
    st.subheader("Matrice de corrélation de Pearson (Variables numériques & profilage)")

    if CORR_PATH.exists():
        corr_df = pd.read_csv(CORR_PATH, index_col=0)

        # Insights automatiques sur la matrice
        c_col1, c_col2 = st.columns(2)
        if "est_fraude" in corr_df.columns:
            corr_cible = corr_df["est_fraude"].drop("est_fraude", errors="ignore")
            top_feat = corr_cible.abs().idxmax()
            top_val = corr_cible[top_feat]
            with c_col1:
                with st.container(border=True):
                    st.metric(
                        "Plus forte corrélation linéaire avec `est_fraude`",
                        f"{top_val:+.4f}",
                        f"Variable : {top_feat}",
                    )

        # Détection automatique de multicolinéarité (> 0.85)
        paires_fortes = []
        cols_list = list(corr_df.columns)
        for i in range(len(cols_list)):
            for j in range(i + 1, len(cols_list)):
                val = corr_df.iloc[i, j]
                if pd.notna(val) and abs(val) >= 0.85:
                    paires_fortes.append((cols_list[i], cols_list[j], val))

        with c_col2:
            with st.container(border=True):
                if paires_fortes:
                    f1, f2, v = paires_fortes[0]
                    st.metric("Multicolinéarité détectée (|r| ≥ 0,85)", f"{v:.4f}", f"{f1} ↔ {f2}")
                else:
                    st.metric("Multicolinéarité forte (|r| ≥ 0,85)", "Aucune", "Variables indépendantes")

        st.write("")

        def color_corr(val):
            if pd.isna(val):
                return ""
            alpha = min(abs(val) * 0.85, 0.85)
            color_rgb = "220, 53, 69" if val >= 0 else "13, 110, 253"
            if abs(val) > 0.45:
                return f"background-color: rgba({color_rgb}, {alpha:.2f}); color: white;"
            return f"background-color: rgba({color_rgb}, {alpha:.2f});"

        st.dataframe(
            corr_df.style.format("{:.4f}").map(color_corr),
            use_container_width=True,
            height=460,
        )

        st.info(
            "**Pourquoi `heure` affiche une corrélation proche de 0 alors qu'elle est déterminante ?** "
            "Pearson ne mesure que les relations linéaires. Or, la fraude suit une courbe en **U** selon l'heure "
            "(pics à 0h–3h et 22h–23h) : les extrêmes s'annulent mathématiquement pour Pearson, mais le profilage "
            "(`ecart_heure_habituelle`), le Heatmap et les modèles non linéaires (RandomForest, MLP) captent pleinement ce signal."
        )
    else:
        st.info("Exécutez l'Étape **4.5** du notebook pour générer `outputs/correlation.csv`.")