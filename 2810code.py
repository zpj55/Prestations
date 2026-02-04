import os
import tempfile
import duckdb
import polars as pl
import pandas as pd
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="MUTELIS Analyses Prestations vTest JP — 2 fichiers", layout="wide")

# =============================================
# DB + IO
# =============================================
@st.cache_resource
def get_db():
    con = duckdb.connect()
    has_excel = False
    try:
        con.execute("PRAGMA threads=auto;")
        con.execute("INSTALL excel; LOAD excel;")
        has_excel = True
    except Exception:
        has_excel = False
    return con, has_excel

con, HAS_EXCEL = get_db()

@st.cache_data
def save_file(_data: bytes, name: str) -> str:
    tmpdir = tempfile.mkdtemp(prefix="mutelis_")
    path = os.path.join(tmpdir, name)
    with open(path, "wb") as f:
        f.write(_data)
    return path

@st.cache_data
def get_headers(path: str, extension: str, has_excel: bool):
    safe = path.replace("'", "''")
    if extension == ".csv":
        q = f"DESCRIBE SELECT * FROM read_csv_auto('{safe}', header=1, all_varchar=1)"
        return [r[0] for r in con.execute(q).fetchall()]
    if has_excel:
        q = f"DESCRIBE SELECT * FROM read_excel('{safe}')"
        return [r[0] for r in con.execute(q).fetchall()]
    df0 = pl.read_excel(path, read_options={"n_rows": 0})
    return df0.columns

def idx(hdrs, name):
    try:
        return 1 + hdrs.index(name)
    except ValueError:
        return 0

def format_euro(x: float) -> str:
    try:
        return f"{x:,.0f} €".replace(",", " ")
    except Exception:
        return "—"

def compute_impact(df: pd.DataFrame) -> float:
    if df is None or df.empty:
        return 0.0
    if "WM_MONT_REMB" in df.columns:
        return pd.to_numeric(df["WM_MONT_REMB"], errors="coerce").fillna(0).sum()
    if "VAL_ABS" in df.columns:
        return pd.to_numeric(df["VAL_ABS"], errors="coerce").fillna(0).sum()
    return 0.0

# =============================================
# STATE (isoler les deux fichiers)
# =============================================

# ---- SENTINELLE INIT ----
if "INIT_DONE" not in st.session_state:
    st.session_state["INIT_DONE"] = True
    DEFAULTS = {
        # Prestations
        "prest_path": None, "prest_ext": None, "prest_headers": [],
        "prest_mapped": False, "prest_map": {}, "prest_val_abs_src": None, "prest_limit": 5000,
        # Radiés
        "rad_path": None, "rad_ext": None, "rad_headers": [],
        "rad_mapped": False, "rad_map": {}, "rad_limit": 2000,
        # UX
        "flash_ok": False,
    }
    st.session_state.update(DEFAULTS)




DEFAULTS = {
    # Prestations
    "prest_path": None,
    "prest_ext": None,
    "prest_headers": [],
    "prest_mapped": False,
    "prest_map": {},
    "prest_val_abs_src": None,
    "prest_limit": 5000,
    # Radiés
    "rad_path": None,
    "rad_ext": None,
    "rad_headers": [],
    "rad_mapped": False,
    "rad_map": {},
    "rad_limit": 2000,
    # UX
    "flash_ok": False,
}


# =============================================
# HELPERS SQL
# =============================================
def select_clause(mapping: dict, val_abs_src: str) -> str:
    picked = []
    seen = set()
    for exp, act in mapping.items():
        if act and exp not in seen:
            picked.append(f'"{act}" AS "{exp}"')
            seen.add(exp)
    picked.append(f'ABS(TRY_CAST("{val_abs_src}" AS DOUBLE)) AS "VAL_ABS"')
    return ", ".join(picked)

def csv_src(path: str) -> str:
    safe = path.replace("'", "''")
    return f"read_csv_auto('{safe}', header=1, all_varchar=1, parallel=true)"

def excel_src(path: str) -> str:
    safe = path.replace("'", "''")
    return f"read_excel('{safe}')"

def make_base_sql(path: str, ext: str, mapping: dict, val_abs_src: str) -> str:
    select_sql = select_clause(mapping, val_abs_src)
    if not path or not ext or not mapping or not val_abs_src:
        return ""
    if ext == ".csv":
        return f"WITH Prest AS (SELECT {select_sql} FROM {csv_src(path)}) "
    if HAS_EXCEL:
        return f"WITH Prest AS (SELECT {select_sql} FROM {excel_src(path)}) "
    # fallback polars
    needed_cols = sorted(set([c for c in mapping.values() if c] + [val_abs_src]))
    df_xl = pl.read_excel(path, columns=needed_cols)
    df_xl = df_xl.with_columns(pl.col(val_abs_src).cast(pl.Float64, strict=False).abs().alias("VAL_ABS"))
    rename_map = {v: k for k, v in mapping.items() if v}
    df_xl = df_xl.rename(rename_map)
    con.register("Prest_df", df_xl)
    return "WITH Prest AS (SELECT * FROM Prest_df) "

# =============================================
# UI HEADER
# =============================================
st.title("MUTELIS Analyses Prestations vTest JP — 2 fichiers")
st.caption("Analyse automatisée des prestations et radiations : détection des incohérences, doublons et schémas suspects à partir des fichiers fournis. Les résultats permettent d’identifier les anomalies de remboursement, les adhérents à risque et les tiers présentant des comportements atypiques.")

tabs = st.tabs(["1) Prestations", "2) Radiés", "3) Analyses & Graphs"])

# =============================================
# TAB 1 — PRESTATIONS
# =============================================
with tabs[0]:
    st.subheader("📄 Fichier Prestations")
    up = st.file_uploader("Fichier Prestations (xlsx, xls, csv)", type=["xlsx", "xls", "csv"], key="up_prest")
    if up:
        st.session_state.prest_path = save_file(up.getvalue(), up.name)
        st.session_state.prest_ext = os.path.splitext(st.session_state.prest_path)[1].lower()
        # NE PAS toucher à rad_* ici.
        st.session_state.prest_mapped = False  # on revalide seulement ce fichier

    if st.session_state.prest_path:
        # entêtes
        try:
            st.session_state.prest_headers = get_headers(st.session_state.prest_path, st.session_state.prest_ext, HAS_EXCEL)
        except Exception as e:
            st.error("Impossible de lire les entêtes du fichier Prestations :")
            st.exception(e)
            st.session_state.prest_headers = []

        headers = st.session_state.prest_headers

        EXPECTED = [
            "NUM_ADH", "NOM", "PRENOM", "COMP_GARA_CODE", "WM_ACTE_RC",
            "RO_DATE_SOINS_DEB", "NUM_DEC", "REGLRC_REG_RC", "WM_MONT_REMB",
            "DESTRC_CODE", "DESTRC_TITULAIRE", "DESTRC_IBAN"
        ]

        with st.expander("⚙️ Mapping Prestations", expanded=not st.session_state.prest_mapped):
            if st.session_state.prest_mapped:
                c1, c2 = st.columns([1, 3])
                with c1:
                    if st.button("♻️ Réinitialiser le mapping Prestations"):
                        st.session_state.prest_mapped = False
                        st.session_state.prest_map = {}
                        st.session_state.prest_val_abs_src = None
                with c2:
                    st.info("Mapping Prestations validé. Modifiez puis revalidez si besoin.")

            if not st.session_state.prest_mapped and headers:
                defaults = st.session_state.prest_map or {}
                with st.form("map_form_prest"):
                    col1, col2 = st.columns([3, 2])
                    with col1:
                        mapping = {}
                        for col in EXPECTED:
                            default_index = 0
                            if defaults.get(col) and defaults[col] in headers:
                                default_index = 1 + headers.index(defaults[col])
                            else:
                                default_index = idx(headers, col)
                            mapping[col] = st.selectbox(f"{col} ⇢", [""] + headers, index=default_index, key=f"map_pre_{col}")
                    with col2:
                        # Par défaut : WM_MONT_REMB si présent
                        val_abs_default = defaults.get("VAL_ABS_SRC") or ("WM_MONT_REMB" if "WM_MONT_REMB" in headers else headers[0])
                        val_abs_src = st.selectbox("Colonne pour VAL_ABS (ABS)", headers,
                                                   index=(headers.index(val_abs_default) if val_abs_default in headers else 0),
                                                   key="prest_valabs_src")
                        limit_rows = st.number_input("Limite lignes affichées", min_value=200, max_value=100_000,
                                                     value=st.session_state.prest_limit, step=500, key="prest_limit_in")

                    submitted = st.form_submit_button("✅ Valider mapping Prestations")
                    if submitted:
                        required = ["NUM_ADH","NOM","PRENOM","COMP_GARA_CODE","WM_ACTE_RC",
                                    "RO_DATE_SOINS_DEB","NUM_DEC","REGLRC_REG_RC","WM_MONT_REMB"]
                        missing = [r for r in required if not mapping.get(r)]
                        if missing:
                            st.error("Colonnes obligatoires manquantes : " + ", ".join(missing))
                        else:
                            with st.spinner("Préparation Prestations…"):
                                st.session_state.prest_map = mapping
                                st.session_state.prest_val_abs_src = val_abs_src
                                st.session_state.prest_limit = int(limit_rows)
                                st.session_state.prest_mapped = True
                                st.session_state.flash_ok = True

    else:
        st.info("Déposez le fichier Prestations, puis mappez. (Cela n’efface pas le fichier Radiés.)")

# =============================================
# TAB 2 — RADIES
# =============================================
with tabs[1]:
    st.subheader("📋 Fichier : Liste des adhérents radiés")
    up_r = st.file_uploader("Fichier Radiés (xlsx, xls, csv)", type=["xlsx", "xls", "csv"], key="up_rad")
    if up_r:
        st.session_state.rad_path = save_file(up_r.getvalue(), up_r.name)
        st.session_state.rad_ext = os.path.splitext(st.session_state.rad_path)[1].lower()
        # NE PAS toucher à prest_*
        st.session_state.rad_mapped = False

    if st.session_state.rad_path:
        try:
            st.session_state.rad_headers = get_headers(st.session_state.rad_path, st.session_state.rad_ext, HAS_EXCEL)
        except Exception as e:
            st.error("Impossible de lire les entêtes du fichier Radiés :")
            st.exception(e)
            st.session_state.rad_headers = []

        EXPECTED_RAD = ["NUM_ADH", "DATE_RADIATION"]  # extensible (MOTIF, etc.)
        rad_headers = st.session_state.rad_headers

        with st.expander("⚙️ Mapping Radiés", expanded=not st.session_state.rad_mapped):
            if st.session_state.rad_mapped:
                c1, c2 = st.columns([1, 3])
                with c1:
                    if st.button("♻️ Réinitialiser mapping Radiés"):
                        st.session_state.rad_mapped = False
                        st.session_state.rad_map = {}
                with c2:
                    st.info("Mapping Radiés validé.")
            if not st.session_state.rad_mapped and rad_headers:
                defaults_r = st.session_state.rad_map or {}
                with st.form("map_form_radies"):
                    col1r, col2r = st.columns([3, 2])
                    with col1r:
                        mapping_r = {}
                        for col in EXPECTED_RAD:
                            default_idx = idx(rad_headers, col) if not defaults_r.get(col) else (
                                1 + rad_headers.index(defaults_r[col]) if defaults_r.get(col) in rad_headers else 0
                            )
                            mapping_r[col] = st.selectbox(f"{col} ⇢", [""] + rad_headers,
                                                          index=default_idx, key=f"map_rad_{col}")
                    with col2r:
                        limit_rad = st.number_input("Limite lignes affichées (radiés)", min_value=100, max_value=50_000,
                                                    value=st.session_state.rad_limit, step=500, key="rad_limit_in")
                    submitted_r = st.form_submit_button("✅ Valider mapping Radiés")
                    if submitted_r:
                        missing_r = [r for r in EXPECTED_RAD if not mapping_r.get(r)]
                        if missing_r:
                            st.error("Colonnes obligatoires manquantes : " + ", ".join(missing_r))
                        else:
                            with st.spinner("Préparation Radiés…"):
                                st.session_state.rad_map = mapping_r
                                st.session_state.rad_limit = int(limit_rad)
                                st.session_state.rad_mapped = True
                                st.session_state.flash_ok = True
    else:
        st.info("Déposez le fichier Radiés, puis mappez. (Cela n’efface pas le fichier Prestations.)")

# petit feedback non bloquant
if st.session_state.flash_ok:
    st.success("Mapping validé.")
    st.session_state.flash_ok = False

# =============================================
# TAB 3 — ANALYSES (ne dépend QUE de Prestations sauf si vous utilisez Radiés pour un test futur)
# =============================================
with tabs[2]:
    st.subheader("🔎 Analyses & Graphs")
    # ---- AUTO-RÉPARATION DU FLAG PREST ----
# ---- AUTO-RÉPARATION DU FLAG PREST ----
    if (not st.session_state.get("prest_mapped")) \
        and st.session_state.get("prest_path") \
        and st.session_state.get("prest_map") \
        and st.session_state.get("prest_val_abs_src"):
            st.session_state.prest_mapped = True

    if not (st.session_state.prest_mapped and st.session_state.prest_path):
        st.warning("Mappez d’abord **Prestations** dans l’onglet 1.")
    else:
        # Base SQL Prestations
        BASE_SQL = make_base_sql(
            st.session_state.prest_path,
            st.session_state.prest_ext,
            st.session_state.prest_map,
            st.session_state.prest_val_abs_src
        )
        limit_rows = st.session_state.prest_limit

        if not BASE_SQL:
            st.error("Base SQL non construite (mapping incomplet).")
        else:
            # ---------- Test 1 : Doublons P_AS ----------
            st.header("Test 1 - Doublons prestations (P_AS)")
            sql_doublons_pas = BASE_SQL + f"""
            , F AS (
              SELECT *
              FROM Prest
              WHERE "REGLRC_REG_RC" = 'P_AS' AND "WM_ACTE_RC" IS NOT NULL AND "WM_ACTE_RC" <> 'REGUL'
            )
            SELECT *
            FROM F
            WHERE ("NUM_ADH","NOM","PRENOM","COMP_GARA_CODE","WM_ACTE_RC","RO_DATE_SOINS_DEB","VAL_ABS") IN (
              SELECT "NUM_ADH","NOM","PRENOM","COMP_GARA_CODE","WM_ACTE_RC","RO_DATE_SOINS_DEB","VAL_ABS"
              FROM F
              GROUP BY "NUM_ADH","NOM","PRENOM","COMP_GARA_CODE","WM_ACTE_RC","RO_DATE_SOINS_DEB","VAL_ABS"
              HAVING COUNT(DISTINCT "NUM_DEC") > 1
            )
            LIMIT {limit_rows}
            """
            df1 = con.execute(sql_doublons_pas).df()
            nb_lignes1 = len(df1)
            nb_adh1 = len(df1["NUM_ADH"].unique()) if nb_lignes1 and "NUM_ADH" in df1.columns else 0
            impact1 = compute_impact(df1)
            c1, c2, c3 = st.columns(3)
            c1.metric("Lignes concernées", f"{nb_lignes1:,}".replace(",", " "))
            c2.metric("Adhérents uniques", f"{nb_adh1:,}".replace(",", " "))
            c3.metric("Impact € (estimé)", format_euro(impact1))
            if df1.empty:
                st.info("Aucun doublon prestation détecté.")
            else:
                st.dataframe(df1, height=380, use_container_width=True)
                st.download_button("Télécharger CSV (doublons prestations)",
                                   data=df1.to_csv(index=False).encode("utf-8"),
                                   file_name="doublons_prestations.csv", mime="text/csv")

            st.divider()

            # ---------- Test 2 : Doublons P_TI ----------
            st.header("Test 2 - Doublons tiers (P_TI)")
            sql_doublons_pti = BASE_SQL + f"""
            , F AS (
              SELECT *
              FROM Prest
              WHERE "REGLRC_REG_RC" = 'P_TI' AND "WM_ACTE_RC" IS NOT NULL AND "WM_ACTE_RC" <> 'REGUL'
            )
            SELECT *
            FROM F
            WHERE ("NUM_ADH","NOM","PRENOM","COMP_GARA_CODE","WM_ACTE_RC","RO_DATE_SOINS_DEB","VAL_ABS") IN (
              SELECT "NUM_ADH","NOM","PRENOM","COMP_GARA_CODE","WM_ACTE_RC","RO_DATE_SOINS_DEB","VAL_ABS"
              FROM F
              GROUP BY "NUM_ADH","NOM","PRENOM","COMP_GARA_CODE","WM_ACTE_RC","RO_DATE_SOINS_DEB","VAL_ABS"
              HAVING COUNT(DISTINCT "NUM_DEC") > 1
            )
            LIMIT {limit_rows}
            """
            df2 = con.execute(sql_doublons_pti).df()
            nb_lignes2 = len(df2)
            nb_adh2 = len(df2["NUM_ADH"].unique()) if nb_lignes2 and "NUM_ADH" in df2.columns else 0
            impact2 = compute_impact(df2)
            d1, d2, d3 = st.columns(3)
            d1.metric("Lignes concernées", f"{nb_lignes2:,}".replace(",", " "))
            d2.metric("Adhérents uniques", f"{nb_adh2:,}".replace(",", " "))
            d3.metric("Impact € (estimé)", format_euro(impact2))
            if df2.empty:
                st.info("Aucun doublon tiers détecté.")
            else:
                st.dataframe(df2, height=380, use_container_width=True)
                st.download_button("Télécharger CSV (doublons tiers)",
                                   data=df2.to_csv(index=False).encode("utf-8"),
                                   file_name="doublons_tiers.csv", mime="text/csv")

            st.divider()

            # ---------- Test 2bis : Mix P_AS vs P_TI ----------
            st.header("Test 2bis - Doublons mixtes (P_AS vs P_TI)")
            sql_doublons_mix = BASE_SQL + f"""
            , F AS (
              SELECT *
              FROM Prest
              WHERE COALESCE("WM_ACTE_RC",'') <> 'REGUL'
                AND "WM_ACTE_RC" IS NOT NULL
                AND "REGLRC_REG_RC" IN ('P_AS','P_TI')
            )
            , K AS (
              SELECT
                "NUM_ADH","NOM","PRENOM","COMP_GARA_CODE","WM_ACTE_RC","RO_DATE_SOINS_DEB","VAL_ABS",
                COUNT(DISTINCT "NUM_DEC") AS n_dec,
                SUM(CASE WHEN "REGLRC_REG_RC" = 'P_AS' THEN 1 ELSE 0 END) AS c_pas,
                SUM(CASE WHEN "REGLRC_REG_RC" = 'P_TI' THEN 1 ELSE 0 END) AS c_pti
              FROM F
              GROUP BY "NUM_ADH","NOM","PRENOM","COMP_GARA_CODE","WM_ACTE_RC","RO_DATE_SOINS_DEB","VAL_ABS"
              HAVING n_dec > 1 AND c_pas > 0 AND c_pti > 0
            )
            SELECT F.*
            FROM F JOIN K USING ("NUM_ADH","NOM","PRENOM","COMP_GARA_CODE","WM_ACTE_RC","RO_DATE_SOINS_DEB","VAL_ABS")
            ORDER BY "NUM_ADH","RO_DATE_SOINS_DEB","NUM_DEC"
            LIMIT {limit_rows}
            """
            df2b = con.execute(sql_doublons_mix).df()
            nb_lignes2b = len(df2b)
            nb_adh2b = len(df2b["NUM_ADH"].unique()) if nb_lignes2b and "NUM_ADH" in df2b.columns else 0
            impact2b = compute_impact(df2b)
            m1, m2, m3 = st.columns(3)
            m1.metric("Lignes concernées", f"{nb_lignes2b:,}".replace(",", " "))
            m2.metric("Adhérents uniques", f"{nb_adh2b:,}".replace(",", " "))
            m3.metric("Impact € (estimé)", format_euro(impact2b))
            if df2b.empty:
                st.info("Aucun doublon mixte P_AS / P_TI détecté.")
            else:
                st.dataframe(df2b, height=380, use_container_width=True)
                st.download_button("Télécharger CSV (doublons mixtes)",
                                   data=df2b.to_csv(index=False).encode("utf-8"),
                                   file_name="doublons_mixtes_PAS_PTI.csv", mime="text/csv")

            st.divider()

            # ---------- Tops ----------
            st.header("Top 20 - Adhérents (P_AS) & Tiers (P_TI)")
            colA, colB = st.columns(2)

            # Top 20 Adhérents (net)
            with colA:
                st.subheader("Top 20 Adhérents (P_AS) - Total remboursé net")
                sql_top_adh = BASE_SQL + """
                SELECT CAST("NUM_ADH" AS VARCHAR) AS NUM_ADH,
                       SUM(TRY_CAST("WM_MONT_REMB" AS DOUBLE)) AS total_montant
                FROM Prest
                WHERE "REGLRC_REG_RC" = 'P_AS' AND COALESCE("WM_ACTE_RC",'') <> 'REGUL'
                GROUP BY 1
                ORDER BY total_montant DESC
                LIMIT 20
                """
                top_adh = con.execute(sql_top_adh).df()
                if top_adh.empty:
                    st.info("Aucune donnée P_AS pour construire le top adhérents.")
                else:
                    top_adh["NUM_ADH"] = top_adh["NUM_ADH"].astype(str)
                    figA = px.bar(
                        top_adh.sort_values("total_montant", ascending=True),
                        x="total_montant", y="NUM_ADH", orientation="h", text="total_montant"
                    )
                    figA.update_traces(texttemplate="%{text:.0f}", textposition="outside", cliponaxis=False)
                    figA.update_yaxes(type="category", categoryorder="array",
                                      categoryarray=top_adh.sort_values("total_montant")["NUM_ADH"])
                    figA.update_layout(margin=dict(l=0, r=0, t=10, b=0),
                                       xaxis_title="Total remboursé net (€)", yaxis_title="NUM_ADH")
                    st.plotly_chart(figA, use_container_width=True)
                    st.download_button("Télécharger Top Adhérents (CSV)",
                                       data=top_adh.sort_values("total_montant", ascending=False).to_csv(index=False).encode("utf-8"),
                                       file_name="top20_adherents_P_AS.csv", mime="text/csv")

            # Top 20 Tiers (net) — priorités clé
            with colB:
                tiers_priority = [("DESTRC_IBAN", "IBAN"), ("DESTRC_CODE", "Code tiers"), ("DESTRC_TITULAIRE", "Titulaire")]
                tiers_key, tiers_label = None, None
                for k, lab in tiers_priority:
                    if st.session_state.prest_map.get(k):
                        tiers_key, tiers_label = k, lab
                        break

                st.subheader(f"Top 20 Tiers (P_TI) — {tiers_label if tiers_label else 'clé indisponible'} — Total remboursé net")
                if not tiers_key:
                    st.info("Mappez au moins une clé tiers : DESTRC_IBAN, DESTRC_CODE ou DESTRC_TITULAIRE (onglet 1).")
                else:
                    sql_top_tiers = BASE_SQL + f"""
                    SELECT CAST("{tiers_key}" AS VARCHAR) AS tiers_cle,
                           SUM(TRY_CAST("WM_MONT_REMB" AS DOUBLE)) AS total_montant
                    FROM Prest
                    WHERE "REGLRC_REG_RC" = 'P_TI'
                      AND COALESCE("{tiers_key}",'') <> ''
                      AND COALESCE("WM_ACTE_RC",'') <> 'REGUL'
                    GROUP BY 1
                    ORDER BY total_montant DESC
                    LIMIT 20
                    """
                    top_tiers = con.execute(sql_top_tiers).df()
                    if top_tiers.empty:
                        st.info("Aucune donnée P_TI pour construire le top tiers.")
                    else:
                        top_tiers["tiers_cle"] = top_tiers["tiers_cle"].astype(str)
                        figB = px.bar(
                            top_tiers.sort_values("total_montant", ascending=True),
                            x="total_montant", y="tiers_cle", orientation="h", text="total_montant"
                        )
                        figB.update_traces(texttemplate="%{text:.0f}", textposition="outside", cliponaxis=False)
                        figB.update_yaxes(type="category", categoryorder="array",
                                          categoryarray=top_tiers.sort_values("total_montant")["tiers_cle"])
                        figB.update_layout(margin=dict(l=0, r=0, t=10, b=0),
                                           xaxis_title="Total remboursé net (€)", yaxis_title=tiers_label)
                        st.plotly_chart(figB, use_container_width=True)
                        st.download_button("Télécharger Top Tiers (CSV)",
                                           data=top_tiers.sort_values("total_montant", ascending=False).to_csv(index=False).encode("utf-8"),
                                           file_name=f"top20_tiers_P_TI_{tiers_key.lower()}.csv", mime="text/csv")

            st.divider()

            # ---------- >3 RIB ----------
            st.header("Test 3 - Adhérents P_AS avec > 3 RIB distincts (IBAN)")
            if not st.session_state.prest_map.get("DESTRC_IBAN"):
                st.info("Mappez la colonne **DESTRC_IBAN** (onglet 1) pour activer ce test.")
            else:
                sql_rib_hdr = BASE_SQL + f"""
                , F AS (
                  SELECT *
                  FROM Prest
                  WHERE "REGLRC_REG_RC" = 'P_AS'
                    AND COALESCE("WM_ACTE_RC",'') <> 'REGUL'
                    AND COALESCE("DESTRC_IBAN",'') <> ''
                )
                , rib_counts AS (
                  SELECT CAST("NUM_ADH" AS VARCHAR) AS NUM_ADH,
                         COUNT(DISTINCT "DESTRC_IBAN") AS nb_rib,
                         SUM(TRY_CAST("WM_MONT_REMB" AS DOUBLE)) AS total_montant
                  FROM F
                  GROUP BY 1
                )
                SELECT NUM_ADH, nb_rib, total_montant
                FROM rib_counts
                WHERE nb_rib > 2
                ORDER BY nb_rib DESC, total_montant DESC
                LIMIT {limit_rows}
                """
                df_hdr = con.execute(sql_rib_hdr).df()

                if df_hdr.empty:
                    st.success("Aucun adhérent P_AS avec plus de 3 RIB distincts.")
                else:
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Adhérents concernés", f"{len(df_hdr):,}".replace(",", " "))
                    c2.metric("Max RIB par adhérent", int(df_hdr["nb_rib"].max()))
                    c3.metric("Total € (net)", format_euro(pd.to_numeric(df_hdr["total_montant"], errors="coerce").fillna(0).sum()))

                    df_hdr_display = df_hdr.copy()
                    df_hdr_display["total_montant"] = pd.to_numeric(df_hdr_display["total_montant"], errors="coerce").fillna(0)
                    df_hdr_display = df_hdr_display.rename(columns={
                        "NUM_ADH": "Adhérent",
                        "nb_rib": "Nb RIB distincts",
                        "total_montant": "Total remboursé net (€)"
                    })
                    st.dataframe(
                        df_hdr_display.sort_values(["Nb RIB distincts", "Total remboursé net (€)"], ascending=[False, False]),
                        height=320, use_container_width=True
                    )
                    st.download_button("Télécharger (CSV) — Adhérents >3 RIB",
                                       data=df_hdr.sort_values(["nb_rib","total_montant"], ascending=[False, False]).to_csv(index=False).encode("utf-8"),
                                       file_name="adherents_P_AS_plus_de_3_RIB.csv", mime="text/csv")

                    adh_list = df_hdr["NUM_ADH"].astype(str).tolist()
                    sql_detail = BASE_SQL + f"""
                    , F AS (
                      SELECT *
                      FROM Prest
                      WHERE "REGLRC_REG_RC" = 'P_AS'
                        AND COALESCE("WM_ACTE_RC",'') <> 'REGUL'
                        AND COALESCE("DESTRC_IBAN",'') <> ''
                    )
                    SELECT CAST("NUM_ADH" AS VARCHAR) AS NUM_ADH,
                           CAST("DESTRC_IBAN" AS VARCHAR) AS IBAN,
                           SUM(TRY_CAST("WM_MONT_REMB" AS DOUBLE)) AS total_montant,
                           COUNT(*) AS n_operations
                    FROM F
                    WHERE CAST("NUM_ADH" AS VARCHAR) IN ({",".join("'" + a.replace("'","''") + "'" for a in adh_list)})
                    GROUP BY 1, 2
                    """
                    df_detail = con.execute(sql_detail).df()
                    df_detail["total_montant"] = pd.to_numeric(df_detail["total_montant"], errors="coerce").fillna(0)

                    st.subheader("Détails par adhérent (IBAN, montants, nb opérations)")
                    max_adh_detail = 15
                    shown = 0
                    for adh in df_hdr.sort_values(["nb_rib","total_montant"], ascending=[False, False])["NUM_ADH"].astype(str):
                        if shown >= max_adh_detail:
                            break
                        sub = df_detail[df_detail["NUM_ADH"] == adh].copy()
                        if sub.empty:
                            continue
                        sub_sorted = sub.sort_values("total_montant", ascending=True)
                        with st.expander(f"Adhérent {adh} — {sub['IBAN'].nunique()} RIB — Total: {format_euro(sub['total_montant'].sum())}", expanded=False):
                            fig = px.bar(sub_sorted, x="total_montant", y="IBAN", orientation="h", text="total_montant")
                            fig.update_traces(texttemplate="%{text:.0f}", textposition="outside", cliponaxis=False)
                            fig.update_layout(margin=dict(l=0, r=0, t=0, b=0),
                                              xaxis_title="Total remboursé net (€)", yaxis_title="IBAN")
                            st.plotly_chart(fig, use_container_width=True)

                            sub_disp = sub.sort_values("total_montant", ascending=False).rename(columns={
                                "IBAN": "IBAN",
                                "total_montant": "Total remboursé net (€)",
                                "n_operations": "Nb opérations"
                            })[["IBAN","Nb opérations","Total remboursé net (€)"]]
                            st.dataframe(sub_disp, use_container_width=True, height=240)
                            st.download_button(f"Télécharger (CSV) — Détail IBAN {adh}",
                                               data=sub.sort_values("total_montant", ascending=False).to_csv(index=False).encode("utf-8"),
                                               file_name=f"detail_IBAN_P_AS_{adh}.csv", mime="text/csv")
                        shown += 1
                    if len(df_hdr) > max_adh_detail:
                        st.caption(f"Affichés: {max_adh_detail} adhérents sur {len(df_hdr)}. Exportez le CSV pour l’ensemble.")

            st.divider()

            # ---------- Motif A→B→A ----------
            st.header("Test 4 - Motif RIB A → B → A (P_AS) — RIB central suspect")
            needed_keys = ["DESTRC_IBAN", "NUM_ADH", "PRENOM", "RO_DATE_SOINS_DEB", "NUM_DEC"]
            missing_keys = [k for k in needed_keys if not st.session_state.prest_map.get(k)]
            if missing_keys:
                st.info("Mappez les colonnes requises : " + ", ".join(missing_keys))
            else:
                sql_triplets = BASE_SQL + f"""
                , F AS (
                  SELECT CAST("NUM_ADH" AS VARCHAR) AS NUM_ADH,
                         CAST("PRENOM" AS VARCHAR) AS PRENOM,
                         CAST("DESTRC_IBAN" AS VARCHAR) AS IBAN,
                         TRY_CAST("WM_MONT_REMB" AS DOUBLE) AS MONTANT_NET,
                         TRY_CAST("RO_DATE_SOINS_DEB" AS TIMESTAMP) AS D_SOINS,
                         CAST("NUM_DEC" AS VARCHAR) AS NUM_DEC
                  FROM Prest
                  WHERE "REGLRC_REG_RC" = 'P_AS'
                    AND COALESCE("WM_ACTE_RC",'') <> 'REGUL'
                    AND COALESCE("DESTRC_IBAN",'') <> ''
                )
                , W AS (
                  SELECT
                    NUM_ADH, PRENOM, IBAN, MONTANT_NET, D_SOINS, NUM_DEC,
                    LAG(IBAN) OVER (PARTITION BY NUM_ADH, PRENOM ORDER BY D_SOINS, NUM_DEC)  AS IBAN_PREV,
                    LEAD(IBAN) OVER (PARTITION BY NUM_ADH, PRENOM ORDER BY D_SOINS, NUM_DEC) AS IBAN_NEXT,
                    LAG(D_SOINS) OVER (PARTITION BY NUM_ADH, PRENOM ORDER BY D_SOINS, NUM_DEC) AS D_PREV,
                    LEAD(D_SOINS) OVER (PARTITION BY NUM_ADH, PRENOM ORDER BY D_SOINS, NUM_DEC) AS D_NEXT,
                    LAG(MONTANT_NET) OVER (PARTITION BY NUM_ADH, PRENOM ORDER BY D_SOINS, NUM_DEC) AS M_PREV,
                    LEAD(MONTANT_NET) OVER (PARTITION BY NUM_ADH, PRENOM ORDER BY D_SOINS, NUM_DEC) AS M_NEXT,
                    LAG(NUM_DEC) OVER (PARTITION BY NUM_ADH, PRENOM ORDER BY D_SOINS, NUM_DEC) AS NUM_DEC_PREV,
                    LEAD(NUM_DEC) OVER (PARTITION BY NUM_ADH, PRENOM ORDER BY D_SOINS, NUM_DEC) AS NUM_DEC_NEXT
                  FROM F
                )
                SELECT
                  NUM_ADH, PRENOM,
                  IBAN_PREV AS RIB_A1, IBAN AS RIB_B_SUSPECT, IBAN_NEXT AS RIB_A2,
                  D_PREV AS DATE_A1, D_SOINS AS DATE_B, D_NEXT AS DATE_A2,
                  M_PREV AS MONTANT_A1, MONTANT_NET AS MONTANT_B, M_NEXT AS MONTANT_A2,
                  NUM_DEC_PREV AS NUM_DEC_A1, NUM_DEC AS NUM_DEC_B, NUM_DEC_NEXT AS NUM_DEC_A2
                FROM W
                WHERE IBAN_PREV IS NOT NULL AND IBAN_NEXT IS NOT NULL
                  AND IBAN_PREV = IBAN_NEXT AND IBAN_PREV <> IBAN
                ORDER BY NUM_ADH, PRENOM, DATE_B
                LIMIT {limit_rows}
                """
                df_trip = con.execute(sql_triplets).df()
                n_trip = len(df_trip)
                n_adh = df_trip[["NUM_ADH","PRENOM"]].drop_duplicates().shape[0] if n_trip else 0
                c1, c2 = st.columns(2)
                c1.metric("Triplets détectés (A→B→A)", f"{n_trip:,}".replace(",", " "))
                c2.metric("Adhérents concernés", f"{n_adh:,}".replace(",", " "))
                if df_trip.empty:
                    st.success("Aucun motif A→B→A détecté.")
                else:
                    df_view = df_trip.copy()
                    for col in ["MONTANT_A1","MONTANT_B","MONTANT_A2"]:
                        df_view[col] = pd.to_numeric(df_view[col], errors="coerce").fillna(0)
                    df_view = df_view.rename(columns={
                        "NUM_ADH": "Adhérent", "PRENOM": "Prénom",
                        "RIB_A1": "RIB A (avant)", "RIB_B_SUSPECT": "RIB B (suspect)", "RIB_A2": "RIB A (après)",
                        "DATE_A1": "Date A (avant)", "DATE_B": "Date B", "DATE_A2": "Date A (après)",
                        "MONTANT_A1": "Montant A (avant) €", "MONTANT_B": "Montant B (suspect) €", "MONTANT_A2": "Montant A (après) €",
                        "NUM_DEC_A1": "N° décompte A (avant)", "NUM_DEC_B": "N° décompte B", "NUM_DEC_A2": "N° décompte A (après)",
                    })
                    st.dataframe(df_view.sort_values(["Adhérent","Prénom","Date B"]), use_container_width=True, height=380)
                    st.download_button("Télécharger (CSV) — Triplets A→B→A",
                                       data=df_trip.sort_values(["NUM_ADH","PRENOM","DATE_B"]).to_csv(index=False).encode("utf-8"),
                                       file_name="motif_A_B_A_triplets.csv", mime="text/csv")

            # ---------- Test 5 : Prestations payées APRÈS date de radiation ----------
            st.header("Test 5 - Prestations payées après date de radiation (P_AS & P_TI, hors REGUL)")

            # Nécessaire : mapping Radiés OK + colonnes clés
            if not (st.session_state.rad_mapped and st.session_state.rad_path and st.session_state.rad_map.get("NUM_ADH") and st.session_state.rad_map.get("DATE_RADIATION")):
                st.info("Mappez d’abord le fichier **Radiés** (NUM_ADH & DATE_RADIATION) dans l’onglet 2 pour activer ce test.")
            else:
                # Source Radiés
            # --- Source Radiés robuste (xlsx/csv + fallback si plugin excel absent) ---
                rad_path = st.session_state.rad_path
                rad_ext  = st.session_state.rad_ext
                col_num  = st.session_state.rad_map["NUM_ADH"]
                col_date = st.session_state.rad_map["DATE_RADIATION"]

                if rad_ext == ".csv":
                    rad_src = csv_src(rad_path)
                elif HAS_EXCEL:
                    rad_src = excel_src(rad_path)
                else:
                    # Fallback : on lit l'Excel avec Polars si plugin Excel absent
                    needed_cols = [col_num, col_date]
                    df_r = pl.read_excel(rad_path, columns=needed_cols)
                    rename_map = {col_num: "NUM_ADH_SRC", col_date: "DATE_RADIATION_SRC"}
                    df_r = df_r.rename(rename_map)
                    con.register("Rad_df", df_r.to_pandas())
                    rad_src = "Rad_df"
                    col_num = "NUM_ADH_SRC"
                    col_date = "DATE_RADIATION_SRC"


                sql_post_rad = BASE_SQL + f"""
                -- R0 : normalisation Radiés (date via texte ou numéro Excel)
                , R0 AS (
                SELECT
                    CAST("{col_num}" AS VARCHAR) AS NUM_ADH,
                    COALESCE(
                    TRY_CAST("{col_date}" AS TIMESTAMP),
                    DATE '1899-12-30' + CAST(TRY_CAST("{col_date}" AS DOUBLE) AS INTEGER)  -- Excel serial -> date
                    ) AS DATE_RADIATION
                FROM {rad_src}
                WHERE "{col_num}" IS NOT NULL
                )
                -- R : 1 ligne par adhérent (première radiation connue)
                , R AS (
                SELECT NUM_ADH, MIN(DATE_RADIATION) AS DATE_RAD
                FROM R0
                WHERE DATE_RADIATION IS NOT NULL
                GROUP BY 1
                )
                -- P : prestations filtrées (hors REGUL)
                , P AS (
                SELECT
                    CAST("NUM_ADH" AS VARCHAR) AS NUM_ADH,
                    CAST("NOM" AS VARCHAR) AS NOM,
                    CAST("PRENOM" AS VARCHAR) AS PRENOM,
                    CAST("REGLRC_REG_RC" AS VARCHAR) AS TYPE_PAIEMENT,
                    CAST("WM_ACTE_RC" AS VARCHAR) AS ACTE,
                    TRY_CAST("RO_DATE_SOINS_DEB" AS TIMESTAMP) AS DATE_SOINS,
                    CAST("NUM_DEC" AS VARCHAR) AS NUM_DEC,
                    TRY_CAST("WM_MONT_REMB" AS DOUBLE) AS MONTANT_NET
                FROM Prest
                WHERE COALESCE("WM_ACTE_RC",'') <> 'REGUL'
                    AND "REGLRC_REG_RC" IN ('P_AS','P_TI')
                )
                SELECT
                P.NUM_ADH, P.NOM, P.PRENOM,
                P.TYPE_PAIEMENT, P.ACTE,
                P.DATE_SOINS, R.DATE_RAD,
                P.NUM_DEC, P.MONTANT_NET
                FROM P
                JOIN R ON P.NUM_ADH = R.NUM_ADH
                WHERE P.DATE_SOINS > R.DATE_RAD
                ORDER BY P.NUM_ADH, P.DATE_SOINS, P.NUM_DEC
                LIMIT {limit_rows}
                """


                df_post_rad = con.execute(sql_post_rad).df()

                n_lignes = len(df_post_rad)
                n_adh = df_post_rad["NUM_ADH"].nunique() if n_lignes else 0
                total_net = pd.to_numeric(df_post_rad.get("MONTANT_NET", pd.Series([], dtype=float)), errors="coerce").fillna(0).sum()

                c1, c2, c3 = st.columns(3)
                c1.metric("Lignes concernées", f"{n_lignes:,}".replace(",", " "))
                c2.metric("Adhérents uniques", f"{n_adh:,}".replace(",", " "))
                c3.metric("Total net €", format_euro(total_net))

                if df_post_rad.empty:
                    st.success("Aucune prestation après la date de radiation.")
                else:
                    # Affichage propre
                    view = df_post_rad.rename(columns={
                        "TYPE_PAIEMENT": "Type (P_AS/P_TI)",
                        "ACTE": "Acte",
                        "DATE_SOINS": "Date de soins",
                        "DATE_RAD": "Date de radiation",
                        "NUM_DEC": "N° décompte",
                        "MONTANT_NET": "Montant net (€)"
                    })
                    st.dataframe(view, use_container_width=True, height=380)

                    st.download_button(
                        "Télécharger (CSV) — Prestations après radiation",
                        data=df_post_rad.to_csv(index=False).encode("utf-8"),
                        file_name="prestations_apres_radiation.csv",
                        mime="text/csv"
                    )

                st.divider()
                st.header("Courbe mensuelle — année en cours (Total net)")
    
                current_year = int(pd.Timestamp.today().year)
    
                # Expressions robustes pour date et montant (FR -> US)
                date_expr = """
                COALESCE(
                  TRY_CAST("RO_DATE_SOINS_DEB" AS TIMESTAMP),
                  DATE '1899-12-30' + CAST(TRY_CAST("RO_DATE_SOINS_DEB" AS DOUBLE) AS INTEGER)
                )
                """
    
                amount_expr = """
                TRY_CAST(
                  REPLACE(
                    REPLACE(CAST("WM_MONT_REMB" AS VARCHAR), ' ', ''),  -- supprime espaces (y compris insécables)
                    ',', '.'                                           -- remplace virgule décimale
                  ) AS DOUBLE
                )
                """
    
                sql_curvey = BASE_SQL + f"""
                SELECT
                  DATE_TRUNC('month', {date_expr}) AS mois,
                  SUM({amount_expr}) AS montant
                FROM Prest
                WHERE COALESCE("WM_ACTE_RC",'') <> 'REGUL'
                  AND "REGLRC_REG_RC" IN ('P_AS','P_TI')
                  AND EXTRACT(YEAR FROM {date_expr}) = {current_year}
                GROUP BY 1
                ORDER BY 1
                """
                df_line = con.execute(sql_curvey).df()
    
                # Assurer tous les mois de l'année, même vides
                idx = pd.date_range(f"{current_year}-01-01", f"{current_year}-12-01", freq="MS")
                if not df_line.empty:
                    df_line["mois"] = pd.to_datetime(df_line["mois"]).dt.to_period("M").dt.to_timestamp()
                else:
                    df_line = pd.DataFrame(columns=["mois","montant"])
                df_line = pd.DataFrame({"mois": idx}).merge(df_line, on="mois", how="left")
                df_line["montant"] = pd.to_numeric(df_line["montant"], errors="coerce").fillna(0)
    
                fig_curvey = px.line(df_line, x="mois", y="montant", labels={"mois": "Mois", "montant": "Montant net (€)"})
                fig_curvey.update_traces(mode="lines+markers")
                fig_curvey.update_layout(
                    margin=dict(l=0, r=0, t=10, b=0),
                    hovermode="x unified",
                    xaxis=dict(dtick="M1", tickformat="%b"),
                    yaxis=dict(tickformat=",")
                )
                st.plotly_chart(fig_curvey, use_container_width=True)
    
                st.download_button(
                    f"Télécharger (CSV) — Courbe {current_year}",
                    data=df_line.to_csv(index=False).encode("utf-8"),
                    file_name=f"courbe_prestations_{current_year}.csv",
                    mime="text/csv"
                )



# =============================================
# STYLE FIN
# =============================================
st.markdown("""
<style>
.block-container {padding-top: 0.6rem; padding-bottom: 1rem;}
.stExpander {border: 1px solid #e5e7eb; border-radius: 12px; background: #fafafa;}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<style>
/* Masquer le ruban Streamlit Cloud + header */
div[data-testid="stDecoration"] { display: none !important; }
div[data-testid="stHeader"] { height: 0px !important; visibility: hidden !important; }
#MainMenu, header, footer { visibility: hidden !important; }

/* Remonter le contenu au plus haut */
.block-container { padding-top: 0.2rem !important; }
</style>
""", unsafe_allow_html=True)
~
