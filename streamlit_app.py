import streamlit as st
import pandas as pd
import numpy as np

# Plotly is optional; fall back to st.bar_chart if missing
try:
    import plotly.express as px
    _HAS_PLOTLY = True
except Exception:
    _HAS_PLOTLY = False

# Map libraries are optional; show helpful message if missing
try:
    import folium, requests
    from streamlit_folium import st_folium
    _HAS_MAP = True
except Exception:
    _HAS_MAP = False

# LVM GEO constants
WMS_URL = "https://lvmgeoserver.lvm.lv/geoserver/ows"
WFS_URL = "https://lvmgeoserver.lvm.lv/geoserver/publicwfs/wfs"
LAYER   = "publicwfs:Kadastra_karte"


def _to_numeric_clean(s: pd.Series) -> pd.Series:
    """Convert strings like '12 345,67€' to numeric, return NaN on failure."""
    if s is None:
        return pd.Series(dtype=float)
    return pd.to_numeric(
        s.astype(str)
         .str.replace(r"[^0-9,\.\-]", "", regex=True)  # keep digits, comma, dot, minus
         .str.replace(",", ".", regex=False),          # EU decimal -> dot
        errors="coerce",
    )


def _iqr_trim_mean(series: pd.Series) -> float:
    """Mean after removing outliers via 1.5*IQR rule. Fallback to simple mean if all trimmed."""
    if series is None:
        return float("nan")
    s = _to_numeric_clean(series).dropna()
    if s.empty:
        return float("nan")
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    if iqr == 0:
        return float(s.mean())
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    s_trim = s[(s >= lower) & (s <= upper)]
    if s_trim.empty:
        return float(s.mean())
    return float(s_trim.mean())


def _derive_size_m2(df: pd.DataFrame) -> pd.Series:
    """Return a size series in m². Use 'Platiba m2' if present; otherwise derive from
    'Platiba Daudzums' + 'Platiba Mervieniba' (supports 'm2'/'m²' and 'ha'/'ha.')."""
    if "Platiba m2" in df.columns:
        return _to_numeric_clean(df["Platiba m2"]).rename("Platiba m2")
    if {"Platiba Daudzums", "Platiba Mervieniba"}.issubset(df.columns):
        qty = _to_numeric_clean(df["Platiba Daudzums"]).fillna(np.nan)
        unit = df["Platiba Mervieniba"].astype(str).str.lower().str.strip()
        mult = np.where(
            unit.str.contains("ha"), 10000.0,
            np.where(unit.str.contains("m2") | unit.str.contains("m²"), 1.0, np.nan)
        )
        size_m2 = qty * mult
        return pd.Series(size_m2, name="Platiba m2")
    # No compatible columns found
    return pd.Series([np.nan] * len(df), name="Platiba m2")


def _guess_cadastral_field(sample_feature):
    """Try to guess the cadastral number property from a sample WFS feature."""
    if not isinstance(sample_feature, dict):
        return None
    props = sample_feature.get("properties", {}) or {}
    candidates = list(props.keys())
    ranked = []
    for k in candidates:
        kl = k.lower()
        score = 0
        if "kadastr" in kl or "kadastra" in kl:
            score += 2
        if "num" in kl or "nr" in kl:
            score += 1
        if kl.endswith("nr") or kl.endswith("numurs"):
            score += 1
        ranked.append((score, k))
    ranked.sort(reverse=True)
    return ranked[0][1] if ranked and ranked[0][0] > 0 else None


def main():
    st.title("Zeme Data Explorer")
    st.write("Simple Streamlit app to explore property data.")
    df = pd.read_csv("df_zeme.csv")

    # ---- Metrics row (filled after filters) ---------------------------------
    m1, m2, m3 = st.columns(3)

    # ---- Filters (under metrics) --------------------------------------------
    cities = sorted(df["Pilseta"].dropna().astype(str).unique()) if "Pilseta" in df.columns else []
    types = sorted(df["Zemes Tips"].dropna().astype(str).unique()) if "Zemes Tips" in df.columns else []
    pp_opts = sorted(df["Pilseta/Pagasts"].dropna().astype(str).unique()) if "Pilseta/Pagasts" in df.columns else []

    f1, f2, f3 = st.columns(3)
    with f1:
        sel_cities = st.multiselect("Pilseta", options=cities, default=[])
    with f2:
        sel_types = st.multiselect("Zemes Tips", options=types, default=[])
    with f3:
        sel_pp = st.multiselect("Pilseta/Pagasts", options=pp_opts, default=[])

    # ---- Apply filters -------------------------------------------------------
    filtered = df.copy()
    if sel_cities and "Pilseta" in filtered.columns:
        filtered = filtered[filtered["Pilseta"].astype(str).isin(sel_cities)]
    if sel_types and "Zemes Tips" in filtered.columns:
        filtered = filtered[filtered["Zemes Tips"].astype(str).isin(sel_types)]
    if sel_pp and "Pilseta/Pagasts" in filtered.columns:
        filtered = filtered[filtered["Pilseta/Pagasts"].astype(str).isin(sel_pp)]

    # ---- KPIs (metrics) based on filtered data ------------------------------
    property_count = int(filtered["Link"].dropna().shape[0]) if "Link" in filtered.columns else int(len(filtered))
    avg_price = _iqr_trim_mean(filtered["Cena EUR"]) if "Cena EUR" in filtered.columns else float("nan")
    size_m2_series = _derive_size_m2(filtered)
    avg_size_m2 = _iqr_trim_mean(size_m2_series)

    with m1:
        st.metric("Property count", f"{property_count:,}")
    with m2:
        st.metric("Avg price (EUR)", "—" if np.isnan(avg_price) else f"{avg_price:,.0f} €")
    with m3:
        st.metric("Avg size (m²)", "—" if np.isnan(avg_size_m2) else f"{avg_size_m2:,.0f} m²")

    # ---- Chart: Properties per City (count of Link per Pilseta) -------------
    if "Pilseta" in filtered.columns:
        if "Link" in filtered.columns:
            tmp = filtered[filtered["Link"].notna()].copy()
            tmp["Pilseta"] = tmp["Pilseta"].astype(str)
            grp = tmp.groupby("Pilseta")["Link"].count().reset_index(name="Property count")
        else:
            tmp = filtered.copy()
            tmp["Pilseta"] = tmp["Pilseta"].astype(str)
            grp = tmp.groupby("Pilseta").size().reset_index(name="Property count")

        if not grp.empty:
            grp = grp.sort_values("Property count", ascending=False)
            if _HAS_PLOTLY:
                grp["Pilseta"] = pd.Categorical(grp["Pilseta"], categories=grp["Pilseta"], ordered=True)
                fig = px.bar(grp, x="Pilseta", y="Property count", title="Properties per City")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Plotly not installed; showing a basic bar chart instead.")
                st.bar_chart(grp.set_index("Pilseta")["Property count"])
        else:
            st.info("No data to plot for selected filters.")
    else:
        st.info("Column 'Pilseta' not found in the dataset.")

    # ---- Kadastra karte overlay (LVM) ---------------------------------------
    st.subheader("Kadastra karte (LVM)")
    if _HAS_MAP:
        # Base map centered on Latvia
        m = folium.Map(location=[56.95, 24.1], zoom_start=7, tiles="CartoDB Positron")

        # WMS overlay (fast raster)
        folium.WmsTileLayer(
            url=WMS_URL,
            layers=LAYER,
            name="Kadastra karte (WMS)",
            fmt="image/png",
            transparent=True,
            version="1.3.0",
            attr="© LVM GEO",
            overlay=True,
            control=True,
            opacity=0.8,
        ).add_to(m)

        # Optional: search one parcel via WFS (GeoJSON)
        zemes_numurs = st.text_input("Zemes Numurs", placeholder="e.g. 64090020188")
        if zemes_numurs:
            # Try to guess the cadastral field if unknown:
            field_name = None
            try:
                sample = requests.get(WFS_URL, params={
                    "service": "WFS",
                    "version": "2.0.0",
                    "request": "GetFeature",
                    "typenames": LAYER,
                    "outputFormat": "application/json",
                    "srsName": "EPSG:4326",
                    "count": 1,
                }, timeout=20).json()
                feats = (sample or {}).get("features") or []
                if feats:
                    field_name = _guess_cadastral_field(feats[0])
            except Exception:
                field_name = None

            # Fallback list of common field names if guessing fails
            if not field_name:
                for cand in ["KADASTRA_NUMURS", "kadastra_numurs", "KADASTRS", "KADASTRA", "KAD_NUMURS"]:
                    field_name = cand
                    break

            params = {
                "service": "WFS",
                "version": "2.0.0",
                "request": "GetFeature",
                "typenames": LAYER,
                "outputFormat": "application/json",
                "srsName": "EPSG:4326",
                "cql_filter": f"{field_name}='{zemes_numurs}'" if field_name else None,
            }
            params = {k: v for k, v in params.items() if v is not None}

            try:
                gj = requests.get(WFS_URL, params=params, timeout=20).json()
                folium.GeoJson(
                    gj,
                    name=f"Zemes {zemes_numurs}",
                    style_function=lambda f: {"color": "#FF6B00", "weight": 2, "fillOpacity": 0.15},
                ).add_to(m)

                # Zoom to bbox if present
                b = gj.get("bbox")
                if b and len(b) == 4:
                    sw = [b[1], b[0]]  # (lat, lon)
                    ne = [b[3], b[2]]
                    m.fit_bounds([sw, ne])
            except Exception as e:
                st.warning(f"WFS fetch failed: {e}")

        folium.LayerControl(collapsed=True).add_to(m)
        st_folium(m, use_container_width=True, height=600)
    else:
        st.info("To enable the cadastral map overlay, install: `pip install folium streamlit-folium requests`")

    # ---- Data table (show filtered results) ---------------------------------
    df_display = filtered.copy()
    if "Platiba m2" not in df_display.columns and not size_m2_series.isna().all():
        df_display["Platiba m2"] = size_m2_series

    # Show links as a single 🔗 icon instead of full URL
    if "Link" in df_display.columns:
        df_show = df_display.copy()
        df_show["Open"] = df_show["Link"]  # preserve URL
        column_order = ["Open"] + [c for c in df_show.columns if c not in ("Open", "Link")]
        st.dataframe(
            df_show[column_order],
            column_config={
                "Open": st.column_config.LinkColumn(
                    "Open", help="Open listing", display_text="🔗"
                ),
            },
            use_container_width=True,
            height=600,
        )
    else:
        st.dataframe(
            df_display,
            use_container_width=True,
            height=600,
        )


if __name__ == "__main__":
    main()
