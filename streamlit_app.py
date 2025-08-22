import streamlit as st
import pandas as pd
import numpy as np

# Try to use Plotly for the sunburst
try:
    import plotly.express as px
    _HAS_PLOTLY = True
except Exception:
    _HAS_PLOTLY = False


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


def main():
    st.title("Zeme Data Explorer")
    st.write("Simple Streamlit app to explore property data.")
    df = pd.read_csv("df_zeme_filtered.csv")  # <-- switched to this CSV

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

    # ---- Sunburst: Pilseta -> Pilseta/Pagasts -> Ciems ----------------------
    st.subheader("Location hierarchy (Sunburst)")
    if _HAS_PLOTLY:
        hierarchy_cols = [c for c in ["Pilseta", "Pilseta/Pagasts", "Ciems"] if c in filtered.columns]
        if hierarchy_cols:
            sun = filtered.copy()
            # Count weights: prefer non-null Link counts; otherwise each row counts as 1
            sun["__value__"] = sun["Link"].notna().astype(int) if "Link" in sun.columns else 1
            # Fill missing labels for cleaner hierarchy
            for c in hierarchy_cols:
                sun[c] = sun[c].fillna("—").astype(str).str.strip().replace({"": "—"})
            fig = px.sunburst(
                sun,
                path=hierarchy_cols,        # 1: Pilseta, 2: Pilseta/Pagasts, 3: Ciems
                values="__value__",
                title="Properties by Pilseta → Pagasts → Ciems",
            )
            fig.update_layout(margin=dict(l=0, r=0, t=40, b=0), height=600)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sunburst requires at least one of: 'Pilseta', 'Pilseta/Pagasts', 'Ciems'.")
    else:
        st.warning("Plotly not installed. Run: pip install plotly")

    # ---- Kadastra karte overlay (LVM) — temporarily disabled -----------------
    # Map overlay (WMS/WFS) is commented out per request. Reinsert later if needed.

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
