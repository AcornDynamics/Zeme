import streamlit as st
import pandas as pd
import numpy as np


def _to_numeric_clean(s: pd.Series) -> pd.Series:
    """Convert strings like '12 345,67€' to numeric, return NaN on failure."""
    if s is None:
        return pd.Series(dtype=float)
    return pd.to_numeric(
        s.astype(str)
         .str.replace(r"[^0-9,\.\-]", "", regex=True)  # keep digits, comma, dot, minus
         .str.replace(",", ".", regex=False),           # EU decimal -> dot
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
    df = pd.read_csv("df_zeme.csv")

    # ---- KPIs (metrics) -----------------------------------------------------
    # Property count (count of non-null 'Link' entries)
    if "Link" in df.columns:
        property_count = int(df["Link"].dropna().shape[0])
    else:
        property_count = int(len(df))

    # Avg price (EUR), outlier-trimmed
    avg_price = _iqr_trim_mean(df["Cena EUR"]) if "Cena EUR" in df.columns else float("nan")

    # Avg size (m²), derive if needed, outlier-trimmed
    size_m2_series = _derive_size_m2(df)
    if "Platiba m2" not in df.columns and not size_m2_series.isna().all():
        # add derived column so the table shows it as well
        df["Platiba m2"] = size_m2_series
    avg_size_m2 = _iqr_trim_mean(size_m2_series)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Property count", f"{property_count:,}")
    with c2:
        st.metric("Avg price (EUR)", "—" if np.isnan(avg_price) else f"{avg_price:,.0f} €")
    with c3:
        st.metric("Avg size (m²)", "—" if np.isnan(avg_size_m2) else f"{avg_size_m2:,.0f} m²")

    # ---- Data table ----------------------------------------------------------
    st.dataframe(
        df,
        column_config={"Link": st.column_config.LinkColumn("Link")},
        use_container_width=True,
        height=600,
    )


if __name__ == "__main__":
    main()
