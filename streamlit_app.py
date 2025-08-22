import streamlit as st
import pandas as pd


def main():
    st.title("Zeme Data Explorer")
    st.write("Simple Streamlit app to explore property data.")
    df = pd.read_csv("df_zeme.csv")
    df["Link"] = df["Link"].apply(lambda url: f'<a href="{url}" target="_blank">{url}</a>')
    st.markdown(df.to_html(escape=False), unsafe_allow_html=True)


if __name__ == "__main__":
    main()
