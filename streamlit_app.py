import streamlit as st
import pandas as pd


def main():
    st.title("Zeme Data Explorer")
    st.write("Simple Streamlit app to explore property data.")
    df = pd.read_csv("df_zeme.csv")
    st.dataframe(df)


if __name__ == "__main__":
    main()
