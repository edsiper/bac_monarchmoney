import streamlit as st
import pandas as pd
from datetime import datetime
import io

def convert_bac_to_target_format(df):
    df.columns = [col.strip() for col in df.columns]
    df = df.rename(columns={
        "Fecha de Transacción": "Date",
        "Descripción de Transacción": "Merchant",
        "Débito de Transacción": "Debit",
        "Crédito de Transacción": "Credit"
    })

    df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y").dt.strftime("%Y-%m-%d")
    df["Amount"] = df["Credit"].fillna(0) - df["Debit"].fillna(0)

    formatted_df = pd.DataFrame({
        "Date": df["Date"],
        "Merchant": df["Merchant"],
        "Category": "",
        "Account": "BAC",
        "Original Statement": df["Merchant"],
        "Notes": "",
        "Amount": df["Amount"].round(2),
        "Tags": ""
    })

    return formatted_df

st.title("Conversor BAC a Monarch")
uploaded_file = st.file_uploader("Sube tu CSV de BAC", type="csv")

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file, skiprows=4, encoding="ISO-8859-1")
    result_df = convert_bac_to_target_format(df)

    st.success("Conversión lista. Puedes descargar el archivo:")
    st.download_button(
        label="Descargar CSV convertido",
        data=result_df.to_csv(index=False, header=False).encode('utf-8'),
        file_name="bac_formatted.csv",
        mime='text/csv'
    )

