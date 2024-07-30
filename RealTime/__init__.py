import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.identity import ClientSecretCredential
from matplotlib.ticker import FuncFormatter
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Kusto connection details from environment variables
cluster = os.getenv('KUSTO_CLUSTERQ')
database = os.getenv('KUSTO_DATABASE')
table = os.getenv('KUSTO_TABLE')
tenant_id = os.getenv('TENANT_ID')
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')

# Authenticate with Azure using client credentials
credentials = ClientSecretCredential(tenant_id, client_id, client_secret)
kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, client_id, client_secret, tenant_id)

# Initialize Kusto client
client = KustoClient(kcsb)

def fetch_data_from_kql():
    query = f"splinter_data | sort by start_time desc | top 10 by start_time | sort by start_time asc"
    response = client.execute(database, query)
    result = response.primary_results[0]
    columns = [col.column_name for col in result.columns]
    rows = [row.to_list() for row in result.rows]
    df = pd.DataFrame(rows, columns=columns)
    
    # Convert columns to float
    for col in df.columns:
        if col not in ["TOTAL_NO", "SPECKS", "FUSED_FIBER", "BIG_FAULT", "start_time"]:
            df[col] = df[col].astype(float)
    df["TOTAL_NO"] = df["TOTAL_NO"].astype(float).round(2)
    return df

def format_func(value, tick_number):
    return f'{value:.3f}'

def plot_graphs(df):
    columns = df.columns.tolist()
    columns.remove("TOTAL_NO")
    columns.remove("SPECKS")
    columns.remove("FUSED_FIBER")
    columns.remove("BIG_FAULT")
    columns.remove("start_time")
    for column in columns:
        plt.figure()
        plt.plot(df["TOTAL_NO"], df[column], marker='o')
        plt.title(f'{column} vs TOTAL_NO')
        plt.xlabel("TOTAL_NO")
        plt.ylabel(column)
        # plt.gca().xaxis.set_major_formatter(FuncFormatter(format_func))
        plt.grid(True)
        st.pyplot(plt)

# Streamlit app
st.title("KQL Data Analysis Report")

st.write("Fetching data from KQL database...")

# Fetch data
data_df = fetch_data_from_kql()
st.write(data_df.head(10))

# Plot graphs
st.write("Plotting graphs for each column against TOTAL_NO...")
plot_graphs(data_df)
