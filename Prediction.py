import streamlit as st
import urllib.request
import json
import os
import ssl
import pandas as pd
import requests
import logging
from datetime import datetime
from azure.kusto.data import KustoConnectionStringBuilder
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, ReportLevel
from azure.kusto.data.data_format import DataFormat
from azure.identity import ClientSecretCredential
from azure.kusto.data.exceptions import KustoServiceError


# Setup logging
logging.basicConfig(level=logging.INFO)

def allowSelfSignedHttps(allowed):
    if allowed and not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
        ssl._create_default_https_context = ssl._create_unverified_context

allowSelfSignedHttps(True)

def get_prediction(data, api_key):
    url = 'https://splinterxg-endpoint-592df72e.centralindia.inference.ml.azure.com/score'
    body = str.encode(json.dumps(data))
    headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + api_key, 'azureml-model-deployment': 'blue'}
    req = urllib.request.Request(url, body, headers)
    
    try:
        response = urllib.request.urlopen(req)
        result = response.read().decode('utf-8')
        output_data = json.loads(result)
        return output_data[0]
    except urllib.error.HTTPError as error:
        st.error(f"Request failed with status code: {error.code}")
        st.error(error.read().decode("utf8", 'ignore'))
        return None

def save_to_kql(input_data, output_data):
    combined_data = [datetime.now()] + input_data + output_data[::-1]
    columns = [
        "start_time", "Steep_Lye_Alk.", "Steep_Lye_Temp", "Dissolver_Lye_Conc", "Dissolver_Ball_Fall",
        "Rec_BF", "Rec_KW", "Flash_Dearator", "GCF_delta_Flow_Stage_1", "GCF_delta_Flow_Stage_2",
        "GCF_delta_Flow_Stage_3", "Machine_Ball_Fall", "Machine_RI", "Machine_Alk.",
        "Machine_Cellulose", "Vis_MC_temp", "MC_1_Sulphate_Conc", "MC_1_Spin_bath_Acid",
        "MC_1_Spin_bath_ZnSO4", "MC_1_Spin_bath_temp", "SB_feed_flow_Side_A", "SB_feed_flow_Side_B",
        "Viscose_Total_volume", "Viscose_Pressure_at_Spg", "Strectch", "BIG_FAULT", "FUSED_FIBER",
        "SPECKS", "TOTAL_NO"
    ]
    df = pd.DataFrame(data=[combined_data], columns=columns)

    cluster = "https://ingest-trd-bm11prjec8t48s5ruy.z8.kusto.fabric.microsoft.com/"
    database = "splinter_db"
    table = "splinter_data"
    tenant_id = "077104d2-9b90-4ca9-8afb-9657962a1309"
    client_id = "4f09a177-020f-4c51-abb6-36ef18cd5d14"
    client_secret = "msl8Q~K_..m3_baSbjM15PmwW56CNlzox36ioc4v"

    credentials = ClientSecretCredential(tenant_id, client_id, client_secret)
    kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster, client_id, client_secret, tenant_id)

    try:
        kusto_client = QueuedIngestClient(kcsb)
        ingestion_props = IngestionProperties(
            database=database,
            table=table,
            data_format=DataFormat.CSV,
            flush_immediately=True,
            report_level=ReportLevel.FailuresAndSuccesses
        )
        response = kusto_client.ingest_from_dataframe(df, ingestion_properties=ingestion_props)
        logging.info(response.status.value)
        st.success("Data ingested successfully.")
        return response
    except KustoServiceError as e:
        st.error(f"Kusto Service Error: {str(e)}")
        raise Exception(f"Kusto Service Error: {str(e)}")

# Streamlit app
st.title("Splinter Prediction and KQL Ingestion")

with st.form("input_form"):
     input_data = [
        st.number_input('Steep Lye Alk.'),
        st.number_input('Steep Lye Temp'),
        st.number_input('Dissolver Lye Conc _gpl_'),
        st.number_input('Dissolver Ball Fall'),
        st.number_input('Rec BF'),
        st.number_input('Rec KW'),
        st.number_input('Flash Dearator'),
        st.number_input('GCF delta Flow Stage 1'),
        st.number_input('GCF delta Flow Stage 2'),
        st.number_input('GCF delta Flow Stage 3'),
        st.number_input('Machine Ball Fall'),
        st.number_input('Machine RI'),
        st.number_input('Machine Alk.'),
        st.number_input('Machine Cellulose'),
        st.number_input('Vis M_C temp'),
        st.number_input('MC 1 Sulphate Conc'),
        st.number_input('MC 1 Spin bath Acid'),
        st.number_input('MC 1 Spin bath ZnSO4'),
        st.number_input('MC 1 Spin bath temp.'),
        st.number_input('SB feed flow Side A'),
        st.number_input('SB feed flow Side B'),
        st.number_input('Viscose Total volume'),
        st.number_input('Viscose Pressure at Spg'),
        st.number_input('Strectch')
    ]
     api_key = "5B6MhD4h83j3umAeZmO2YiaINAl0wYXl"
     submitted = st.form_submit_button("Submit")
     if submitted and all(input_data):
        data = {
            "input_data": [input_data],
            "params": {}
        }
        
        output_data = get_prediction(data, api_key)
        if output_data:
            st.write("Prediction Results:")
            st.write(output_data)
            save_to_kql(input_data, output_data)
