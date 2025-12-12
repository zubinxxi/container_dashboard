import os
import streamlit as st 
import mysql.connector
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

def init_connector():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWD"),
    )

#@st.cache_data(ttl = 60)
def run_query(query, params=None):
    conn = init_connector()
    with conn.cursor(dictionary=True) as cursor:
        cursor.execute(query, params or ())
        result = cursor.fetchall()
    conn.close()
    return result