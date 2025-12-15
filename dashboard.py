import os
import streamlit as st
import pandas as pd
import plotly.express as px
import mysql.connector
from datetime import datetime, time
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# --- Configuración de la Página ---
st.set_page_config(
    page_title="INFOPORT | Movimiento de Contenedores",
    page_icon="assets/favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Configuración de la Base de Datos ---
# **IMPORTANTE:** Reemplaza estos valores con tus credenciales de CloudPanel
DB_CONFIG = {
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWD"),
    'host': os.getenv("DB_HOST"), # Podría ser '127.0.0.1' o el host de tu base de datos
    'database': os.getenv("DB_NAME")
}

@st.cache_data(ttl=600)
def load_data():
    """
    Carga los datos de la tabla movimiento_contenedores y sus lookups.
    Usa st.cache_data para que la conexión y la carga se hagan solo una vez.
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        
        # 1. Movimientos de Contenedores
        query_movimientos = """
        SELECT 
            id, operator, trip_number, ship_name, loading_port, discharge_port, delivery_port, dock, 
            arrival_date, arrival_time, departure_date, departure_time, container_number, 
            size, type, status, full_empty, temperature, description, dgn_code, imo, 
            call_sign, visit_no, eqd_qual, port_register
        FROM movimiento_contenedores ORDER BY -id;
        """
        df_movimientos = pd.read_sql(query_movimientos, conn)
        
        # 2. Lookups (Estatus y Contenido)
        df_status = pd.read_sql("SELECT code, description FROM estatus_contenedor", conn)
        df_content = pd.read_sql("SELECT code, description FROM contenido_contenedor", conn)
        df_ports = pd.read_sql("SELECT codPaisPuerto, descripcion FROM puertosInternacionales", conn)
        df_eqd_qual = pd.read_sql("SELECT code, description FROM calificador_de_equipo", conn)
        
        conn.close()

        # 3. Mapeo de códigos a descripciones para el dashboard
        status_map = df_status.set_index('code')['description'].to_dict()
        content_map = df_content.set_index('code')['description'].to_dict()
        ports_map = df_ports.set_index('codPaisPuerto')['descripcion'].to_dict()
        eqd_qual_map = df_eqd_qual.set_index('code')['description'].to_dict()
        
        # Función de mapeo con fallback: usa la descripción si existe, si no, usa el valor original
        def map_loading_port(code):
            # Usa el método .get(key, default_value)
            if code is None or pd.isna(code):
                return ''
            # Se asegura que la clave sea string antes de la búsqueda
            return ports_map.get(str(code), code)

        df_movimientos['status_'] = df_movimientos['status'].map(status_map).fillna('Desconocido')
        df_movimientos['full_/_empty_'] = df_movimientos['full_empty'].map(content_map).fillna('Desconocido')
        df_movimientos['port_register_'] = df_movimientos['port_register'].apply(map_loading_port)
        df_movimientos['loading_port_'] = df_movimientos['loading_port'].apply(map_loading_port)
        df_movimientos['discharge_port'] = df_movimientos['discharge_port'].apply(map_loading_port)
        df_movimientos['delivery_port'] = df_movimientos['delivery_port'].apply(map_loading_port)
        df_movimientos['eqd_-_qual'] = df_movimientos['eqd_qual'].map(eqd_qual_map).fillna('Desconocido')
        
        # Conversión de tipos de datos para filtros (especialmente fechas)
        df_movimientos['arrival_date'] = pd.to_datetime(df_movimientos['arrival_date'], errors='coerce').dt.date
        df_movimientos['departure_date'] = pd.to_datetime(df_movimientos['departure_date'], errors='coerce').dt.date
        
        return df_movimientos
    
    except mysql.connector.Error as e:
        st.error(f"Error al conectar o cargar datos de la base de datos: {e}")
        st.info("Por favor, asegúrate de que MariaDB/MySQL esté corriendo y las credenciales en el script `dashboard.py` sean correctas.")
        return pd.DataFrame() # Devuelve un DataFrame vacío en caso de error

df = load_data()
# --- Diseño del Dashboard ---
st.title("🚢 Dashboard Interactivo: Movimiento de Contenedores")
st.markdown("Filtra los datos usando la barra lateral izquierda y observa las métricas clave.")

if not df.empty:
    
    # -----------------------------------
    # Sidebar para Filtros Interactivos
    # -----------------------------------
    st.sidebar.image('assets/amp-logo.png', width=250)
    st.sidebar.header("Opciones de Filtrado")
    
    # Campos disponibles para filtrar
    filter_cols = ['operator', 'loading_port_', 'discharge_port', 'arrival_date', 'departure_date', 'status_', 'full_/_empty_', 'port_register_']
    
    # Crear un diccionario para almacenar los valores filtrados
    filter_values = {}

    for col in filter_cols:
        if col in ['arrival_date', 'departure_date']:
            # Manejo de Filtros de Fecha
            min_date = df[col].min()
            max_date = df[col].max()
            
            if pd.isna(min_date) or pd.isna(max_date):
                st.sidebar.warning(f"No hay datos de fecha válidos para '{col}'.")
                continue
                
            # --- CORRECCIÓN: Verifica si la fecha mínima y máxima son iguales ---
            if min_date == max_date:
                st.sidebar.info(f"Fecha Única para '{col}': {min_date}")
                # Establecer el filtro al valor único y continuar
                filter_values[col] = (min_date, max_date) 
            else:
                # Si hay un rango de fechas válido, muestra el slider
                date_range = st.sidebar.slider(
                    f"Selecciona rango de {col}",
                    min_value=min_date,
                    max_value=max_date,
                    value=(min_date, max_date),
                    format="YYYY-MM-DD"
                )
                # El filtro de fecha se aplicará después
                filter_values[col] = date_range
            
        elif col in ['status_desc', 'full_empty_']:
            # Usar la descripción mapeada para la selección
            unique_options = sorted(df[col].unique().tolist())
            selected = st.sidebar.multiselect(
                f"Filtrar por {col.replace('_', '').capitalize()}",
                options=unique_options,
                default=unique_options
            )
            filter_values[col] = selected
            
        else:
            # Filtros de Multiselect (Operador, Puertos, Registro)
            unique_options = sorted(df[col].astype(str).unique().tolist())
            selected = st.sidebar.multiselect(
                f"Filtrar por {col.replace('_', ' ').capitalize()}",
                options=unique_options,
                default=unique_options
            )
            filter_values[col] = selected

    # -----------------------------------
    # Lógica de Filtrado
    # -----------------------------------
    df_filtered = df.copy()

    for col, selected_values in filter_values.items():
        if col in ['arrival_date', 'departure_date']:
            # Aplicar filtro de rango de fechas
            start_date, end_date = selected_values
            # Se convierte a datetime.date para la comparación
            df_filtered = df_filtered[
                (df_filtered[col] >= start_date) & 
                (df_filtered[col] <= end_date)
            ]
        else:
            # Aplicar filtro de multiselect
            df_filtered = df_filtered[df_filtered[col].astype(str).isin(selected_values)]


    st.info(f"Mostrando {len(df_filtered)} registros de {len(df)} totales.")

    # -----------------------------------
    # Gráficas Interactivas
    # -----------------------------------
    
    col1, col2, col3 = st.columns(3)
    
    # 1. Gráfico de Barras: Movimientos por Operador (Bar Chart)
    with col1:
        st.subheader("Movimientos por Operador")
        if not df_filtered.empty:
            df_count = df_filtered['operator'].value_counts().reset_index()
            df_count.columns = ['Operador', 'Conteo']
            fig_bar = px.bar(
                df_count,
                x='Operador',
                y='Conteo',
                title='Conteo de Movimientos por Operador'
            )
            st.plotly_chart(fig_bar, width="stretch")

    # 2. Gráfico de Pastel: Distribución Lleno/Vacío (Pie Chart)
    with col2:
        st.subheader("Contenedores: Llenos vs. Vacíos")
        if not df_filtered.empty:
            df_pie = df_filtered['full_/_empty_'].value_counts().reset_index()
            df_pie.columns = ['Contenido', 'Conteo']
            fig_pie = px.pie(
                df_pie,
                names='Contenido',
                values='Conteo',
                title='Distribución de Contenido'
            )
            st.plotly_chart(fig_pie, width="stretch")

    # 3. Gráfico de Líneas: Movimientos por Fecha de Llegada (Line Chart)
    with col3:
        st.subheader("Movimientos Diarios (Llegada)")
        if not df_filtered.empty:
            df_line = df_filtered.groupby('arrival_date').size().reset_index(name='Conteo')
            df_line.columns = ['Fecha de Llegada', 'Conteo']
            fig_line = px.line(
                df_line,
                x='Fecha de Llegada',
                y='Conteo',
                title='Tendencia de Llegadas por Día'
            )
            st.plotly_chart(fig_line, width="stretch")


    # -----------------------------------
    # Dataframe Filtrado
    # -----------------------------------
    st.subheader("Tabla de Movimiento de Contenedores (Filtrada)")
    
    # Columnas a mostrar (excluyendo 'id' y los códigos originales)
    cols_to_display = [
        'id', 'operator', 'loading_port_', 'discharge_port', 'arrival_date', 'arrival_time', 
        'departure_date', 'departure_time', 'status_', 'full_/_empty_', 'port_register_',
        'ship_name', 'container_number', 'size', 'type', 'temperature', 'description', 
        'dgn_code', 'imo', 'call_sign', 'trip_number', 'delivery_port', 'dock', 'visit_no', 'eqd_-_qual'
    ]

    # Reemplazar los NaN o None por un string vacío para mejor visualización
    df_display = df_filtered[cols_to_display].fillna('')
    
    # Renombrar columnas para la visualización en el DataFrame
    column_mapping = {
        'id':'ID',
        'status_': 'Estatus',
        'full_/_empty_': 'Lleno/Vacío',
        'operator': 'Operador',
        'loading_port_': 'Puerto Carga',
        'discharge_port': 'Puerto Descarga',
        'arrival_date': 'Fch. Llegada',
        'arrival_time': 'Hr. Llegada',
        'departure_date': 'Fch. Salida',
        'departure_time': 'Hr. Salida',
        'port_register_': 'Pto. Registro',
        'ship_name': 'Nombre Navio',
        'container_number': 'No. Contenedor',
        'size': 'Tamaño',
        'type': 'Tipo',
        'temperature': 'Temperatura',
        'description': 'Descripción',
        'dgn_code': 'Cód. DGN',
        'imo': 'IMO',
        'call_sign': 'Letra de Radio',
        'trip_number': 'No. Viaje',
        'delivery_port': 'Pto. Entrega',
        'dock': 'Muelle',
        'visit_no': 'No. Visita',
        'eqd_-_qual': 'Eqd-Qual'
    }

    df_display = df_display.rename(columns=column_mapping)
    
    st.dataframe(df_display, width="stretch", hide_index=True,)

else:
    st.warning("No se pudieron cargar los datos. Por favor, revisa la configuración de la base de datos.")