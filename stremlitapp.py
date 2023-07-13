import streamlit as st
import numpy as np
import pandas as pd
import psycopg2 as psy
import pandas.io.sql as psql

def init_connection():
    return psy.connect(**st.secrets["postgres"])

st.set_page_config(
    page_title="Real-Time Data Science Dashboard",
    page_icon="✅",
    layout="wide",
)
st.title("Live data Dashboard")

placeholder = st.empty()
try:
    connection = init_connection()
    print("Conexion exitosa")
    df = psql.read_sql("""SELECT date(fechahora_autorizacion) as fecha, 
    sum(total_coniva) as ventas, codigo_proveedor_pp, c.nombre_tienda
    FROM cu_pedidos_enc p
    JOIN cu_proveedores c ON c.codigo_proveedor = p.codigo_proveedor_pp
    WHERE codigo_proveedor_pp IN ('8347', '9256','9438','9889') AND
    fechahora_autorizacion  BETWEEN '2023-07-01 00:00:00' AND now()
    AND	p.estado  = 'pagado'
    AND	p.estado_autorizacion  = 'aprobado'
    GROUP BY fecha, p.codigo_proveedor_pp, c.nombre_tienda""", connection)
    tienda_filter = st.selectbox("Seleccione la tienda", pd.unique(df["nombre_tienda"]))
    venta = df['ventas'].sum()
    st.metric(label="vta",value= venta ,label_visibility= "visible")
    df = df[df["nombre_tienda"] == tienda_filter]
    st.dataframe(df)


except Exception as ex:
    print(ex)
finally:
    connection.close()
    print('Conexion finalizada')

