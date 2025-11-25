import streamlit as st
import pandas as pd
import plotly.express as px
import os

# --- CONFIGURACIÓN DE LA PÁGINA ---
st.set_page_config(
    page_title="Monitor de Churn - ENTEL",
    page_icon="📡",
    layout="wide"
)

# --- TÍTULO Y DESCRIPCIÓN ---
st.title("📡 Dashboard de Monitoreo de Churn")

# --- CARGA AUTOMÁTICA DE DATOS ---
DATA_PATH = "/home/chex/BigData_UPAO/output_modelo"

@st.cache_data(ttl=60)
def load_data():
    if not os.path.exists(DATA_PATH):
        st.error(f"No se encontró la carpeta de datos en: {DATA_PATH}")
        return None
    
    try:
        df = pd.read_parquet(DATA_PATH)
        
        if 'prob_churn' in df.columns:
            df = df.rename(columns={'prob_churn': 'Probability'})
        
        if 'prediction' in df.columns:
            df = df.rename(columns={'prediction': 'Prediction'})
            
        return df
    except Exception as e:
        st.error(f"Error leyendo el archivo Parquet: {e}")
        return None

df = load_data()

if df is not None:
    # ---------------------------------------------------------
    # SECCIÓN 1: KPIS GENERALES
    # ---------------------------------------------------------
    st.subheader("📊 Panorama General")
    
    total_clientes = len(df)
    churners = df[df['Prediction'] == 1.0]
    tasa_churn = (len(churners) / total_clientes) * 100
    ingresos_riesgo = churners['monto_total_facturado'].sum()
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Clientes", f"{total_clientes:,}")
    col2.metric("Clientes en Riesgo (Predichos)", f"{len(churners):,}")
    col3.metric("Tasa de Churn", f"{tasa_churn:.2f}%", delta_color="inverse")
    col4.metric("Ingresos en Riesgo Total", f"S/. {ingresos_riesgo:,.2f}")
    
    st.divider()

    # ---------------------------------------------------------
    # SECCIÓN 2: ZONA DE ACCIÓN INMEDIATA (LO NUEVO)
    # ---------------------------------------------------------
    st.markdown("### 🚨 ZONA DE ACCIÓN INMEDIATA (Probabilidad ≥ 75%)")
    
    # 1. Filtrar Clientes Críticos
    umbral_riesgo = 0.75
    df_criticos = df[df['Probability'] >= umbral_riesgo].copy()
    
    # Ordenar por probabilidad (los más urgentes primero)
    df_criticos = df_criticos.sort_values(by='Probability', ascending=False)
    
    # Calcular dinero específico de este grupo
    dinero_critico = df_criticos['monto_total_facturado'].sum()
    cantidad_criticos = len(df_criticos)

    if cantidad_criticos > 0:
        # Mostrar mensaje de impacto
        st.warning(
            f"⚠️ Se han detectado **{cantidad_criticos} clientes** con riesgo extremo de abandono. "
            f"Esto representa **S/. {dinero_critico:,.2f}** en facturación mensual en peligro inminente."
        )

        # Selector de columnas para mostrar solo lo útil al Call Center
        cols_mostrar = [
            'id_cliente', 'Probability', 'region', 'id_plan', 
            'monto_total_facturado', 'promedio_calidad_red', 
            'total_facturas_pendientes', 'total_comentarios_social'
        ]
        
        # Mostrar tabla interactiva
        st.dataframe(
            df_criticos[cols_mostrar].style.format({
                'Probability': '{:.1%}',
                'monto_total_facturado': 'S/. {:.2f}',
                'promedio_calidad_red': '{:.1f}'
            }).background_gradient(subset=['Probability'], cmap='Reds'),
            use_container_width=True
        )

        # BOTÓN DE DESCARGA (Para enviar al equipo de Retención)
        csv = df_criticos[cols_mostrar].to_csv(index=False).encode('utf-8')
        
        st.download_button(
            label="📥 Descargar Lista para Call Center (CSV)",
            data=csv,
            file_name='clientes_riesgo_critico.csv',
            mime='text/csv',
            type='primary' # Botón rojo destacado
        )
        
    else:
        st.success("✅ ¡Excelente! No hay clientes con probabilidad de riesgo superior al 75% en este momento.")

    st.divider()

    # ---------------------------------------------------------
    # SECCIÓN 3: GRÁFICOS DE ANÁLISIS
    # ---------------------------------------------------------
    c1, c2 = st.columns(2)
    
    with c1:
        st.subheader("📉 Facturas Pendientes vs Churn")
        df_facturas = df.groupby('total_facturas_pendientes')['Prediction'].mean().reset_index()
        fig_facturas = px.bar(
            df_facturas, x='total_facturas_pendientes', y='Prediction',
            labels={'Prediction': 'Probabilidad', 'total_facturas_pendientes': 'Facturas Pendientes'},
            color='Prediction', color_continuous_scale='Reds'
        )
        st.plotly_chart(fig_facturas, use_container_width=True)

    with c2:
        st.subheader("📶 Calidad de Red vs Probabilidad")
        if 'promedio_calidad_red' in df.columns:
            # Redondear para agrupar mejor en el gráfico
            df['calidad_redondeada'] = df['promedio_calidad_red'].round(1)
            df_red = df.groupby('calidad_redondeada')['Prediction'].mean().reset_index()
            
            fig_red = px.line(
                df_red, x='calidad_redondeada', y='Prediction', markers=True,
                labels={'calidad_redondeada': 'Calidad de Red', 'Prediction': 'Tasa de Abandono'}
            )
            fig_red.update_traces(line_color='red', line_width=3)
            st.plotly_chart(fig_red, use_container_width=True)

    # --- SEGMENTACIÓN ---
    c3, c4 = st.columns(2)

    with c3:
        st.subheader("💬 Impacto de Redes Sociales")
        fig_social = px.box(
            df, x='Prediction', y='total_comentarios_social', color='Prediction',
            labels={'Prediction': 'Es Churner (0=No, 1=Si)'}
        )
        st.plotly_chart(fig_social, use_container_width=True)

    with c4:
        st.subheader("🌎 Riesgo por Región")
        df_region = df.groupby('region')['Prediction'].mean().reset_index().sort_values('Prediction', ascending=False)
        fig_region = px.bar(
            df_region, x='region', y='Prediction', color='Prediction'
        )
        st.plotly_chart(fig_region, use_container_width=True)
        
    # Botón manual
    if st.button('🔄 Recargar Datos del Pipeline'):
        st.cache_data.clear()
        st.rerun()
