import streamlit as st  
import pandas as pd  
import numpy as np  
import matplotlib.pyplot as plt  
import joblib  
import folium  
from streamlit_folium import st_folium  
from folium.plugins import MarkerCluster  
from geopy.distance import geodesic  

# Configuración de la página 
st.set_page_config(layout="wide")

# Carga del modelo  
try:  
    rf_model = joblib.load("modelo_restaurante.pkl")  
except FileNotFoundError:  
    rf_model = None  
    st.warning("El modelo de predicción no está disponible. Algunas funcionalidades estarán deshabilitadas.")  

try:  
    kmeans_model = joblib.load("kmeans_model.joblib")  
except FileNotFoundError:  
    kmeans_model = None  
    st.warning("El modelo K-Means no está disponible. Algunas funcionalidades estarán deshabilitadas.")  

# Crear la disposición con columnas
col1, col2 = st.columns([5, 1])  # Ajusta la proporción según el diseño deseado

with col1:
    st.markdown("<h1 style='text-align: center; color: white;'>Predicción de Éxito de Restaurantes</h1>", unsafe_allow_html=True)
with col2:
    st.image("Deploy/DataPioneer Consulting_Logo.png", width=100)  # Asegúrate de que la ruta sea correcta
#Deploy/DataPioneer Consulting_Logo.png
# Estilo general  
dashboard_bg_color = "#200558"  
st.markdown(f"""  
    <style>  
        .stApp {{  
            background-color: {dashboard_bg_color};  
            color: white;  
        }}  
        .background-color{{  
            background-color: #200558; 
        }} 
        .card {{  
            background-color: #c7b8e7;   
            padding: 15px;   
            margin: 10px;  
            border-radius: 8px;  
            text-align: center;  
            box-shadow: 0 3px 6px rgba(0, 0, 0, 0.1);   
            border: 1px solid #c0c0c0;   
            width: auto;  
            height: 150px;  
        }}
        .custom-container {{  
            background-color: #c7b8e7;   
            padding: 15px;   
            margin: 10px;   
            border-radius: 8px;   
            text-align: center;   
            box-shadow: 0 3px 6px rgba(0, 0, 0, 0.1);   
            border: 1px solid #c0c0c0;   
            width: 800px;   
            height: 600px;   
        }}    
        .metric {{  
            font-size: 1.2em;  
            font-weight: bold;  
            margin-top: 10px;  
        }}  
        h1 {{  
            font-size: 2.5em;  
        }}  
        hr {{  
            border: 1px solid #9f9bab;  
            margin: 20px 0;  
        }}
    </style>  
""", unsafe_allow_html=True)

# Sidebar  
st.sidebar.markdown("<h1 style='font-size: 28px; color:white;'>Filtros</h1>", unsafe_allow_html=True)  

# Cargar los datos de los estados  
states = pd.read_csv('df2.csv', usecols=['state_name'])['state_name'].unique()  

#st.sidebar.markdown("<h3 style='font-size: 18px;'>Selecciona un Estado</h3>", unsafe_allow_html=True)
# Selecciona un estado con un label descriptivo  
selected_state = st.sidebar.selectbox(  
    label='Selecciona un estado',  # Etiqueta válida, aunque esté oculta
    options=[
        "Arizona",  
        "California",  
        "Florida",  
        "Illinois",  
        "New York",  
        "Texas",  
    ],
    label_visibility='hidden'  # Oculta el label visualmente
)
# Control deslizante para seleccionar el radio de proximidad  
st.sidebar.markdown("<h3 style='font-size: 18px;'>Selecciona el radio de proximidad (km)</h3>", unsafe_allow_html=True)  # Ajusta el tamaño  
radius = st.sidebar.slider("", min_value=0.5, max_value=10.0, value=1.0)  

# Funciones auxiliares
def load_data(selected_state):
        """Cargar datos filtrados por estado."""
        df = pd.read_csv('df2.csv', usecols=['name', 'avg_income', 'avg_rating', 'hispanic_population', 'num_of_reviews', 'latitude', 'longitude', 'restaurante_categoria', 'total_population', 'state_name'])
        df = df[df['state_name'] == selected_state]
        return df
# Cargar los datos filtrados  
df_final = load_data(selected_state)  

total_population = df_final['total_population'].mean()  
hispanic_population = df_final['hispanic_population'].mean()  
ratio_hispano = hispanic_population / total_population if total_population > 0 else 0  
avg_income = df_final['avg_income'].mean() 
indice_popularidad = (df_final['avg_rating'].mean() / df_final['num_of_reviews'].mean()) if df_final['num_of_reviews'].mean() > 0 else 0 
# Mostrar los valores calculados en las tarjetas
st.subheader("Métricas del Estado")  

col1, col2, col3, col4 = st.columns(4)

with col1:
        st.markdown(f"""  
            <div class="card">  
                <h3 style="color: black; text-align: center;">Población Total🌎</h3>  
                <div class="metric" style="color: black;  font-size: 22px; font-weight: bold;">{total_population:,.0f}</div>  
            </div> """, unsafe_allow_html=True)
with col2:
        st.markdown(f"""  
            <div class="card">  
                <h3 style="color: black; text-align: center;">Ratio Hispano 👨‍👩‍👧‍👦</h3>  
                <div class="metric" style="color: black;  font-size: 22px; font-weight: bold;">{ratio_hispano:.2%}</div>  
            </div> """, unsafe_allow_html=True)
        
with col3:
        st.markdown(f"""  
            <div class="card">  
                <h3 style="color: black; text-align: center;">Ingreso Promedio💰</h3>  
                <div class="metric" style="color: black;  font-size: 22px; font-weight: bold;">${avg_income:,.2f}</div>  
            </div> """, unsafe_allow_html=True)
with col4:
        st.markdown(f""" 
            <div class="card">  
                <h3 style="color: black; text-align: center;">Cant. de Restaurantes🍽️</h3>  
                <div class="metric" style="color: black; font-size: 22px; font-weight: bold;">{df_final.shape[0]}</div>  
            </div>   """, unsafe_allow_html=True)

# Configuración de columnas
col1 = st.columns([1])
with st.container():
        st.markdown('<div class="custom-background">', unsafe_allow_html=True)
        # Calcular las coordenadas iniciales  
        if df_final.empty:
            st.warning("No hay datos disponibles para mostrar en el mapa.")
        else:
            initial_lat = df_final['latitude'].mean()
            initial_lon = df_final['longitude'].mean()
            m = folium.Map(location=[initial_lat, initial_lon], zoom_start=10)
            marker_cluster = MarkerCluster().add_to(m)  

        # Agregar marcadores al mapa  
        for _, row in df_final.iterrows():  
            lat, lon = row['latitude'], row['longitude']  
            if pd.notna(lat) and pd.notna(lon):  
                color = "green" if row['avg_rating'] >= 4 else "orange" if row['avg_rating'] >= 3 else "red"  
                folium.Marker(location=[lat, lon],  
                            popup=(f"<b>Nombre:</b> {row['name']}<br>"  
                                    f"<b>Categoría:</b> {row['restaurante_categoria']}<br>"  
                                    f"<b>Rating Promedio:</b> {row['avg_rating']}<br>"  
                                    f"<b>Número de Reseñas:</b> {row['num_of_reviews']}"),  
                            tooltip=f"{row['name']} ({row['avg_rating']} ⭐)",  
                            icon=folium.Icon(color=color)).add_to(marker_cluster)  

# Añadir el header dentro del contenedor  
st.header(f"Mapa de Locales en {selected_state}")  

# Renderizar el mapa dentro del contenedor  
st_map = st_folium(m, width="100%", height=500)  

clicked_lat, clicked_lon = None, None  

# Obtener coordenadas del clic  
if st_map and 'last_clicked' in st_map and st_map['last_clicked']:  
    clicked_location = st_map['last_clicked']  
    clicked_lat = clicked_location['lat']  
    clicked_lon = clicked_location['lng']  
else:  
    st.sidebar.markdown("<p style='font-size: 20px;'>Haz clic en el mapa para obtener información de los locales cercanos.</p>", unsafe_allow_html=True)  

st.markdown(f"""  
<p style="font-size: 20px;">  
    Coordenadas seleccionadas: Latitud: {clicked_lat}, Longitud: {clicked_lon}  
</p>  
""", unsafe_allow_html=True)  

col1, col2, = st.columns([1,1])

with col1:# Verificación antes de predecir clúster  
        cluster = None  

        if clicked_lat is not None and clicked_lon is not None:  
            # Predecir el clúster según las coordenadas seleccionadas  
            cluster = kmeans_model.predict([[clicked_lat, clicked_lon]])[0]  

        # Cargar datos del clúster solo si `cluster` no es None  
        if cluster is not None:  
            try:  
                # Cargar datos de clúster desde un archivo CSV  
                df_model = pd.read_csv('df_model_cluster.csv')  

                # Filtrar datos para el clúster seleccionado  
                cluster_data = df_model[df_model['cluster'] == cluster]  

                if cluster_data.empty:  
                    st.warning("No hay datos disponibles para el clúster seleccionado.")  
                else:  
                    # Obtener estadísticas del clúster  
                    stats = {  
                        "median_rating": cluster_data['avg_rating'].median(),  # Mediana de calificaciones  
                        "median_reviews": cluster_data['num_of_reviews'].median(),  # Mediana de reseñas  
                        "most_common_services": {  
                            "CreditCards": cluster_data['CreditCards'].mode(),  # Sacar el primer valor de la moda  
                            "OutdoorSeating": cluster_data['OutdoorSeating'].mode(),  
                            "TakeOut": cluster_data['TakeOut'].mode()  
                        }  
                    }  
                    # Mostrar estadísticas de la zona  
                    st.header("Características de la zona")

                    st.markdown(f"<p style='font-size: 20px;'><strong>- Mediana de Calificaciones:</strong> {stats['median_rating']}</p>", unsafe_allow_html=True)  
                    st.markdown(f"<p style='font-size: 20px;'><strong>- Mediana de Reseñas:</strong> {stats['median_reviews']}</p>", unsafe_allow_html=True)  

                    st.markdown("<p style='font-size: 20px;'><strong>- Servicios más comunes:</strong></p>", unsafe_allow_html=True)  
                    st.write(stats['most_common_services'])
                    # Análisis de categorías más comunes  
                    categorical_columns = [col for col in cluster_data.columns if col.startswith("cat_")]  
                    if categorical_columns:  
                        df_categorias = cluster_data[categorical_columns + ["cluster"]]  
                        categorias_por_cluster = df_categorias.groupby("cluster").sum()  

                        # Identificar la categoría más común por clúster  
                        categorias_por_cluster["categoria_mas_comun"] = categorias_por_cluster.idxmax(axis=1)  

                        # Graficar categorías de restaurante en el clúster  
                        row = categorias_por_cluster.loc[cluster]  
                        st.subheader("Distribución de Categorías en la zona")  
                        fig, ax = plt.subplots()  
                        ax.barh(categorical_columns, row[categorical_columns])  
                        ax.set_title(f"Categorías de Restaurante en la zona {cluster}")  
                        ax.set_ylabel("Categorías")  
                        ax.set_xlabel("Frecuencia")  
                        st.pyplot(fig)  
                    else:  
                        st.warning("No se encontraron columnas de categorías para este clúster.")  
            except FileNotFoundError:  
                st.error("El archivo 'df_model_cluster.csv' no se encontró. Verifica que el archivo esté en el directorio correcto.")  
        else:  
            st.warning("Haz clic en el mapa para ver información de la zona.")
        

with col2:
        # Encabezado principal
        st.header("Locales en la ubicación seleccionada")
        # Calcular distancias desde el punto seleccionado
        df_final['distance'] = df_final.apply(
            lambda row: geodesic((clicked_lat, clicked_lon), (row['latitude'], row['longitude'])).km, axis=1
            )
        # Filtrar locales dentro del radio seleccionado
        nearby_businesses = df_final[df_final['distance'] <= radius]

        if not nearby_businesses.empty:
            avg_rating = nearby_businesses['avg_rating'].mean()
            total_businesses = len(nearby_businesses)
            positive_sentiment_percentage = (
                (nearby_businesses['avg_rating'] >= 3).sum() / total_businesses * 100
                )
            # Métricas con formato
            st.markdown(f"<p style='font-size: 20px;'><strong>- Locales cercanos:</strong> {total_businesses}</p>", unsafe_allow_html=True)
            st.markdown(f"<p style='font-size: 20px;'><strong>- Promedio de rating:</strong> {avg_rating:.2f}</p>", unsafe_allow_html=True)
            st.markdown(f"<p style='font-size: 20px;'><strong>- Porcentaje de locales con rating positivo:</strong> {positive_sentiment_percentage:.2f}%</p>", unsafe_allow_html=True)

            # Mostrar los 10 mejores locales
            st.write("#### Caracteristicas de locales cercanos")
            top_locales = nearby_businesses.nlargest(10, 'avg_rating')
            st.dataframe(top_locales[['name', 'avg_rating', 'num_of_reviews', 'distance']])
        else:
            st.write("Haz clic para ver informacion de locales cercanos.")

        with st.container():
            st.markdown('<div class="custom-background">', unsafe_allow_html=True)
            st.header("Predicción de Éxito")  

            # Diccionario de categorías con nombres internos (que deben coincidir con las características del modelo)
            categories = {
                "cat_restaurante brasileño": "Restaurante Brasileño",
                "cat_restaurante caribeño": "Restaurante Caribeño",
                "cat_restaurante centroamericano": "Restaurante Centroamericano",
                "cat_restaurante colombiano": "Restaurante Colombiano",
                "cat_restaurante cubano": "Restaurante Cubano",
                "cat_restaurante dominicano": "Restaurante Dominicano",
                "cat_restaurante latinoamericano": "Restaurante Latinoamericano",
                "cat_restaurante mexicano": "Restaurante Mexicano",
                "cat_restaurante peruano": "Restaurante Peruano",
                "cat_restaurante venezolano": "Restaurante Venezolano"
            }

            # Crear la lista de opciones para el multiselect con los textos visibles
            options = list(categories.values())

            # Multiselect en Streamlit
            selected_categories = st.multiselect(
                "Selecciona las categorías de restaurante",
                options=options,
                default=["Restaurante Mexicano"],  # Opcional: el valor por defecto si lo deseas
            )

            # Diccionario de características (checkboxes)
            caracteristicas = {
                "CreditCards": st.checkbox('Tarjetas de Credito'),
                "OutdoorSeating": st.checkbox('Mesas al aire libre'),
                "TakeOut": st.checkbox('Take away'),
            }

            # Crear DataFrame para el modelo solo si las coordenadas están disponibles  
            if clicked_lat is not None and clicked_lon is not None:  
                # Asegurarse de que todas las columnas del modelo estén presentes
                nuevo_restaurante1 = pd.DataFrame([{
                    "latitude": clicked_lat,
                    "longitude": clicked_lon,
                    # Asegurar que todas las características de los checkboxes estén presentes
                    **{cat: 1 if selected else 0 for cat, selected in caracteristicas.items()},
                    # Mapear las categorías seleccionadas con las claves internas del diccionario `categories`
                    **{cat: 1 if cat in [key for key, value in categories.items() if value in selected_categories] else 0 
                    for cat in categories.keys()},
                    "hispanic_population_ratio": ratio_hispano,
                    "avg_income_normalized": avg_income,
                    "popularity_score": 5.0,  # Placeholder
                }])

                # Aplicar estilo CSS al botón
                st.markdown(
                    """
                    <style>
                    .stButton > button {
                        background-color: #9f9bab;  
                        color: white;              
                        padding: 10px 20px;        
                        border: none;              
                        cursor: pointer;          
                        transition: transform 0.2s, background-color 0.3s; 
                    }
                    .stButton > button:hover {
                        background-color: #7a738e; 
                        transform: scale(1.05);    
                    }
                    </style>
                    """,
                    unsafe_allow_html=True,
                )
                        # Botón para predicción  
                if st.button("Predecir éxito"):  
                    # Predicción  
                    prediccion = rf_model.predict(nuevo_restaurante1)[0]  
                    probabilidad = rf_model.predict_proba(nuevo_restaurante1)[0][1]  # Probabilidad de éxito  

                    # Mostrar resultados  
                    st.subheader("Resultados de la predicción")  
                    if prediccion == 1:  
                        st.markdown(f"<h3 style='color: green;'>El restaurante tendrá éxito con una probabilidad del {probabilidad * 100:.2f}%.</h3>", unsafe_allow_html=True)  
                    else:  
                        st.markdown(f"<h3 style='color: red;'>El restaurante no tendrá éxito con una probabilidad del {(1 - probabilidad) * 100:.2f}%.</h3>", unsafe_allow_html=True)




# Agregar tu contenido de Streamlit aquí

# Footer fijo en la parte inferior
st.markdown("""
    <style>
        footer {
            position: fixed;
            bottom: 0;
            width: 100%;
            text-align: center;
            color: #ccc;
            background-color: #200558;
            padding: 15px;
        }
    </style>
    <footer>
        Dashboard desarrollado por Data Pioneer | 
        Proyecto de Predicción de Exito para Restaurantes
    </footer>
    """, unsafe_allow_html=True)