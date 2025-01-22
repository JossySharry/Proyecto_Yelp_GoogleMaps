# Modelo de Machine Learning

Esta carpeta `ML` contiene dos modelos de Machine Learning desarrollados para un proyecto: Análisis del Mercado de Restaurantes Latinos en USA, el cual está enfocado agrupar los restaurantes latinos según su ubicación y características del mercado y además en predecir el éxito de apertura de restaurantes latinos en Estados Unidos. Los modelos proporcionan insights clave para la toma de decisiones estratégicas de nuestro cliente.

## Modelos Desarrollados

### 1. **Modelo No Supervisado de KMeans para Agrupar Restaurantes Latinos Según su Ubicación**
Este modelo no supervisado utiliza el algoritmo KMeans para agrupar restaurantes en función de su ubicación geográfica (latitud y longitud) y características relevantes del mercado.
El objetivo es entender la distribución de los restaurantes latinos en EE.UU. y explorar cómo las variables culturales y geográficas afectan su rendimiento.

### **Características del Modelo**
**Variables Usadas:**
- `latitude`: Latitud del restaurante.
- `longitude`: Longitud del restaurante.

**Características adicionales analizadas**:
- `avg_rating`: Calificaciones de los restaurantes.
- `num_of_reviews`: Número de reseñas de los restuarantes.
- `CreditCards`: Si el restaurante acepta tarjetas de crédito.
- `OutdoorSeating`: Si el restaurante ofrece asientos al aire libre.
- `TakeOut`: Si el restaurante ofrece servicio para llevar.
- `cat_restaurante brasileño`, `cat_restaurante caribeño`, `cat_restaurante centroamericano`, `cat_restaurante colombiano`, `cat_restaurante cubano`, `cat_restaurante dominicano`, `cat_restaurante latinoamericano`, `cat_restaurante mexicano`, `cat_restaurante peruano`, `cat_restaurante venezolano`: Categorías del tipo de restaurante.

### **Entrenamiento y Evaluación**
- **Algoritmo**: KMeans
- **Número de Clústeres**: Determinado mediante el codo de KMeans y Análisis de Silueta para k Óptimo.
- **Objetivo**: Agrupar restaurantes en clústeres que tengan patrones geográficos y de mercado similares.

#### **Código de Entrenamiento**:

```python
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

# Preparar los datos
X = df[['latitude', 'longitude']]

# Determinar el número óptimo de clústeres utilizando el método del codo
inertia = []
for i in range(1, 11):
    kmeans = KMeans(n_clusters=i, random_state=42)
    kmeans.fit(X)
    inertia.append(kmeans.inertia_)

# Graficar el codo
plt.plot(range(1, 11), inertia)
plt.title('Método del Codo para Determinar el Número de Clústeres')
plt.xlabel('Número de Clústeres')
plt.ylabel('Inercia')
plt.show()

# Aplicar KMeans con el número óptimo de clústeres (ya calculado)
kmeans = KMeans(n_clusters=4, random_state=42)
df['cluster'] = kmeans.fit_predict(X)

# Visualizar los clústeres
plt.scatter(df['longitude'], df['latitude'], c=df['cluster'], cmap='viridis')
plt.title('Distribución de Restaurantes Latinos Según Clústeres')
plt.xlabel('Longitud')
plt.ylabel('Latitud')
plt.show()

```
**El código detallado y explicado de este modelo se encuentra en el archivo `Modelo_kmean.ipynb`**

### 2. **Modelo de Random Forest para Predecir el Éxito de Apertura de Restaurantes Latinos**

Este modelo de clasificación utiliza un algoritmo Random Forest para predecir la probabilidad de éxito de apertura de un restaurante latino en EE.UU. El éxito se define a partir de varias características de entrada, como las condiciones geográficas, demográficas y económicas.

#### **Características del Modelo**
- **Variables Independientes**: 
  - `latitude`: Latitud del restaurante.
  - `longitude`: Longitud del restaurante.
  - `CreditCards`: Si el restaurante acepta tarjetas de crédito.
  - `OutdoorSeating`: Si el restaurante ofrece asientos al aire libre.
  - `TakeOut`: Si el restaurante ofrece servicio para llevar.
  - `cat_restaurante brasileño`, `cat_restaurante caribeño`, `cat_restaurante centroamericano`, `cat_restaurante colombiano`, `cat_restaurante cubano`, `cat_restaurante dominicano`, `cat_restaurante latinoamericano`, `cat_restaurante mexicano`, `cat_restaurante peruano`, `cat_restaurante venezolano`: Categorías del tipo de restaurante.
  - `hispanic_population_ratio`: Proporción de población hispánica en la zona.
  - `avg_income_normalized`: Ingreso promedio normalizado de la zona.
  - `popularity_score`: Puntuación de popularidad del restaurante.

#### **Entrenamiento y Evaluación**
- **Algoritmo**: Random Forest
- **Métricas**: Precisión, Recall, F1-Score, AUC, Validación cruzada y otros.
- **Objetivo**: Predecir si un restaurante tendrá éxito en su apertura basado en las características descritas.

#### **Código de Entrenamiento**:

```python
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score
from imblearn.over_sampling import SMOTE

# Preparar los datos
# Seleccionar las columnas relevantes para el modelo
X = df_model.drop(columns=['is_open'])  # Variables independientes, se eliminan columnas redudantes
y = df_model['is_open']  # Variable objetivo

# Dividir en conjuntos de entrenamiento (70%) y prueba (30%)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)

# Aplicar SMOTE para balancear las clases
smote = SMOTE(random_state=42)
X_train_balanced, y_train_balanced = smote.fit_resample(X_train, y_train)

# Crear el modelo de Random Forest
rf_model = RandomForestClassifier(n_estimators=50, max_depth=8, random_state=42)

# Entrenar el modelo
rf_model.fit(X_train_balanced, y_train_balanced)

# Evaluar el modelo
train_accuracy = accuracy_score(y_train, y_train_pred)
test_accuracy = accuracy_score(y_test, y_test_pred)

print("Resultados del Random Forest:")
print(f"Precisión en el conjunto de entrenamiento: {train_accuracy:.2f}")
print(f"Precisión en el conjunto de prueba: {test_accuracy:.2f}")
print("\nReporte de clasificación (conjunto de prueba):")
print(classification_report(y_test, y_test_pred))
```

**El código detallado y explicado de este modelo se encuentra en el archivo `Modelo_rf.ipynb`**
