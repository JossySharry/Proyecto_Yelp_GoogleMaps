# <h1 align=center> Guía para Contribuir al Proyecto </h1>
¡Gracias por tu interés en contribuir a este proyecto! Sigamos este flujo para mantener el trabajo organizado y eficiente. 😄

##  Cómo Contribuir 
1. Clona el repositorio:

bash<br>
Copiar código<br>
git clone https://github.com/JossySharry/Proyecto_Yelp_GoogleMaps.git


Ve al directorio del proyecto:
```bash
Copiar código
cd Proyecto_Yelp_GoogleMaps

2. Crea una nueva rama para tu funcionalidad o corrección: Utiliza un nombre claro para describir tu trabajo.

```bash

Copiar código

git checkout -b feature/mi-funcionalidad

Ejemplo:

```bash
Copiar código
git checkout -b feature/agregar-filtro

3. Realiza tus cambios:

- Asegúrate de que los cambios cumplen con el objetivo del proyecto.
- Verifica que tu código esté bien formateado y funcionando.

4. Guarda tus cambios y súbelos al repositorio remoto: Agrega los cambios:

```bash
Copiar código
git add .

Confirma los cambios con un mensaje descriptivo:

```bash
Copiar código
git commit -m "Descripción breve de los cambios"

Sube la rama:

```bash
Copiar código
git push origin feature/mi-funcionalidad

5. Crea un Pull Request (PR):

- Ve a la página del repositorio en GitHub.
- Busca tu rama y selecciona la opción "Compare & pull request".
- Agrega una descripción detallada de los cambios realizados.

## Convenciones
- Nombres de Ramas: Usa el formato feature/mi-funcionalidad.
- Commits: Escribe mensajes claros y concisos. Ejemplo:
    - ✅ "Agregar funcionalidad para buscar lugares por categoría"
    - ❌ "Cambios varios"

## Reglas del Proyecto 
1. No hagas cambios directos a la rama main.
Todas las contribuciones deben pasar por un Pull Request.

2. Revisiones:
Cada PR será revisado antes de ser fusionado. Si hay comentarios, realiza las modificaciones necesarias.

3. Conflictos:
Si encuentras conflictos al fusionar, resuélvelos localmente:

```bash
Copiar código
git pull origin main
git merge main
Luego, sube los cambios resueltos.

## 
¡Gracias por contribuir y ser parte de este proyecto! 🚀