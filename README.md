# 📊 Portafolio de Análisis de Datos: Mercado Inmobiliario USA

¡Bienvenido! En este repositorio presento un proyecto integral de Ingeniería y Análisis de Datos utilizando **SQL Server** y **Power BI**.

## 🏗️ Fase 1: Ingeniería de Datos (SQL ETL)
He diseñado un proceso ETL (Extracción, Transformación y Carga) siguiendo una arquitectura de capas para organizar la información de precios de viviendas.

* **Capa Bronze (`brz_housing`):** Ingesta de datos crudos en formato texto para asegurar la captura total de la información.
* **Capa Silver (`slv_housing`):** Limpieza de datos y modelado dimensional. 
* **Modelo de Datos:** Implementé un **Esquema en Estrella ** creando tablas de dimensiones (`dimCity`, `dimTime`, `dimFeatures`) y una tabla de hechos (`fctSales`) para optimizar el rendimiento de las consultas y reportes.

> Puedes ver el script completo de SQL en este repositorio como: `Analisis_Inmobiliaria_UsaHousePrices.sql`.

---

## 📈 Fase 2: Visualización en Power BI
Con el modelo de datos optimizado en SQL, creé un dashboard interactivo para identificar hallazgos clave.

### 🖼️ Vista del Dashboard
![Captura del Dashboard](./dashboard_screenshot.png) 
*(Nota: Aquí subiremos tu imagen más adelante)*

### 💡 Hallazgos Clave
* **Análisis de Precios:** Identificación de las ciudades con mayor costo por pie cuadrado.
* **Filtros Interactivos:** Segmentación por condiciones de la vivienda y periodos de construcción.

---

## 🛠️ Herramientas Utilizadas
* **Base de Datos:** SQL Server Management Studio (SSMS).
* **Lenguaje:** T-SQL (Transact-SQL).
* **Visualización:** Power BI Desktop.
