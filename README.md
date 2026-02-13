# CES Projects Database Scraper

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Pandas](https://img.shields.io/badge/pandas-data%20processing-orange)
![Status](https://img.shields.io/badge/status-active-success)

## Descripción del Proyecto

Este proyecto es una herramienta automatizada diseñada para extraer, procesar y consolidar información sobre proyectos de **Certificación Edificio Sustentable (CES)** en Chile. 

El script recorre el sitio web oficial, captura datos detallados de cada proyecto (como mandante, arquitecto, puntaje, ubicación, etc.) y genera una base de datos estructurada en formato CSV, lista para análisis o integración con herramientas de Business Intelligence (como Power BI).

### Características Principales
- **Scraping Automatizado**: Recorre múltiples categorías (En Proceso, Pre-Certificación, Certificación, Sello Plus).
- **Procesamiento Concurrente**: Utiliza 5 workers paralelos para acelerar la descarga de datos (reducción de tiempo del 80%).
- **Consolidación de Datos**: Une automáticamente todos los registros en un único archivo maestro.
- **Robustez**: Manejo de errores de conexión y sistema de reintentos.
- **Logging**: Registro detallado de la ejecución para fácil depuración.
- **Limpieza Automática**: Elimina archivos intermedios al finalizar.

## Estructura del Proyecto

```
ces-projects-database/
├── data/
│   └── raw/                  # Archivos CSV generados (salida del scraper)
├── utils/
│   └── scraper.py            # Módulo con lógica de scraping y manejo de requests
├── scrape_projects.py        # Script principal para proyectos (Entry point)
├── scrape_assessors.py       # Script para limpiar base de datos de asesores
├── requirements.txt          # Dependencias del proyecto
├── README.md                 # Documentación general
└── MANUAL_DE_USUARIO.md      # Guía detallada de uso
```

## Instalación y Uso

1.  **Clonar el repositorio**:
    ```bash
    git clone <url-del-repo>
    cd ces-projects-database
    ```

2.  **Instalar dependencias**:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Ejecutar el Scraper de Proyectos**:
    ```bash
    python scrape_projects.py
    ```
    Este comando descargará la data actualizada y generará un archivo consolidado en `data/raw/[CES]_Projects_Full_List-YYYY_MM_DD.csv`.

Para más detalles, consulta el [Manual de Usuario](MANUAL_DE_USUARIO.md).

## Tecnologías Utilizadas
-   **Python**: Lenguaje principal.
-   **Requests & lxml**: Para la extracción de datos HTML.
-   **Pandas**: Para la manipulación, limpieza y estructuración de los datos.
-   **Logging**: Para el registro de eventos y monitoreo.

## Autor
Desarrollado como una solución eficiente para el monitoreo de proyectos sustentables en Chile.