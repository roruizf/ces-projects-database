# Manual de Usuario - Base de Datos Proyectos CES

## Introducción
Esta herramienta permite extraer (scrapear) automáticamente información de proyectos desde el sitio web de Certificación Edificio Sustentable (CES). El sistema descarga los datos, los procesa y genera una base de datos consolidada en formato CSV.

## Requisitos Previos
1.  **Python 3.8+**: Asegúrate de tener Python instalado.
2.  **Conexión a Internet**: Necesaria para acceder al sitio web de CES.

## Instalación

1.  Clonar el repositorio o descargar el código.
2.  Instalar las dependencias:
    ```bash
    pip install -r requirements.txt
    ```

## Uso

### 1. Actualizar la Base de Datos de Proyectos
Para descargar la información más reciente de todos los proyectos y generar un archivo consolidado:

```bash
python scrape_projects.py
```

**¿Qué hace este comando?**
1.  Recorre todas las categorías de certificación (`en-proceso`, `pre-certificacion`, etc.).
2.  Descarga los detalles de cada proyecto.
3.  Guarda archivos temporales en `data/raw/` con la fecha de ejecución.
4.  Genera automáticamente un archivo consolidado final llamado `[CES]_Projects_Full_List-YYYY_MM_DD.csv`.

### 2. Procesar Registro de Asesores
Si dispones de un archivo actualizado de asesores (ej: `Registro_AsesoresCES_v41.2022.csv`) y deseas limpiarlo:

1.  Asegúrate de que el archivo CSV original esté en la carpeta raíz.
2.  Ejecuta:
    ```bash
    python scrape_assessors.py
    ```
3.  El archivo limpio se guardará en `data/raw/[CES]_Registro_AsesoresCES_Full_List-YYYY_MM_DD.csv`.

## Solución de Problemas

-   **Error de conexión**: Si el script falla por problemas de red, reintentará automáticamente. Si persiste, verifica tu conexión.
-   **Interrupción**: Si detienes el script (`Ctrl+C`), puedes volver a ejecutarlo. Los archivos ya descargados no se reanudan, pero el proceso comenzará de nuevo.

## Estructura de Datos
El archivo final consolidado contiene columnas como:
-   `project_name`: Nombre del proyecto.
-   `status`: Estado de certificación.
-   `mandante`, `arquitecto`, `asesor`: Información de los participantes.
-   `puntaje_obtenido`: Puntaje final de la certificación.
