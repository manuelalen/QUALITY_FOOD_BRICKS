# QUALITY_FOOD_BRICKS
### *Arquitectura Metadata-Driven para el Bloqueo Automático de Calidad*

[![Databricks](https://img.shields.io/badge/Runtime-14.3+-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Format-Delta_Lake-00ADD8?logo=delta-lake&logoColor=white)](https://delta.io/)
[![Apache Spark](https://img.shields.io/badge/Engine-Spark_3.5-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)

## 📝 Descripción
Este proyecto implementa una solución de **Captura de Datos Modificados (CDC)** y **Streaming Estructurado** para la gestión de calidad en una multinacional industrial. La arquitectura sincroniza en tiempo real los veredictos de laboratorio con el inventario físico, garantizando la seguridad alimentaria y operativa mediante el bloqueo automático de pallets.

---

## 🏗️ Arquitectura de Datos
El sistema sigue un patrón **Metadata-Driven** (orientado a metadatos), lo que desacopla la lógica de negocio del motor de ejecución.

### 🧩 Componentes del Ecosistema
* 🥉 **Bronze / Raw (`FACT_QUALITY_SAMPLES`)**: Origen de datos en formato Delta que recibe los análisis de laboratorio.
* 🥇 **Gold (`FACT_PALLET_STOCK`)**: Tabla de inventario que refleja el estado real (Released/Blocked) de cada unidad de carga.
* **Control Table (`f_metadata_logics`)**: Tabla maestra que contiene las lógicas de `MERGE` en formato JSON, permitiendo actualizaciones de reglas sin desplegar nuevo código.
* **Streaming Engine**: Script PySpark que utiliza **DeltaTable API** para ejecutar operaciones atómicas e incrementales.

---

## ⚙️ Configuración de Metadatos
La lógica de negocio reside en la columna `LOGIC_PAYLOAD` mediante una estructura JSON estandarizada:

```json
[
    {
        "action": "update",
        "condition": "source.FINAL_FAIL_IND = 1 AND target.STATUS_IND != 'BLOCKED_QUALITY'",
        "set": {"STATUS_IND": "'BLOCKED_QUALITY'"}
    },
    {
        "action": "update",
        "condition": "source.FINAL_FAIL_IND = 0 AND target.STATUS_IND != 'RELEASED'",
        "set": {"STATUS_IND": "'RELEASED'"}
    }
]
```

## 🚀 Implementación Técnica
1. Gestión de Estados con Unity Catalog
El proyecto utiliza Unity Catalog Volumes para el almacenamiento de checkpoints, cumpliendo con las políticas de seguridad que deshabilitan el DBFS raíz público:
path: (`/Volumes/workspace/global_quality_db/mis_checkpoints/`)**

2. Optimización de Cómputo
Se utiliza el trigger (`availableNow=True`)**. Esto permite:

* Procesamiento incremental de todos los datos disponibles.

* Reducción de costes al no requerir clústeres encendidos 24/7.

* Compatibilidad total con clústeres tipo Shared y Serverless.

## ⚒ Ejemplo de blockedo con `FAIL_IND = 1`

![Resultado del ejemplo(images/result.png)]
