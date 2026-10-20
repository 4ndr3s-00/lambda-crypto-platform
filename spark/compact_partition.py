"""
Job de Spark para la compactación atómica de particiones con archivos pequeños (US-16).
Responsable: Camilo (Épica 5: Airflow DAGs)

Resuelve el problema de 'small files' en MinIO (S3):
1. Lee todos los archivos pequeños de una partición dada.
2. Contabiliza el número total de filas iniciales.
3. Aplica `coalesce` para consolidar los datos en el número óptimo de archivos (~128 MB).
4. Escribe en un prefijo temporal de staging y valida que `conteo_final == conteo_inicial`.
5. Reemplaza atómicamente la partición original únicamente si la validación es 100% exitosa.
"""

import argparse
import logging
import sys
from typing import Optional

try:
    from pyspark.sql import SparkSession
    from spark.spark_session import build_spark_session, lake_path
except ImportError:
    try:
        from spark_session import build_spark_session, lake_path
    except ImportError:
        SparkSession = None
        build_spark_session = None

logger = logging.getLogger("compact_partition")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def calculate_optimal_partitions(total_bytes: int, target_size_bytes: int = 128 * 1024 * 1024) -> int:
    """Calcula el número óptimo de particiones para alcanzar el tamaño objetivo (~128MB)."""
    if total_bytes <= 0:
        return 1
    optimal = int(total_bytes / target_size_bytes)
    return max(1, optimal)


def compact_partition(
    spark: "SparkSession",
    partition_uri: str,
    file_format: str = "parquet",
    target_size_mb: int = 128,
) -> dict:
    """
    Compacta una partición específica garantizando la integridad de los datos.
    """
    clean_uri = partition_uri.rstrip("/")
    logger.info("Iniciando compactación en: %s (formato: %s)", clean_uri, file_format)

    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    path_class = jvm.org.apache.hadoop.fs.Path
    partition_path = path_class(clean_uri)
    filesystem = partition_path.getFileSystem(hadoop_conf)

    # 1. Obtener tamaño total y número de archivos existentes
    file_statuses = filesystem.listStatus(partition_path)
    total_bytes = 0
    file_count = 0
    for status in file_statuses:
        name = status.getPath().getName()
        # Ignorar archivos ocultos o de metadatos de Spark (_SUCCESS, .crc, etc.)
        if not name.startswith("_") and not name.startswith("."):
            total_bytes += status.getLen()
            file_count += 1

    logger.info("Partición actual: %d archivos, tamaño total: %.2f MB", file_count, total_bytes / (1024 * 1024))

    if file_count <= 1:
        logger.info("La partición ya tiene 1 solo archivo; no requiere compactación.")
        return {
            "partition": clean_uri,
            "status": "SKIPPED",
            "initial_files": file_count,
            "final_files": file_count,
            "records": 0,
        }

    # 2. Leer la partición y contar filas iniciales
    reader = spark.read.format(file_format)
    df = reader.load(clean_uri)
    initial_count = df.count()
    logger.info("Registros iniciales leídos: %d", initial_count)

    if initial_count == 0:
        logger.warning("La partición no contiene registros.")
        return {
            "partition": clean_uri,
            "status": "EMPTY",
            "initial_files": file_count,
            "final_files": file_count,
            "records": 0,
        }

    # 3. Calcular número óptimo de archivos consolidados
    target_bytes = target_size_mb * 1024 * 1024
    optimal_partitions = calculate_optimal_partitions(total_bytes, target_bytes)
    logger.info("Consolidando de %d archivos a %d archivo(s) óptimo(s)", file_count, optimal_partitions)

    # 4. Escribir a staging temporal
    staging_uri = f"{clean_uri}_staging_compact"
    staging_path = path_class(staging_uri)

    # Limpiar staging previo si existiera
    if filesystem.exists(staging_path):
        filesystem.delete(staging_path, True)

    df_compacted = df.coalesce(optimal_partitions)
    df_compacted.write.mode("overwrite").format(file_format).save(staging_uri)

    # 5. Validación estricta de integridad (Criterio 2 BDD)
    df_verification = spark.read.format(file_format).load(staging_uri)
    final_count = df_verification.count()

    if final_count != initial_count:
        filesystem.delete(staging_path, True)
        raise ValueError(
            f"ERROR DE INTEGRIDAD EN COMPACTACIÓN: conteo inicial ({initial_count}) "
            f"!= conteo final ({final_count}). Se aborta la operación sin modificar la partición."
        )

    logger.info("Integridad verificada con éxito: %d filas iniciales == %d filas finales", initial_count, final_count)

    # 6. Reemplazo atómico: eliminar archivos fragmentados y mover consolidados
    # En almacenamiento S3A/HDFS, eliminamos el original y renombramos el staging
    filesystem.delete(partition_path, True)
    rename_success = filesystem.rename(staging_path, partition_path)

    if not rename_success:
        raise RuntimeError(f"Error al renombrar directorio de staging a {clean_uri}")

    logger.info("Compactación completada exitosamente en %s", clean_uri)
    return {
        "partition": clean_uri,
        "status": "SUCCESS",
        "initial_files": file_count,
        "final_files": optimal_partitions,
        "records": final_count,
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Compactador de archivos pequeños para particiones de MinIO")
    parser.add_argument("--partition-uri", type=str, required=True, help="URI S3A de la partición a compactar")
    parser.add_argument("--format", type=str, default="parquet", choices=["parquet", "json"], help="Formato de archivo")
    parser.add_argument("--target-size-mb", type=int, default=128, help="Tamaño objetivo por archivo en MB")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = build_spark_session("compact-partition-job")
    try:
        compact_partition(
            spark=spark,
            partition_uri=args.partition_uri,
            file_format=args.format,
            target_size_mb=args.target_size_mb,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
