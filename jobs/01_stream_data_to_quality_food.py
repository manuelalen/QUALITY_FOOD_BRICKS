import json
from delta.tables import DeltaTable
import pyspark.sql.functions as F

def motor_metadata_streaming(micro_batch_df, batch_id, process_name):
    # Si no hay datos nuevos, salimos
    if micro_batch_df.count() == 0:
        return

    # 1. Leer los metadatos desde la tabla de control
    try:
        metadata = spark.table("workspace.global_quality_db.f_metadata_logics") \
            .filter((F.col("PROCESS_NAME") == process_name) & (F.col("ACTIVE") == True)) \
            .first()
    except Exception as e:
        print(f"Error leyendo metadatos: {e}")
        return

    if not metadata:
        print(f"Proceso '{process_name}' inactivo o no encontrado.")
        return

 
    target_name = metadata["TARGET"]
    source_name = metadata["SOURCE"] 
    join_key = metadata["JOIN_KEY"]
    logicas = json.loads(metadata["LOGIC_PAYLOAD"])

    micro_batch_df.createOrReplaceTempView("raw_stream")
    df_source = spark.sql("""
        SELECT BAT_COD, MAX(FAIL_IND) AS FINAL_FAIL_IND 
        FROM raw_stream 
        GROUP BY BAT_COD
    """)

    df_source.createOrReplaceTempView(source_name)


    target_table = DeltaTable.forName(spark, target_name)
    

    merge_builder = target_table.alias("target").merge(
        source=df_source.alias("source"),
        condition=f"target.{join_key} = source.{join_key}"
    )

    # 5. Iterar sobre el JSON e inyectar las lógicas
    for regla in logicas:
        if regla["action"] == "update":
            set_expr = {k: F.lit(v.replace("'", "")) for k, v in regla["set"].items()}
            
            merge_builder = merge_builder.whenMatchedUpdate(
                condition=regla["condition"],
                set=set_expr
            )
    merge_builder.execute()
    print(f"✅ Proceso {process_name} ejecutado con éxito (Batch ID: {batch_id})")


calidad_stream = (spark.readStream
    .format("delta")
    .option("ignoreChanges", "true")
    .table("workspace.global_quality_db.fact_quality_samples")
)

automatizacion_job = (calidad_stream.writeStream
    .foreachBatch(lambda df, id: motor_metadata_streaming(df, id, "PALLET_QUALITY_BLOCKER"))
    .trigger(availableNow=True)
    .option("checkpointLocation", "/Volumes/workspace/global_quality_db/mis_checkpoints/metadata_v3")
    .start()
)

automatizacion_job.awaitTermination()
