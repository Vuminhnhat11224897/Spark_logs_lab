"""Reader helpers will be implemented when pipeline logic is wired."""
from __future__ import annotations
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

def read_csv(
    spark: SparkSession, 
    path: str | Path, 
    schema: StructType | None = None,
    header: bool = True, 
) -> DataFrame:
    reader = spark.read.option("header", "true" if header else "false")
    
    if schema is not None:
        reader = reader.schema(schema)
        
    return reader.csv(str(path))
    
def read_parquet(
    spark: SparkSession,    
    path: str | Path,
    schema: StructType | None = None,
) -> DataFrame:
    reader = spark.read

    if schema is not None:
        reader = reader.schema(schema)

    return reader.parquet(str(path))