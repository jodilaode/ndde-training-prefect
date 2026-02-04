#!/usr/bin/env python
# coding: utf-8

# ## Module 4: Prefect Workflow Basics
# 
# Memasuki DAY 2, fokus kita bergeser dari penulisan skrip isolasi ke Data Orchestration. Kita akan menggunakan Prefect, salah satu modern workflow orchestrator paling populer yang sangat ramah terhadap ekosistem Python.

# ### Script 1: Basic Flow (Modular Data Ingestion)
# Script ini mendemonstrasikan cara membungkus fungsi data engineering (dari Day 1) ke dalam Prefect Tasks.

# In[1]:


import pandas as pd
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from pyspark.sql import SparkSession
from prefect import task, flow, get_run_logger
from datetime import timedelta
from urllib.parse import quote_plus


# ==========================================
# 1. CONFIGURATION & UTILS
# ==========================================

# Config and create engine MySQL
user = 'cbi'
password = quote_plus('Cbi123!@#')
host = '103.93.236.91'
port = '3306'
db = 'ecommerce_dummy'

db_uri = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"

def mysql_engine():
    return create_engine(
        db_uri,
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_pre_ping=True
    )

# Config and create engine Spark
hdfs_base_path = "/training/warehouse/jo"
#.config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
def get_spark():
    return SparkSession.builder \
        .appName("final pipeline") \
        .enableHiveSupport() \
        .getOrCreate()
# ==========================================
# 2. TASKS (The Units of Work)
# ==========================================

@task(
    name="Extract Data Dari MySQL",
    retries=3,
    retry_delay_seconds=10
)
def extract_data(table_name: str):
    logger = get_run_logger()
    logger.info("Extract data from table Transaksi Detail")
    engine = mysql_engine()
    query = f"select * from {table_name}"

    try:
        with engine.connect() as conn:
            # Chunking size
            chunks = pd.read_sql(text(query), conn, chunksize=1000000)
            df = pd.concat(chunks)
            logger.info(f"Berhasil extract table {table_name}: {len(df)} baris.")
            return df
    except Exception as e:
        logger.error(f"Gagal extract table {table_name}: {e}")
        raise

@task(name="Transform Data")
def transform_data(df: pd.DataFrame, table_name: str):
    """Task untuk membersihkan data dan menambah metadata partisi."""
    logger = get_run_logger()

    # Metadata partisi harian
    df['update_date'] = pd.Timestamp.now().strftime('%Y-%m-%d')
    logger.info(f"Transformasi selesai untuk table {table_name}.")
    return df

@task(name="Load to HDFS")
def load_to_hdfs(df: pd.DataFrame, table_name: str):
    """Spark Session dibuat di dalam task ini."""
    logger = get_run_logger()
    spark = get_spark()
    full_path = f"{hdfs_base_path}/{table_name}"

    try:
        spark_df = spark.createDataFrame(df)
        spark_df.write \
            .mode("overwrite") \
            .partitionBy("update_date") \
            .parquet(full_path)
        logger.info(f"Data {table_name} berhasil disimpan di HDFS: {full_path}")
        return full_path
    except Exception as e:
        logger.error(f"Gagal simpan {table_name} ke HDFS: {e}.")
        raise

@task(name="Sync to Hive")
def sync_to_hive(table_name: str):
    """Merefresh tabel ke Hive. Contoh script dilakukan dengan catatan melakukan create table external langsung di hive"""
    logger = get_run_logger()
    spark = get_spark()
    hive_table_name = f"dw_{table_name}"
    # 2. Tulis ke Physical Location (HDFS)
    # Karena ini tabel External, kita cukup isi foldernya saja.
    try:      
        logger.info(f"Merefresh metadata untuk tabel: {hive_table_name}")
        #spark.sql(f"REFRESH TABLE {hive_table_name}")
        spark.sql(f"MSCK REPAIR TABLE {hive_table_name}")
        count = spark.sql(f"SELECT COUNT(*) FROM {hive_table_name}").collect()[0][0]
        logger.info(f"Sukses! Tabel {hive_table_name} sekarang memiliki {count} baris.")
    except Exception as e:
        logger.error(f"Gagal refresh dengan Hive: {e}.")
        raise

@task(name="Load to Datamart")
def load_to_datamart():
    logger = get_run_logger()
    spark = get_spark()

    try:
        logger.info("Memulai proses pembuatan Datamart...")
        # Menggunakan Spark SQL untuk join tabel-tabel Hive
        datamart_df = spark.sql("""
            SELECT
                last_day(concat(tahun, '-', lpad(bulan, 2, '0'), '-01')) tanggal,
    			nama_pelanggan,
    			kategori_produk,
    			nama_produk,
    			total,
                update_date
            FROM (
                SELECT
                	YEAR(a.Tanggal_Transaksi) tahun,
                	MONTH(a.Tanggal_Transaksi) bulan,
                	p.Nama_Lengkap nama_pelanggan,
                	k.Nama_Kategori kategori_produk,
                	pp.Nama_Produk nama_produk,
                	SUM(a.Total_Pembayaran) total,
                    a.update_date
                FROM dw_transaksi_header a
                LEFT JOIN dw_transaksi_detail td 
                	ON a.ID_Transaksi = td.ID_Transaksi
                LEFT JOIN dw_pelanggan p 
                	ON a.ID_Pelanggan = p.ID_Pelanggan
                LEFT JOIN dw_produk pp
                	ON td.ID_Produk = pp.ID_Produk
                LEFT JOIN dw_kategori k
                	ON pp.ID_Kategori = k.ID_Kategori
                WHERE a.Status_Pesanan = 'Selesai'
                GROUP BY
                	year(a.Tanggal_Transaksi),
                	month(a.Tanggal_Transaksi),
                	p.Nama_Lengkap,
                	k.Nama_Kategori,
                	pp.Nama_Produk,
                    a.update_date
            )x
        """)
        # Simpan hasil join ke tabel Datamart (Tabel Managed)
        # Gunakan overwrite agar datamart selalu berisi data terbaru yang sudah digabung
        datamart_df.write \
            .mode("overwrite") \
            .format("parquet") \
            .saveAsTable("dm_sales")
        count = spark.sql("SELECT COUNT(*) FROM dm_sales").collect()[0][0]
        logger.info(f"Datamart berhasil dibuat! total {count} baris di dm_sales.")

    except Exception as e:
        logger.error(f"Gagal membuat datamart: {e}")
        raise


# ==========================================
# 3. FLOW (Orchestration)
# ==========================================

@flow(
    name="Final pipeline"
)
def main_flow(tables: list):
    for table in tables:
        raw_data = extract_data(table)
        cleaned_data = transform_data(raw_data, table)
        load_to_hdfs(cleaned_data, table)
        sync_to_hive(table)

    # Dipanggil setelah semua table source di sync ke hive 
    load_to_datamart()

if __name__ == "__main__":
    #main_flow(tables=['transaksi_detail','transaksi_header','produk','pelanggan','kategori'])
    main_flow.from_source(
        # GANTI BAGIAN INI: Masukkan URL HTTPS repository GitHub Anda
        source="https://github.com/jodilaode/ndde-training-prefect.git",    
        # Tetap sama: nama_file.py:nama_fungsi_flow
        entrypoint="etl_flow_jo.py:main_flow" 
    ).deploy(
        name="training-ndde-deployment",
        work_pool_name="my-managed-pool", # Managed pool (Prefect Cloud)
        job_variables={
            "pip_packages": ["pandas", "sqlalchemy", "pyspark","pymysql"]
        },
        parameters={
            "tables": [
                'transaksi_detail',
                'transaksi_header',
                'produk',
                'pelanggan',
                'kategori'
            ]
        },
    )


