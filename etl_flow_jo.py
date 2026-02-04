from prefect import flow, task

@task(retries=2)
def extract_data(table):
    # Lazy import untuk menghindari ModuleNotFoundError saat deployment
    import pandas as pd
    print(f"--- Extracting data for table: {table} ---")
    # Logika extract Anda di sini
    return f"raw_data_{table}"

@task
def transform_data(raw_data, table):
    import pandas as pd
    print(f"--- Transforming data for table: {table} ---")
    # Logika transform Anda di sini
    return f"cleaned_data_{table}"

@task
def load_to_hdfs(cleaned_data, table):
    print(f"--- Loading {table} to HDFS ---")
    return True

@task
def sync_to_hive(table):
    print(f"--- Syncing {table} to Hive ---")
    return True

@task
def load_to_datamart():
    print("--- ALL TABLES SYNCED. Building Datamart... ---")

@flow(name="Final pipeline")
def main_flow(tables: list):
    sync_results = []
    
    for table in tables:
        raw = extract_data(table)
        cleaned = transform_data(raw, table)
        l_hdfs = load_to_hdfs(cleaned, table)
        # Hive sync menunggu HDFS load selesai
        sync_status = sync_to_hive(table, wait_for=[l_hdfs])
        sync_results.append(sync_status)

    # Datamart hanya jalan setelah semua tabel di sync_results selesai
    load_to_datamart(wait_for=sync_results)

if __name__ == "__main__":
    # Cukup jalankan flow secara lokal untuk test
    # main_flow(tables=['produk'])
    
    # Untuk deploy, kita akan pakai CLI saja sekarang supaya lebih stabil
    print("Gunakan command 'prefect deploy' di terminal untuk mendaftar.")
