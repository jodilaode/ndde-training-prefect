from prefect import flow, task

@task
def extract_data(table):
    # Import di sini, bukan di atas
    import pandas as pd 
    print(f"Extracting {table} using pandas version {pd.__version__}")
    return f"data_{table}"

@task
def transform_data(raw_data, table):
    import pandas as pd
    import sqlalchemy
    # Logika Anda...
    return f"cleaned_{table}"

@flow(name="Final pipeline")
def main_flow(tables: list):
    for table in tables:
        raw = extract_data(table)
        cleaned = transform_data(raw, table)
        # ... rest of your code

if __name__ == "__main__":
    main_flow.from_source(
        source="https://github.com/jodilaode/ndde-training-prefect.git",
        entrypoint="etl_flow_jo.py:main_flow"
    ).deploy(
        name="training-ndde-deployment",
        work_pool_name="training-ndde",
        job_variables={
            "pip_install": ["pandas", "sqlalchemy", "pyspark"]
        }
    )
