{{ config(materialized='view') }}

-- This model acts as our "Source" by mounting the Parquet files
-- We handle the Windows path here once so it's easy to change later
select * from read_parquet('C:/Users/shahm/OneDrive/Desktop/Project/Project 1/Code/eurogrid-energy-lakehouse/data/silver/prices/*/*/*/*.parquet')