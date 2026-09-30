# Zomato End-to-End Data Engineering Pipeline

An end-to-end data engineering project built using the Zomato dataset to learn and implement a modern data pipeline.

## Project Overview

The goal of this project is to build a complete data pipeline that ingests raw Zomato data, transforms and cleans it, and creates analytics-ready datasets.

The project is being developed incrementally as I learn and implement each component of the data engineering workflow.

## Tech Stack

- Snowflake
- dbt
- Python
- SQL

Additional technologies will be added as the pipeline develops.

## Current Progress

- [x] Set up Snowflake database and schemas
- [x] Created RAW tables
- [x] Loaded raw datasets into Snowflake
- [x] Initialized dbt project
- [ ] Configure dbt with Snowflake
- [ ] Build staging models
- [ ] Build marts
- [ ] Add data quality tests
- [ ] Add snapshots
- [ ] Add orchestration

## Planned Data Flow

Raw Data → Snowflake RAW → dbt Staging → dbt Marts → Analytics

## Project Structure

    zomato/
    ├── analyses/
    ├── macros/
    ├── models/
    ├── seeds/
    ├── snapshots/
    ├── tests/
    └── dbt_project.yml

## Status

🚧 Project currently under development.
