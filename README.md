\# NYC Taxi ETL Pipeline



This project implements an end-to-end ETL pipeline that processes NYC taxi trip data and loads it into a PostgreSQL data warehouse.



\## Overview



The pipeline extracts raw trip data, applies data cleaning and transformations, and loads it into a structured schema using fact and dimension tables.



\## Features



\* Chunk-based processing for large datasets

\* Data cleaning and validation

\* Star schema design (fact \& dimensions)

\* Surrogate key mapping

\* Slowly Changing Dimension (SCD Type 2)



\## Tech Stack



\* Python (Pandas)

\* PostgreSQL

\* SQLAlchemy



\## Project Structure



\* `src/` → ETL logic (extract, transform, load)

\* `sql/` → schema creation

\* `requirements.txt` → dependencies



\## How to Run



1\. Install dependencies:

&#x20;  pip install -r requirements.txt



2\. Setup database:

&#x20;  python src/setup\_schema.py



3\. Run pipeline:

&#x20;  python src/main.py



\## Notes



The pipeline is designed to handle large datasets efficiently using chunk-based processing and incremental loading techniques.



