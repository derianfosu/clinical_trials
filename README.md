# Data Cleansing and Ingestion Tool
CS-519 Project (Big Data Analytics - Data Integration)

Functions:
- Parsed XML files into cElementTree Data Type
- Automatic DB relations' attributes name and type identification
- Crash Management System (Rollback and Checkpoint - avoid missing data)
- Clean and Ingest parsed data to PostgreSQL DB

EXECUTING THE SCRIPT:
- update data.txt
- place XML files into folder data or specify path in python script
- RUN 519project.py

HOW TO CREATE OTHER RELATIONS
- update relations name in the script
- update primary key and its value in the script
- the rest should be automatically created based on the XML input files

