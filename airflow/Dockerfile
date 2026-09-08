# Astro Runtime 12.x = Airflow 2.10 + Python 3.12 (aligne l'image sur le socle local).
# Verifier la derniere version disponible : https://quay.io/repository/astronomer/astro-runtime?tab=tags
FROM quay.io/astronomer/astro-runtime:12.1.1

# Le package `ingestion` n'est pas dans le contexte de build Docker (celui-ci est
# limite au dossier airflow/) : il est monte a l'execution par
# docker-compose.override.yml, d'ou l'ajout du repertoire au PYTHONPATH.
ENV PYTHONPATH=/usr/local/airflow

# Chemins de stockage a l'interieur du conteneur (data/ est monte en volume).
ENV ECO2MIX_RAW_DIR=/usr/local/airflow/data/raw \
    ECO2MIX_DUCKDB_PATH=/usr/local/airflow/data/warehouse/eco2mix.duckdb
