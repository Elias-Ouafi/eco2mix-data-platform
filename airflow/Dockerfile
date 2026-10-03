# Astro Runtime 12.x = Airflow 2.10 + Python 3.12 (aligne l'image sur le socle local).
# Verifier la derniere version disponible : https://quay.io/repository/astronomer/astro-runtime?tab=tags
FROM quay.io/astronomer/astro-runtime:12.1.1

# dbt dans un environnement virtuel dedie : ses dependances epinglees entrent en
# conflit avec celles d'Airflow. Les DAGs l'appellent par son chemin absolu.
COPY dbt-requirements.txt /tmp/dbt-requirements.txt
RUN python -m venv /usr/local/airflow/dbt_venv \
    && /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir -r /tmp/dbt-requirements.txt

# Les packages `ingestion` et `reporting` et le projet dbt ne sont pas dans le contexte de build
# Docker (celui-ci est limite au dossier airflow/) : ils sont montes a
# l'execution par docker-compose.override.yml, d'ou l'ajout au PYTHONPATH.
ENV PYTHONPATH=/usr/local/airflow

# Chemins de stockage a l'interieur du conteneur (data/ est monte en volume).
ENV ECO2MIX_BRONZE_DIR=/usr/local/airflow/data/bronze \
    ECO2MIX_DUCKDB_PATH=/usr/local/airflow/data/warehouse/eco2mix.duckdb \
    ECO2MIX_REPORTS_DIR=/usr/local/airflow/data/reports \
    MPLCONFIGDIR=/tmp/matplotlib

# Identifiants de l'API RTE (calendrier Tempo) : jamais dans l'image. Astro
# injecte le fichier airflow/.env (gitignore) dans les conteneurs ; y placer
# ECO2MIX_RTE_CLIENT_ID et ECO2MIX_RTE_CLIENT_SECRET.

# Le projet dbt est monte en lecture seule : ses artefacts vont dans /tmp.
ENV DBT_PROFILES_DIR=/usr/local/airflow/dbt \
    DBT_TARGET_PATH=/tmp/dbt/target \
    DBT_LOG_PATH=/tmp/dbt/logs \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false
