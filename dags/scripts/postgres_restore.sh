#!/bin/bash
echo "Hello Airflow bash!"
curl https://github.com/digital-land/planning-data-design/raw/refs/heads/main/data/latest_backup.dump > postgres.dump
pg_restore --dbname=TBC --data-only postgres.dump