# airflow-dags
A repository to store airflow dags for use with MWAA. they key output is an action that pushed the dags to s3.

## Dependencies

- localstack
- docker
- a local image of MWAA-local-runner. The code to produce this is in the makefile.