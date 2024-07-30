.PHONY: local-mwaa-image

ifeq ($(ENVIRONMENT),)
ENVIRONMENT='development'
endif

local-mwaa-image::
	rm -rf var
	mkdir -p var
	cd var; \
	git clone https://github.com/aws/aws-mwaa-local-runner.git; \
	cd aws-mwaa-local-runner; \
	./mwaa-local-env build-image;

dags/config.json::
	python bin/generate_dag_config.py --env=$(ENVIRONMENT)

compose-up::
	docker compose up -d --build

compose-down::
	docker compose down  --rmi 'all'