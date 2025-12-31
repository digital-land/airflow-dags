.PHONY: local-mwaa-image

ifeq ($(ENVIRONMENT),)
ENVIRONMENT='development'
endif

init::
	pip install --upgrade pip
	pip install -r requirements/dev-requirements.txt

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


test:: test-integration test-acceptance

test-integration::
	python -m pytest tests/integration

test-acceptance::
	python -m pytest tests/acceptance

compile ::
	python -m piptools compile --output-file=requirements/requirements.txt requirements/requirements.in
	python -m piptools compile --output-file=requirements/dev-requirements.txt requirements/dev-requirements.in