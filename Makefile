.PHONY: local-mwaa-image

local-mwaa-image::
	rm -rf var
	mkdir -p var
	cd var; \
	git clone https://github.com/aws/aws-mwaa-local-runner.git; \
	cd aws-mwaa-local-runner; \
	./mwaa-local-env build-image;

dag-config::
	python -m 

compose-up::
	docker compose up -d --build

compose-down::
	docker compose down  --rmi 'all'