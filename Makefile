.PHONY: test package deploy fmt

test:
	python -m pytest -q

package:
	mkdir -p build
	cd src && zip -qr ../build/pipeline.zip pipeline

deploy: package
	cd infra/terraform && terraform init && terraform apply

fmt:
	terraform -chdir=infra/terraform fmt
