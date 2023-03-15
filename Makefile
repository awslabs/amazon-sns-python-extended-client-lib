SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

install:  # Install the app locally
	poetry install
.PHONY: install

ci: lint test ## Run all checks (test, lint)
.PHONY: ci

test:  ## Run tests
	poetry run pytest --cov=sns_extended_client --cov-report term-missing test
.PHONY: test

lint:  ## Run linting
	poetry run black --check src test
	poetry run isort -c src test
	poetry run flake8 src test
.PHONY: lint

lint-fix:  ## Run autoformatters
	poetry run black src test
	poetry run isort src test
.PHONY: lint-fix

.DEFAULT_GOAL := help
help: Makefile
	@grep -E '(^[a-zA-Z_-]+:.*?##.*$$)|(^##)' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[32m%-30s\033[0m %s\n", $$1, $$2}' | sed -e 's/\[32m##/[33m/'
