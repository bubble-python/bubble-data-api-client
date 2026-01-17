export-all-requirements:
	uv export --output-file requirements.txt --quiet

# auto format and fix
lint:
	ruff format
	ruff check --fix

# check the ruff rules
lint-ci:
	ruff format --check
	ruff check

test:
	pytest

test-unit:
	pytest src/tests/unit

test-integration:
	pytest src/tests/integration

coverage:
	coverage run -m pytest && coverage report

check-updates:
	uv pip list --outdated

update:
	uv lock --upgrade
	uv sync
	$(MAKE) export-all-requirements

stats:
	@echo "=== Total Downloads ==="
	@pypistats overall bubble-data-api-client
	@echo "\n=== Recent (excludes mirrors) ==="
	@pypistats recent bubble-data-api-client
	@echo "\n=== By Python Version ==="
	@pypistats python_minor bubble-data-api-client
	@echo "\n=== By OS ==="
	@pypistats system bubble-data-api-client
	@echo "\n=== Monthly Downloads ==="
	@pypistats overall bubble-data-api-client --monthly --mirrors without
	@echo "\n=== Monthly by Python Version ==="
	@pypistats python_minor bubble-data-api-client --monthly
	@echo "\n=== Monthly by OS ==="
	@pypistats system bubble-data-api-client --monthly
