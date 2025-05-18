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

coverage:
	coverage run -m pytest && coverage report

check-updates:
	uv pip list --outdated

update:
	uv lock --upgrade
	uv sync
	$(MAKE) export-all-requirements
