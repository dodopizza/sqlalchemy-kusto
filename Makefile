.PHONY: venv install install-dev build clean check test unit integration release pypi
-include .env

##############################################################################
# Environment variables
##############################################################################
VENV_DIR = .venv
PYTHON=${VENV_DIR}/bin/python

##############################################################################
# Development set up
##############################################################################
install: venv install-dev

venv: # Create new venv if not exists
	@echo "Creating new virtual environment $(GREEN_ITALIC)$(VENV_DIR)$(DEFAULT) if not exists..."
	@test -d $(VENV_DIR) || python -m venv $(VENV_DIR)
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) --version
	@echo "Done! You may use $(GREEN_ITALIC)source $(VENV_DIR)/bin/activate$(DEFAULT) to activate it and install packages manually, or use Makefile targets for all project setup routines.\n"

install-dev: # Install dev dependencies
	@echo "Installing dev dependencies..."
	$(PYTHON) -m pip install -e ".[dev]"
	@echo "Done.\n"

##############################################################################
# Development process
##############################################################################
format: 
	@echo "Running formatters..."

	@echo "\n1. Run $(GREEN_ITALIC)ruff$(DEFAULT) to format code."
	$(PYTHON) -m ruff check --fix-only . 

	@echo "\n2. Run $(GREEN_ITALIC)black$(DEFAULT) to format code."
	$(PYTHON) -m black .


check: # Run formatters and linters
	@echo "Running checkers..."

	@echo "\n1. Run $(GREEN_ITALIC)ruff$(DEFAULT) to check code."
	$(PYTHON) -m ruff check .

	@echo "\n2. Run $(GREEN_ITALIC)black$(DEFAULT) to check code formatting."
	$(PYTHON) -m black . --check

	@echo "\n4. Run $(GREEN_ITALIC)mypy$(DEFAULT) for type checking."
	$(PYTHON) -m mypy .

	@echo "Done.\n"

test: unit integration

unit: # Run unit tests
	@echo "Running unit tests..."
	$(PYTHON) -m pytest -v tests/unit/
	@echo "Done.\n"

integration: # Run integration tests
	@echo "Running integration tests..."
	$(PYTHON) -m pytest -v tests/integration/
	@echo "Done.\n"

##############################################################################
# Build and cleanup
##############################################################################
build: # Build sqlalchemy-kusto package
	@echo "Building the project..."
	-rm -rf build/*
	-rm -rf dist/*
	$(PYTHON) -m pip install --upgrade build
	$(PYTHON) -m build
	@echo "Done. You may find the project artifact in the $(GREEN_ITALIC)dist$(DEFAULT) folder.\n"

clean: # Clean all working folders
	@echo "Removing working folders..."
	-rm -rf $(VENV_DIR)
	-rm -rf dist
	-rm -rf sqlalchemy_kusto.egg-info
	@echo "Done.\n"


##############################################################################
# Release new version
##############################################################################
release: build pypi

pypi: # Upload package to PyPi repository
	@echo "Uploading to PyPi..."
	$(PYTHON) -m pip install --upgrade twine
	$(PYTHON) -m twine upload --username __token__ --password $(PYPI_API_TOKEN) dist/*

##############################################################################
# Output highlights
##############################################################################
DEFAULT = \033[0m
GREEN_ITALIC = \033[32;3;1m
