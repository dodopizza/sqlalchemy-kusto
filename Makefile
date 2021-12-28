.PHONY: venv install install-dev build

##############################################################################
# Environment variables
##############################################################################
VENV_DIR = venv
PYTHON=${VENV_DIR}/bin/python

##############################################################################
# Development set up
##############################################################################
venv: # Create new venv if not exists
	@echo "Create new virtual environment $(RED_ITALIC)$(VENV_DIR)$(DEFAULT) if not exists..."
	@test -d $(VENV_DIR) || python -m venv $(VENV_DIR)
	@echo "Done! You may use $(RED_ITALIC)source $(VENV_DIR)/bin/activate$(DEFAULT) to activate it and install packages manually, or use Makefile targets for all project setup routines.\n"

install-dev: # Install dev dependencies
	@echo "Install dev dependencies..."
	$(PYTHON) -m pip install -e ".[dev]"
	@echo "Done.\n"

install: venv install-dev

##############################################################################
# Development set up
##############################################################################
build: # Build sqlalchemy-kusto package
	@echo "Build the project..."
	rm -rf build/*
	rm -rf dist/*
	$(PYTHON) setup.py clean bdist_wheel
	@echo "Done. You may find the project artifact in the $(RED_ITALIC)dist$(DEFAULT) folder.\n"

##############################################################################
# Output highlights
##############################################################################
DEFAULT = \033[0m
BLACK = \033[30;1;1m
RED = \033[31;1;1m
RED_ITALIC = \033[31;3;1m
GREEN = \033[32;1;1m
GOLD = \033[33;1;1m
BLUE = \033[34;1;1m
PURPLE = \033[35;1;1m
TEAL = \033[36;1;1m
GREY = \033[37;1;1m
