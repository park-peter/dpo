PYTHON ?= python3

.PHONY: install install-dev

install:
	$(PYTHON) -m pip install -e .

install-dev:
	$(PYTHON) -m pip install -e ".[dev]"
