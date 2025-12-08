.PHONY: all import prepare submit

import:
	uv run python extract/src/import.py

prepare:
	uv run python adapter/src/prepare.py

submit:
	uv run python adapter/src/submit.py