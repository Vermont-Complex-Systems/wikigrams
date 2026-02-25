.PHONY: all import partition-week partition-month prepare submit

import:
	bash extract/import.sh

partition-week:
	bash partitions/weekly.sh

partition-month:
	bash partitions/monthly.sh

prepare:
	uv run python adapter/prepare.py

submit:
	uv run python adapter/submit.py
