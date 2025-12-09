.PHONY: all import partition-week partition-month prepare submit

import:
	bash extract/import.sh

partition-week:
	bash partitions/weekly.sh

partition-month:
	bash partitions/monthly.sh

prepare:
	uv run python adapter/src/prepare.py

submit:
	uv run python adapter/src/submit.py