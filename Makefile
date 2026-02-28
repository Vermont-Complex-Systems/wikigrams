SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

.PHONY: extract partition-week partition-month prepare submit

# --- Extract: processes only missing days (~12s/day) ---
# First run = full seed; subsequent runs = incremental update
extract:
	bash extract/extract.sh

partition-week:
	bash partitions/weekly.sh

partition-month:
	bash partitions/monthly.sh

prepare:
	uv run python adapter/prepare.py

submit:
	uv run python adapter/submit.py
