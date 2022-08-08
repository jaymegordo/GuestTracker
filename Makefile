#!/bin/bash

SHELL := /bin/bash
utils := @poetry run python -m scripts.utils
code := guesttracker scripts


.PHONY : codecount
codecount:  ## show lines of code
	@pygount --suffix=py --format=summary guesttracker

.PHONY : format
format:  ## autopep, isort, flake
	@poetry run autopep8 --recursive --in-place $(code)
	@poetry run isort $(code)
	@poetry run flake8 $(code)

.PHONY : flake
flake:  ## run flake with only selected dirs
	@poetry run flake8 $(code)

.PHONY : dbconfig
dbconfig:  ## make dbmodel.py table definitions from database
	$(utils) --write_dbconfig

.PHONY : docs
docs:  ## create jupyterbook documentation
	@poetry run jupyter-book clean docs && poetry run jupyter-book build docs
	@open docs/_build/html/index.html

.PHONY : push_docs
push_docs:  ## push docs to live github pages branch
	@poetry run jupyter-book clean docs && poetry run jupyter-book build docs
	@poetry run ghp-import -n -p -f docs/_build/html

.PHONY : creds
creds:  ## re-encrypt credentials
	$(utils) --encrypt_creds

.PHONY : smr
smr:  ## make smr report
	$(utils) --smr

.PHONY : framecracks
framecracks:  ## make framecracks excel file
	$(utils) --framecracks

.PHONY : reqs
reqs:  # write requirements.txt from pyproject.toml for azure app
	@poetry export -f requirements.txt --output requirements.txt --without-hashes

.PHONY : app
app:  ## push guesttracker app to azure
	@if ! docker info >/dev/null 2>&1; then\
		echo "Starting Docker";\
		open /Applications/Docker.app;\
	fi
	@func azure functionapp publish guesttracker-app --build-native-deps

.PHONY : build
build:  ## make guesttracker.exe and push to aws s3 bucket (pyupdater)
	@bash scripts/build.sh true

.PHONY : build-local
build-local:  ## make guesttracker.exe for local testing (pyinstaller)
	@poetry run python -m scripts.build

.PHONY : require_poetry
require_poetry:  ## check poetry installed, download if required
	@if ! [ -x "$$(command -v poetry)" ]; then\
		echo -e '\n\033[0;31m ❌ poetry not installed. Installing Poetry.\n\033[0m';\
		pip install poetry;\
	else\
		echo -e "\033[0;32m ✔️ poetry installed\033[0m";\
	fi

.PHONY : init
init: require_poetry  ## setup environment
	@echo "Checking poetry dependencies"
	@poetry install
	# @poetry run pre-commit install --hook-type pre-commit --hook-type pre-push

	# install gtk3 runtime
	# https://github.com/tschoonj/GTK-for-Windows-Runtime-Environment-Installer/releases

	# TODO make wheel or something?
	# pyupdater needs bsdiff4 which doesnt have wheel on pypi... needs c++ build toools

	# pyodbc
	# brew install unixodbc
	# export LDFLAGS="-L/opt/homebrew/Cellar/unixodbc/2.3.9_1/lib"
	# export CPPFLAGS="-I/opt/homebrew/Cellar/unixodbc/2.3.9_1/include"

help: ## show this help message
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)