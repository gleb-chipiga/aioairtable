flake8:
	flake8 --exclude .tox,.env .

mypy:
	mypy --strict .

isort:
	isort aioairtable tests setup.py

coverage:
	COVERAGE_FILE=.coverage/.coverage python -m pytest --cov=aioairtable --cov-report term --cov-report html:.coverage tests

build:
	if [ -d dist ]; then rm -rf dist; fi
	python -m build
	rm -rf build
	rm -rf *.egg-info

upload:
	twine upload dist/*
	rm -rf dist
