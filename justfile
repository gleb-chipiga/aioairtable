isort:
    isort --line-length 79 --skip .mypy_cache --skip .hypothesis --skip .tox .

sort-all:
    -sort-all aioairtable/*.py
    -sort-all tests/*.py

black:
    black --line-length 79 --extend-exclude="\.tox/" .

flake8:
    flake8 --exclude .tox .

mypy:
    mypy --strict .

coverage:
    COVERAGE_FILE=.coverage/.coverage python -m pytest --cov=aioairtable \
      --cov-report term --cov-report html:.coverage tests

all: isort sort-all black flake8 mypy coverage

build:
    if [ -d dist ]; then rm -rf dist; fi
    python -m build
    rm -rf *.egg-info

upload:
    twine upload dist/*
    rm -rf dist
