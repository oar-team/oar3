.PHONY: docs dist

help:
	@echo "Please use 'make <target>' where <target> is one of"
	@echo "  init       to install the project in development mode (using virtualenv is highly recommended)"
	@echo "  test       to run all tests with pytest"
	@echo "  tox-test   to run all tests in multiples versions of python with tox"
	@echo "  ci-test    to run all tests and get junitxml report for CI (Travis, Jenkins...)"
	@echo "  docs       to build the documentation in the HTML format"
	@echo "  dist       to build python sdist and wheel packages"
	@echo "  publish    to upload packages to Pypi website"
	@echo "  flake8     to run flake8 code checker"

init:
	pip install tox ipdb pytest pytest-cov flake8 sphinx==1.1.3

test:
	py.test --verbose --cov-report term --cov-report html --cov=oar_lib

tox-test:
	tox

ci-test:
	py.test --junitxml=junit.xml

docs:
	$(MAKE) -C docs html
	@echo "\033[95m\n\nBuild successful! View the docs homepage at docs/_build/html/index.html.\n\033[0m"

dist:
	python setup.py sdist
	python setup.py bdist_wheel

publish:
	python setup.py register
	python setup.py sdist upload
	python setup.py bdist_wheel upload

flake8:
	flake8 .
