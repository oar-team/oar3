.PHONY: docs

init:
	pip install -e .

dev-init: init
	pip install tox ipdb pytest-cov flake8

test:
	py.test --verbose --cov-report term --cov-report html --cov=oar_lib

tox:
	tox

ci: init
	py.test --junitxml=junit.xml

docs-init:
	pip install -r docs/requirements.txt

docs:
	cd docs && make html
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
