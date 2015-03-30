.PHONY: docs

init:
	pip install -e .

dev-init: init
	pip install tox ipdb

test:
	py.test

tox:
	tox

coverage:
	py.test --verbose --cov-report term --cov=requests test_requests.py

ci: init
	py.test --junitxml=junit.xml

publish:
	python setup.py register
	python setup.py sdist upload
	python setup.py bdist_wheel upload

docs-init:
	pip install -r docs/requirements.txt

docs:
	cd docs && make html
	@echo "\033[95m\n\nBuild successful! View the docs homepage at docs/_build/html/index.html.\n\033[0m"
