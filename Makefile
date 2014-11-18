.PHONY: tests run

deps:
	@pip install -r requirements.txt

tests_deps:
	@pip install -r test_requirements.txt

tests: tests_deps
	@python -m unittest discover
	$(MAKE) clean

clean:
	@find . -name "*.pyc" -delete

run:
	@python challange
