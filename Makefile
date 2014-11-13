.PHONY: tests

tests:
	@python -m unittest discover
	$(MAKE) clean

clean:
	@find . -name "*.pyc" -delete

run:
	@python challange
