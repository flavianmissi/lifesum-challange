tests: clean
	@python -m unittest discover

clean:
	@find . -name "*.pyc" -delete

run:
	@python challange
