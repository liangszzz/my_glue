poetry run pytest tests -v --cov-config=.coveragerc --cov=src/my_glue --cov-fail-under=100 --cov-report=xml:coverage/coverage.xml
