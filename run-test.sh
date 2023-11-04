clear && poetry run pytest tests --cov-config=.coveragerc --cov=src/my_glue --cov-fail-under=100 --cov-report=xml:coverage/coverage.xml 
