install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	python -m pytest -vv --cov=main 

format:	
	black *.py 

lint:
	pylint --disable=R,C --ignore-patterns=test_.*?py *.py

run:
	python main.py
		
all: install lint format test

activate:
	source /home/vscode/venv/bin/activate
