FROM python:3.10

WORKDIR /app

COPY Pipfile.lock .
COPY Pipfile .

RUN python3 -m pip install --no-cache-dir pipenv==2023.5.19 && \
    PIPENV_VENV_IN_PROJECT=1 pipenv install --dev --deploy --ignore-pipfile

COPY ./Exercises/Exercise-8/main.py .
COPY ./Exercises/Exercise-8/data ./data
COPY ./Exercises/Exercise-8/tests ./tests
COPY ./Exercises/Exercise-8/soda-sql ./soda-sql
