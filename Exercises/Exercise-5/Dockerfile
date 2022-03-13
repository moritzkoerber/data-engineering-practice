FROM python:3.10

WORKDIR /app

COPY ./Exercises/Exercise-5/main.py .
COPY ./Exercises/Exercise-5/data ./data

COPY Pipfile .
COPY Pipfile.lock .

RUN python3 -m pip install --no-cache-dir pipenv==2022.3.28 && \
    PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy --ignore-pipfile
