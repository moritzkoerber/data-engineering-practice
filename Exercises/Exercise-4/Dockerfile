FROM python:3.10

WORKDIR /app

COPY ./Exercises/Exercise-4/main.py .
COPY ./Exercises/Exercise-4/data ./data
COPY Pipfile .
COPY Pipfile.lock .

RUN python3 -m pip install --no-cache-dir pipenv==2023.5.19 && \
    PIPENV_VENV_IN_PROJECT=1 pipenv install --deploy --ignore-pipfile
