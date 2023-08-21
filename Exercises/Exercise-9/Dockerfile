FROM python:3.10

WORKDIR /app

COPY Pipfile.lock .
COPY Pipfile .

RUN python3 -m pip install --no-cache-dir pipenv==2023.5.19 && \
    PIPENV_VENV_IN_PROJECT=1 pipenv install --dev --deploy --ignore-pipfile

COPY ./Exercises/Exercise-9/main.py .
COPY ./Exercises/Exercise-9/data ./data
