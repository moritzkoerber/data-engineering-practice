FROM python:3.10

WORKDIR /app

COPY ./Exercises/Exercise-2/main.py .
COPY Exercises/Exercise-2/tests ./tests
COPY Pipfile .
COPY Pipfile.lock .

RUN python3 -m pip install --no-cache-dir pipenv==2022.10.25 && \
    PIPENV_VENV_IN_PROJECT=1 pipenv install --dev --deploy --ignore-pipfile
