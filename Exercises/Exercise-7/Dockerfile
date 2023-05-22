FROM ubuntu:focal-20220316

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends software-properties-common gpg-agent \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update && \
    apt-get install --no-install-recommends -y default-jdk scala wget vim python3.10 python3-pip curl unzip libpq-dev build-essential libssl-dev libffi-dev python3.10-dev python3.10-distutils && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ARG spark_version=3.2.1
ARG hadoop_version=3.2

RUN wget --progress=dot:giga https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz && \
    tar xvf spark-${spark_version}-bin-hadoop${hadoop_version}.tgz && \
    mv spark-${spark_version}-bin-hadoop3.2/ /usr/local/spark && \
    ln -s /usr/local/spark spark && \
    wget --progress=dot:giga https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.890/aws-java-sdk-bundle-1.11.890.jar && \
    mv aws-java-sdk-bundle-1.11.890.jar /spark/jars && \
    wget --progress=dot:giga https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar && \
    mv hadoop-aws-3.2.0.jar /spark/jars

WORKDIR /app

COPY ./Exercises/Exercise-7/main.py .
COPY ./Exercises/Exercise-7/data ./data
COPY ./Exercises/Exercise-7/tests ./tests
COPY ./Exercises/utils ./utils

COPY Pipfile .
COPY Pipfile.lock .

RUN ["/bin/bash", "-c", "set -o pipefail && wget -O- -q https://bootstrap.pypa.io/get-pip.py | python3.10"]
RUN python3.10 -m pip install --no-cache-dir pipenv==2023.5.19 && \
    PIPENV_VENV_IN_PROJECT=1 pipenv install --dev --deploy --ignore-pipfile

ENV PYSPARK_PYTHON=python3.10
ENV PYSPARK_SUBMIT_ARGS='--packages io.delta:delta-core_2.12:0.8.0 pyspark-shell'
