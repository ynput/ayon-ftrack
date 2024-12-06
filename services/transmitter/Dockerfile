FROM python:3.9-bullseye
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
  && apt-get install -y curl python3-pip python3-virtualenv \
  && pip install poetry

RUN mkdir /service
COPY ./transmitter /service/transmitter
COPY ./pyproject.toml /service/pyproject.toml
WORKDIR /service


RUN poetry config virtualenvs.create false \
 && poetry install --no-interaction --no-ansi

CMD ["python", "-m", "transmitter"]
