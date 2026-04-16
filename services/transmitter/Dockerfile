FROM python:3.11-alpine
ENV PYTHONUNBUFFERED=1

RUN mkdir /service
COPY ./transmitter /service/transmitter
COPY ./ftrack_common /service/ftrack_common
COPY ./pyproject.toml /service/pyproject.toml
WORKDIR /service

RUN pip install .

CMD ["python", "-m", "transmitter"]
