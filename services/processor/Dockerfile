FROM python:3.9-alpine
ENV PYTHONUNBUFFERED=1

RUN mkdir /service
COPY ./processor /service/processor
COPY ./ftrack_common /service/ftrack_common
COPY ./pyproject.toml /service/pyproject.toml
WORKDIR /service

RUN pip install .

CMD ["python", "-m", "processor"]
