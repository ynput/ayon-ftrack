FROM python:3.11-alpine
ENV PYTHONUNBUFFERED=1

RUN mkdir /service
COPY ./leecher /service/leecher
COPY ./pyproject.toml /service/pyproject.toml
WORKDIR /service

RUN pip install .

CMD ["python", "-m", "leecher"]
