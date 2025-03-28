FROM python:alpine
LABEL authors="ivan.gudak"
RUN mkdir -p /opt/app
WORKDIR /opt/app
COPY ./ .
RUN source ./venv/bin/activate &&  pip install -r ./requirements.txt
ENTRYPOINT source ./venv/bin/activate && python ./stock_client.py
EXPOSE 8080
