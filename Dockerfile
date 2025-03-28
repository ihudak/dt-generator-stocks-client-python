FROM python:alpine
LABEL authors="ivan.gudak"
RUN mkdir -p /opt/app
WORKDIR /opt/app
COPY ./ .
RUN pip install -r ./requirements.txt
ENTRYPOINT ["python", "./stock_client.py"]
EXPOSE 8080
