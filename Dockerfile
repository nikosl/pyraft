FROM python:2-slim
ENV PYTHONUNBUFFERED 1
ARG PROXY_URL
ENV http_proxy=${PROXY_URL}
ENV https_proxy=${PROXY_URL}
ENV no_proxy=${PROXY:+localhost,127.0.0.1,peer0,peer1,peer2}
ENV ADDRESS 127.0.0.1:8001
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD ["python", "/app/pyraft/kstored.py"]
