FROM python:2-slim
ENV PYTHONUNBUFFERED 1
ARG PROXY
ENV http_proxy=${PROXY:+http://10.144.1.10:8080}
ENV https_proxy=${PROXY:+http://10.144.1.10:8080}
ENV no_proxy=${PROXY:+localhost,127.0.0.1,peer0,peer1,peer2}
ENV ADDRESS 127.0.0.1:8001
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD ["python", "/app/pyraft/kstored.py"]
