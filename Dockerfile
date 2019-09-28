FROM python:2-slim
ENV PYTHONUNBUFFERED 1
ENV ADDRESS 127.0.0.1:8001
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
CMD ["python", "/app/pyraft/kstored.py"]
