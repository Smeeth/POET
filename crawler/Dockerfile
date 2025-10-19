FROM python:3.11-slim
WORKDIR /app
RUN pip install requests mariadb
COPY collector.py .
CMD ["python", "collector.py"]
