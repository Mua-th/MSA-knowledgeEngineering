FROM python:3.9-slim

WORKDIR /app
# Copy files from producer directory
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./mock_producer.py .

ENTRYPOINT ["python"]
CMD ["mock_producer.py"]
