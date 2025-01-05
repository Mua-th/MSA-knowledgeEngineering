FROM python:3.9-slim

WORKDIR /app

# Copy only the requirements file to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m spacy download en_core_web_sm

# Copy the rest of the application code
COPY . .

# Set the entry point for the container
CMD ["python", "main.py"]