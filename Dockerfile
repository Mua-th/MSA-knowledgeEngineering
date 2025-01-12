FROM python:3.9-slim AS base

# Consumer Service
FROM base AS consumer
WORKDIR /app

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download NLTK data and TextBlob corpora
RUN python -c "import nltk; \
    nltk.download('punkt'); \
    nltk.download('averaged_perceptron_tagger'); \
    nltk.download('wordnet'); \
    nltk.download('brown'); \
    nltk.download('movie_reviews')" && \
    python -m textblob.download_corpora light && \
    python -m spacy download en_core_web_sm

# Copy application code
COPY . .
CMD ["python", "main.py"]