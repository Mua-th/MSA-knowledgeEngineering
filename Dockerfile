FROM python:3.9-slim AS base

# Consumer Service
FROM base AS consumer
WORKDIR /app

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download and verify NLTK data
RUN python -c "import nltk; \
    nltk.download('punkt', download_dir='/usr/local/share/nltk_data'); \
    nltk.download('averaged_perceptron_tagger', download_dir='/usr/local/share/nltk_data'); \
    nltk.download('wordnet', download_dir='/usr/local/share/nltk_data'); \
    nltk.download('brown', download_dir='/usr/local/share/nltk_data'); \
    nltk.download('movie_reviews', download_dir='/usr/local/share/nltk_data')"

# Download TextBlob corpora
RUN python -m textblob.download_corpora

# Install and verify spaCy model
RUN python -m spacy download en_core_web_sm && \
    python -c "import spacy; nlp = spacy.load('en_core_web_sm'); doc = nlp('test')"

# Verify all NLP dependencies work together
RUN python -c "import nltk; from textblob import TextBlob; import spacy; \
    nlp = spacy.load('en_core_web_sm'); \
    TextBlob('test').sentiment; \
    nltk.data.find('tokenizers/punkt'); \
    print('All NLP dependencies verified successfully')"

# Copy application code
COPY . .

CMD ["python", "main.py"]