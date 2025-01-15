FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir nltk && \
    pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /usr/local/share/nltk_data && \
    chmod -R 777 /usr/local/share/nltk_data

RUN python -c "import nltk; nltk.download('punkt')"

RUN python -m spacy download en_core_web_sm

COPY . .

ENV PYTHONPATH=/app
ENV NLTK_DATA=/usr/local/share/nltk_data

RUN python -c "import nltk; print('NLTK version:', nltk.__version__)"

COPY verify_installations.py .
RUN python verify_installations.py

CMD ["python", "main.py"]