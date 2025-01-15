import sys
import nltk
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import spacy

def verify_installations():
    success = True
    
    # Verify NLTK
    try:
        print(f"NLTK version: {nltk.__version__}")
        nltk.data.find('tokenizers/punkt')
        print('NLTK data found')
    except Exception as e:
        print(f'Error with NLTK: {e}')
        success = False

    # Verify VADER
    try:
        analyzer = SentimentIntensityAnalyzer()
        print('VADER Sentiment Analyzer initialized')
    except Exception as e:
        print(f'Error initializing VADER: {e}')
        success = False

    # Verify spaCy
    try:
        nlp = spacy.load('en_core_web_sm')
        print('spaCy model loaded')
    except Exception as e:
        print(f'Error loading spaCy: {e}')
        success = False

    if success:
        print('All NLP dependencies verified successfully')
        return 0
    else:
        print('Some verifications failed')
        return 1

if __name__ == '__main__':
    sys.exit(verify_installations())