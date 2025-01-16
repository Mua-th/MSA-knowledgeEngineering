import unittest
from market_analyzer import MarketAnalyzer

class TestMarketAnalyzer(unittest.TestCase):
    def setUp(self):
        self.analyzer = MarketAnalyzer()
        self.sample_texts = {
            'positive': "The tech industry is experiencing strong growth with innovative AI solutions.",
            'negative': "Market crashes as tech stocks plummet amid rising concerns.",
            'neutral': "The market closed at 15000 points today.",
            'empty': "",
            'special': "Tech & AI @ 2024!!"
        }

    def test_sentiment_analysis(self):
        result = self.analyzer.analyze_market_context(self.sample_texts['positive'])
        self.assertIsInstance(result, dict)
        self.assertIn('overall_sentiment', result)
        self.assertTrue(-1 <= result['overall_sentiment'] <= 1)

    def test_sector_detection(self):
        result = self.analyzer.analyze_market_context(self.sample_texts['positive'])
        self.assertIn('sectors', result)
        self.assertIsInstance(result['sectors'], dict)
        self.assertIn('tech', result['sectors'])

    def test_empty_text(self):
        result = self.analyzer.analyze_market_context(self.sample_texts['empty'])
        self.assertIsInstance(result, dict)
        self.assertEqual(result['overall_sentiment'], 0)
        self.assertEqual(len(result['sectors']), 0)

    def test_special_characters(self):
        result = self.analyzer.analyze_market_context(self.sample_texts['special'])
        self.assertIsInstance(result, dict)
        self.assertIn('tech', result['sectors'])

if __name__ == '__main__':
    unittest.main()