# TrendSense

Stock Market Sentiment Analyzer for Predicting Market Trends

# Capstone Project Proposal: High-Yield Stock Predictor – Analyzing Market Sentiment for Stock Value Prediction

## Problem Statement

This project aims to explore the impact of market sentiment on stock prices. By analyzing both qualitative (market sentiment) and quantitative (10-K filings) data, the project seeks to predict stock value changes based on a variety of indicators, focusing on how sentiment influences stock performance.

## Data Sources

### Market Sentiment Data Sources

1. **Reuters**: Known for reliable financial and business news with factual reporting, essential for unbiased sentiment analysis.
2. **Bloomberg**: Offers timely news on individual stocks and market trends, though some content requires a subscription.
3. **Yahoo Finance**: Provides stock-specific news, earnings reports, and sentiment indicators by ticker symbol, which can be scraped for stock-specific insights.
4. **MarketWatch**: Features extensive stock, investing, and market sentiment coverage, useful for detailed sentiment insights.
5. **Seeking Alpha**: A platform with analyst opinions and investment strategies, allowing sentiment-rich commentary (note that some features may require a subscription).
6. **Financial Times (FT)**: Offers global financial reporting, ideal for a comprehensive perspective on market sentiment.
7. **CNBC**: Provides real-time financial market news, expert insights, and daily summaries, which can enhance understanding of short-term sentiment shifts.

### 10-K Filing Data Sources

1. **EDGAR (SEC Database)**: The SEC’s free EDGAR database provides access to public companies' filings, including 10-K and 10-Q reports, and is a comprehensive resource for structured financial data.
   - Website: [https://www.sec.gov/edgar.shtml](https://www.sec.gov/edgar.shtml)
2. **Seeking Alpha**: In addition to news, it offers 10-K filings and analyst commentary, adding qualitative context.
3. **Yahoo Finance**: Includes SEC filing links under company profiles, generally under “Financials” or “Analysis” tabs.
4. **Morningstar**: Provides access to 10-Ks along with analytical tools and ratings (some features may require a subscription).
5. **BamSEC**: Specializes in SEC filings, with enhanced navigation and key section highlights; basic access is free.
6. **MarketWatch**: Also provides 10-K access, alongside news and analytical insights within company profiles.

## Approach

### Stock Price Data Collection

To align sentiment analysis and 10-K data with stock price changes, stock data will be collected from reliable sources:

1. **Yahoo Finance API (yfinance)**: A Python library to access historical stock prices.
2. **Alpha Vantage API**: Provides daily and intraday stock data (free tier available).
3. **Polygon.io API**: Offers real-time and historical stock data, suitable for high-frequency updates.
4. **Yahoo Finance (Web Scraping)**: If API restrictions limit data access, web scraping can be performed using BeautifulSoup and requests libraries (with caution to avoid frequent access issues).

## Project Outline

1. **Data Collection**

   - **Market Sentiment**: Web scraping tools like BeautifulSoup or Scrapy will be used to gather sentiment data from chosen news sites. Selenium may be used if interaction with dynamic content is needed.
   - **10-K Filings**: The SEC’s EDGAR API will streamline access to financial reports, with content filtering for sections rich in sentiment (e.g., “Risk Factors,” “Management Discussion”).

2. **Preprocessing**

   - **Text Processing**: Libraries like NLTK and spaCy will be used for tokenization, stopword removal, and standard text preprocessing steps.
   - **Section Extraction**: Important sections of 10-K filings will be extracted using regular expressions (re) for focus on sentiment-rich content.
   - **Numerical Data Parsing**: Specific financial data, such as revenue and debt, will be extracted for structured feature generation.

3. **Sentiment Analysis**

   - **Basic Sentiment Models**: Use VADER and TextBlob for initial sentiment scoring.
   - **Advanced NLP Models**: Hugging Face transformers library with FinBERT or BERT models will provide nuanced sentiment and topic analysis on financial text.

4. **Feature Engineering**

   - **TF-IDF and Word Embeddings**: Utilize scikit-learn’s TF-IDF and Word2Vec from Gensim for turning text into features.
   - **BERT Embeddings**: Capture sentence-level semantics with BERT embeddings from transformers.
   - **Structured Data Extraction**: Financial details from 10-Ks (like profit and revenue) will be integrated into feature sets.

5. **Model Selection**

   - **Traditional ML Models**: Random Forest and Gradient Boosting models in scikit-learn or XGBoost for initial predictions.
   - **Neural Networks**: For handling large datasets, LSTM and Transformer models using TensorFlow or PyTorch to capture long-term dependencies in financial narratives.

6. **Model Training and Evaluation**

   - **Train-Test Split**: Split data into training and test sets, and use scikit-learn or TensorFlow for model training.
   - **Performance Metrics**: Evaluate model performance with Mean Squared Error (MSE) for regression accuracy or classification accuracy for sentiment outcomes.

7. **Deployment**
   - **API Development**: Use FastAPI to create a REST API for real-time analysis.
   - **Interactive Dashboard**: Streamlit will provide a visual dashboard for predictions and insights, allowing users to interact with sentiment and stock data in real-time.

## Conclusion

This capstone project aims to deliver a predictive model that leverages both market sentiment and fundamental data to forecast stock performance. By combining advanced NLP techniques with traditional and deep learning models, this project will provide insights into the correlation between sentiment and stock price trends, offering valuable insights for investors and financial analysts.
