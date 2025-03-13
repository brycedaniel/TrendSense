# **TrendSense**
## Stock Market Predictor Using AI, Financial Metrics, and Market News  

---

## **Capstone Project: High-Yield Stock Predictor**  

### **Overview**  
TrendSense is a **stock prediction system** that integrates **financial metrics, analyst recommendations, and market sentiment** to forecast stock movements and long-term trends. Using **machine learning, regression analysis, and AI-driven sentiment analysis**, the system provides insights into stock price fluctuations.  

This project leverages **multiple data sources, automated data collection, predictive modeling, and interactive dashboards** to help investors make data-driven decisions.  

---

## **Problem Statement**  
Stock prices are influenced by a vast number of factors, including **financial reports, market sentiment, macroeconomic indicators, and news events**. Investors often struggle to synthesize all of this information into a coherent investment strategy.  

TrendSense solves this problem by:  
- **Predicting next-day price movements** using multiple regression models.  
- **Aggregating key financial and sentiment indicators** into a **proprietary TS Score** to assess long-term trends.  
- **Providing interactive dashboards** for data visualization and stock analysis.  

By offering a structured approach to **market analysis and stock forecasting**, TrendSense empowers investors to make more **informed and confident decisions**.  

---

## **Data Sources**  

### **Stock Market & Financial Data**  
- **Yahoo Finance API** – Stock prices, financial statements, and analyst ratings.  
- **Alpha Vantage API** – Real-time and historical stock data.  
- **Seeking Alpha** – Analyst opinions and investment sentiment.  
- **RapidAPI** – Supplementary stock market data sources.  

### **Market Sentiment Data**  
- **News API** – Aggregated financial news from multiple publishers.  
- **Reuters** – Reliable financial and business reporting.  
- **Bloomberg** – Stock-specific news and earnings insights.  
- **CNBC** – Market trend analysis and expert commentary.  
- **MarketWatch** – Financial data and market sentiment coverage.  

### **Data Collection & Processing**  
- **Data Extraction**: API calls & web scraping (BeautifulSoup, requests).  
- **Data Preprocessing**: Cleaning, structuring, and sentiment analysis using NLP techniques.  
- **Storage**: Google BigQuery (cloud database for structured data).  
- **Automation**: Google Cloud Functions & Google Scheduler for periodic updates.  

---

## **Feature Engineering & Model Development**  

### **Data Integration & Processing**  
- **SQL joins** merge financial and sentiment datasets.  
- **Feature normalization** ensures consistent scaling across all variables.  
- **Natural Language Processing (NLP)** for sentiment scoring using **VADER, TextBlob, and AI-driven models**.  

### **Predictive Modeling**  
- **Multiple Regression Model**: Predicts next-day price movements using financial metrics and sentiment scores.  
- **TS Score (TrendSense Score)**: Aggregates **AI sentiment, analyst ratings, and financial health metrics** into a single ranking system for long-term stock recommendations.  

### **Core Features of TrendSense Dataset**  

| Column Name | Description |
|-------------|------------|
| **Ticker** | The ticker symbol of a tracked stock (e.g., AAPL, TSLA). |
| **Date** | Date of the data collection. |
| **Stock_Category** | Industry classification (e.g., AI, Semiconductor, Energy). |
| **TS_Score** | Aggregate score combining AI Sentiment, Analyst Ratings, and Financial Health. |
| **Close Price** | The stock’s closing price for the day. |
| **Price_Movement_Today** | Percentage change in the stock price from the previous day. |
| **Predicted_Price_Movement** | Next-day stock price movement forecasted by regression models. |
| **AI_Score** | AI-driven sentiment score (-10 to 10). |
| **Sentiment_Score** | Weighted sentiment score from financial news analysis. |
| **Health_Score** | Score based on financial fundamentals and analyst opinions. |
| **TS_Score_4Week** | Rolling 4-week average of TS Score for long-term analysis. |
| **TS_Rank_4Week** | Ranking based on TS Score, where lower ranks indicate better stocks. |
| **Composite_Rank** | A weighted ranking combining short-term and long-term TS trends. |
| **Top 10 Predictions** | Identifies the 10 best-performing stocks based on TS Score and predicted movement. |

---

## **System Architecture & Deployment**  

### **Data Pipeline & Automation**  
1. **Data Extraction**:  
   - APIs retrieve stock, financial, and news sentiment data.  
   - Web scraping (BeautifulSoup) supplements missing data points.  
2. **Data Storage & Processing**:  
   - Raw data is **stored in Google BigQuery**.  
   - Automated **SQL joins** and Python scripts clean and transform data.  
3. **Predictive Model Execution**:  
   - Google Cloud Functions run **regression models and sentiment analysis**.  
4. **Dashboard Integration**:  
   - **Power BI Dashboards** display **real-time stock analysis & predictions**.  
5. **Scheduled Updates**:  
   - **Google Scheduler automates data refreshes**, ensuring up-to-date insights.  

---

## **Analysis & Insights**  

### **Visualization Dashboards**  
The **TrendSense Dashboard** provides key insights on stock trends, predictions, and rankings.  

- **Stock Performance Tracker**: Displays historical and predicted stock movements.  
- **TS Score Heatmap**: Highlights top-performing stocks by long-term ranking.  
- **Sector-Wide Comparisons**: Analyzes stock categories (AI, Semiconductors, Energy, etc.).  
- **Sentiment & Price Movement Correlation**: Measures the influence of news sentiment on stock trends.  

### **Stock Ranking System**  
- **Short-Term Predictions**: Uses regression models to forecast **next-day price movement**.  
- **Long-Term Investment Picks**: The **TS Score aggregates sentiment & financial indicators** to rank stocks over time.  

---

## **Limitations & Challenges**  

### **Cost Constraints**  
- **API Costs**: Some **real-time data sources require expensive API subscriptions**.  
- **Cloud Processing & Storage Costs**: Running **Google BigQuery queries at scale** adds costs.  
- **AI Model Processing**: NLP-based sentiment analysis requires **high computational resources**.  

### **Predictive Accuracy Constraints**  
- **Market Complexity**: **Stock prices are influenced by factors outside the model’s scope**, such as geopolitical events.  
- **Model Limitations**: TrendSense currently captures **~15% of stock price movements**, leaving room for improvement.  

---

## **Future Enhancements**  
- **Deep Learning Models**: Experiment with **LSTM & Transformers** for better trend forecasting.  
- **Expanded Data Sources**: Integrate **real-time trading volume & economic indicators**.  
- **Improved Sentiment Analysis**: Use **topic modeling** to identify key themes in financial news.  
- **Mobile & Web Application**: Develop an intuitive UI for traders to access **TrendSense insights on the go**.  

---

## **Conclusion**  
TrendSense is an **AI-powered stock prediction tool** that combines **financial fundamentals, market sentiment, and regression modeling** to generate **data-driven investment insights**.  

While **not a definitive predictor**, it serves as an **invaluable resource for understanding market trends** and identifying high-performing stocks based on **quantitative and qualitative factors**.  

By continuing to refine its models and expand its data sources, TrendSense aims to become a **leading tool for stock market forecasting** and **investment decision-making**.  
