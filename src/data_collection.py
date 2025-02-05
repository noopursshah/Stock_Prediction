import requests
from bs4 import BeautifulSoup
import yfinance as yf
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime

# Set up SQLAlchemy for database interaction
Base = declarative_base()

# Define News and StockPrice models
class News(Base):
    __tablename__ = 'news'
    id = Column(Integer, primary_key=True, autoincrement=True)
    headline = Column(String, nullable=False)
    source = Column(String, nullable=False)
    date = Column(DateTime, default=datetime.datetime.utcnow)

class StockPrice(Base):
    __tablename__ = 'stock_prices'
    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, nullable=False)
    date = Column(DateTime, default=datetime.datetime.utcnow)
    price = Column(Float, nullable=False)

# Create a database connection (SQLite example)
engine = create_engine('sqlite:///data_collection.db', echo=True)
Base.metadata.create_all(engine)

# Set up session for DB operations
Session = sessionmaker(bind=engine)
session = Session()

# 1. Scrape News Headlines using BeautifulSoup and Requests
def scrape_news():
    url = 'https://news.ycombinator.com/'  # You can use any news website here
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    headlines = []
    for item in soup.find_all('a', class_='storylink'):
        headline = item.get_text()
        source = item.get('href')  # Getting the link as the source
        headlines.append((headline, source))
    
    # Insert headlines into the SQL database
    for headline, source in headlines:
        news_item = News(headline=headline, source=source)
        session.add(news_item)
    session.commit()
    print(f"Inserted {len(headlines)} news headlines into the database.")

# 2. Fetch Stock Prices using yfinance
def fetch_stock_prices(ticker='AAPL'):
    stock = yf.Ticker(ticker)
    data = stock.history(period='1d', interval='1d')  # Fetch daily stock price

    prices = []
    for date, row in data.iterrows():
        price = row['Close']
        prices.append((ticker, date, price))
    
    # Insert stock prices into the SQL database
    for ticker, date, price in prices:
        stock_price = StockPrice(ticker=ticker, date=date, price=price)
        session.add(stock_price)
    session.commit()
    print(f"Inserted stock prices for {ticker} into the database.")

# Main function to run the collection process
def main():
    scrape_news()
    fetch_stock_prices('AAPL')  # You can change the ticker symbol to any other stock

if __name__ == '__main__':
    main()
