import pandas as pd
import yfinance as yf
from datetime import date
from tradingview_ta import TA_Handler, Interval, Exchange
from datetime import date, datetime
from yahoofinancials import YahooFinancials
import financedatabase as fd
import requests
import json
from bs4 import BeautifulSoup 

class Stock(object): 
    def __init__(self, tickers  = None, exchange = None,  from_date = None, to_date = None): 
        if type(tickers) != list: 
            self.tickers = [tickers]
            
        self.tickers = tickers
        if from_date: 
            self.from_date = from_date
            
        if to_date: 
            self.to_date = to_date 
        else: 
            self.to_date = str(date.today())
        
        self.statement_list = {
        "income": {
            "quarterly": "https://www.marketwatch.com/investing/stock/{}/financials/income/quarter",
            "annual": "https://www.marketwatch.com/investing/stock/{}/financials/income",
        },
        "balance": {
            "quarterly": "https://www.marketwatch.com/investing/stock/{}/financials/balance-sheet/quarter",
            "annual": "https://www.marketwatch.com/investing/stock/{}/financials/balance-sheet",
        },
        "cashflow": {
            "quarterly": "https://www.marketwatch.com/investing/stock/{}/financials/cash-flow/quarter",
            "annual": "https://www.marketwatch.com/investing/stock/{}/financials/cash-flow",
        },
    }

    
    def get_info(self):
        ticker = yf.Ticker(self.symbol)
        return ticker.info
    
    #make the yfinance request
    def make_yfinance_request(self,  dividends = False, stock_splits = False, **kwargs): 
        
        df = yf.Ticker(self.tickers[0]).history(**kwargs)
        
        if not dividends: 
            df.drop("Dividends", inplace=True, axis=1)
            
        if not stock_splits: 
            df.drop("Stock Splits", inplace=True, axis=1)
        
        return df

        
    def get_price(self, dividends = False, stock_splits = False, **kwargs): 
        
        """
        Input: yfinance 
        Output: A dataFrame of OHLC data
        """
        
        #yfinance errors out when time period is more than 1 week or 3 months 
        #for 1min and 5min timeframe. Hence, divide the requests. 
        try: 
            interval = kwargs['interval']
            start_ = datetime.strptime(kwargs['start'], '%y/%m/%d')

            end = kwargs['end']

        except:
            interval = None 
            start = None 
            end = None 

            
        try:
            kwargs.values()
        except: 
            raise ValueError("get_price() should either contain period or start & end dates")
        
        return self.make_yfinance_request(**kwargs)
    
    def quote(self): 
        
         #If not a list, put it in one 

        yf = YahooFinancials(self.tickers, concurrent=True, max_workers=8, country="US")
        tickers = self.tickers
        data = []
        data_to_extract = [("longName", "NAME"), ("regularMarketPrice", "PRICE"), ("regularMarketChangePercent","CHANGES PERCENTAGES"),
                          ("regularMarketChange", "CHANGE"), ("regularMarketDayLow", "DAY LOW"), ("regularMarketDayHigh", "DAY HIGH"),
                          ("marketCap","MARKET CAP"), ("exchangeName","EXCHANGE NAME"), ("regularMarketVolume", "VOLUME"), 
                           ("regularMarketOpen", "OPEN"), ("regularMarketPreviousClose","PREVIOUS CLOSE"), ("quoteSourceName","SOURCE NAME"),("regularMarketTime", "MARKET TIME"), ]
#         yf = YahooFinancials(tickers, concurrent=True, max_workers=8, country="US")
        yf_stock = yf.get_stock_price_data()

        yearly_high = yf.get_yearly_high()
        yearly_low = yf.get_yearly_low()
        price_avg50 = yf.get_50day_moving_avg()
        price_avg200 = yf.get_200day_moving_avg()
        eps = yf.get_earnings_per_share()
        pe = yf.get_pe_ratio()


        for ticker in tickers: 
            temp_data = {"TICKER": ticker}
            for tup in data_to_extract: 
                temp_data[tup[1]] = yf_stock[ticker][tup[0]]
            temp_data["YEAR HIGH"] = yearly_high[ticker]
            temp_data["YEAR LOW"] = yearly_low[ticker]
            temp_data["PRICE AVG50"] = price_avg50[ticker]
            temp_data["PRICE AVG200"] = price_avg200[ticker]
            temp_data["EPS"] = eps[ticker]
            temp_data["PE"] = pe[ticker]


            data.append(temp_data)
        
        return pd.DataFrame(data)
    
    def search(self, query = "",
    country: str = "",
    sector: str = "",
    industry_group: str = "",
    industry: str = ""):
        """
        Search by industry and other details using the finance database 
        """
        kwargs: Dict[str, Any] = {"exclude_exchanges": False}
      
        if country:
            kwargs["country"] = country.replace("_", " ").title()
        if sector:
            kwargs["sector"] = sector
        if industry:
            kwargs["industry"] = industry
        if industry_group:
            kwargs["industry_group"] = industry_group

        eq = fd.Equities()
        data = eq.search( **kwargs, name = query)
       #Initially we only returned some of the values, same as OpenBB
       #Now decided to return all. Have to decide what to do. 
        returned_data = data
        
        return returned_data
    
    def get_top_of_book(self,
    symbol: str, exchange: str = "BZX"
):
   

        if exchange not in ["BZX", "EDGX", "BYX", "EDGA"]:
            print("Pick one of the following exchanges: ['BZX', 'EDGX', 'BYX', 'EDGA']", )
            return 

        request_url = f"https://www.cboe.com/json/{exchange.lower()}/book/{symbol}"

        r = requests.get(
            request_url,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)"
                " Chrome/106.0.0.0 Safari/537.36",
                "referer": "https://www.cboe.com/us/equities/market_statistics/book_viewer/",
            },
        )
        if r.status_code != 200:
            print("Unable to fetch data")
            return 

        r_json = r.json()
        if r_json["data"]["company"] == "unknown symbol":
            print("Symbol not recognized")

        bids = pd.DataFrame(r_json["data"]["bids"], columns=["Size", "Price"])
        asks = pd.DataFrame(r_json["data"]["asks"], columns=["Size", "Price"])
        if bids.empty or asks.empty:
            print("Only real-time data. No aftermarket.")
        return bids.join(asks, lsuffix= ': Bid', rsuffix = ': Ask')
    
    def get_sec_filings(self, symbol:str):
        NASDAQ_HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }
        data = requests.get(f"https://api.nasdaq.com/api/company/{symbol}/sec-filings", headers =NASDAQ_HEADERS)
        parse = data.json()["data"]["rows"]
        links = []
        for i in parse: 
            links.append(i["view"]["docLink"])
        df = pd.DataFrame(parse)    
        df["download_link"] = links
        df = df.rename(columns={'companyName': 'name', 'formType': 'form_type'})
        return df[["name", "form_type", "filed", "period", "download_link"]]
    
    def get_insider_data(self, symbol):
        """
        Returns the OpenInsider data for a ticker 
        Input: str - the ticker symbol 
        Output: pd.DataFrame - The last 4 years insider trading data for the given ticker
        """
        url = f'http://openinsider.com/screener?s={symbol}&o=&pl=&ph=&ll=&lh=&fd=1461&fdr=&td=0&tdr=&fdlyl=&fdlyh=&daysago=&xp=1&xs=1&vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=1000&page=1'.format(symbol)
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        try:
                rows = soup.find('table', {'class': 'tinytable'}).find('tbody').findAll('tr')
        except:
                print("Error! Skipping")

        data = []
        for row in rows:
            cols = row.findAll('td')
            if not cols:
                continue
            insider_data = {key: cols[index].find('a').text.strip() if cols[index].find('a') else cols[index].text.strip() 
                            for index, key in enumerate(['X', 'transaction_date', 'trade_date', 'ticker', 'Insider_Name', 
                                                          'Title', 'transaction_type', 'last_price', 'Qty', 
                                                         'shares_held', 'Owned', 'Value'])}
            data.append(insider_data)
        return pd.DataFrame(data)
    
    def get_income_balance_cashflow(self, ticker, statement_type, quarter = False): 
        period = "quarterly" if quarter else "annual"
        req = requests.get(
                        self.statement_list[statement_type][period].format(ticker),
                        headers={
                        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)"
                        " Chrome/106.0.0.0 Safari/537.36",
                        "referer": "https://www.cboe.com/us/equities/market_statistics/book_viewer/",
                    })
        content = BeautifulSoup( req.text,  "lxml")
        list_headers = []
        list_headers = [
            financials_header.text.strip("\n").split("\n")[0]
            for financials_header in content.findAll(
                "th", {"class": "overflow__heading"}
            )
        ]

        list_trend = ("5-year trend", "5- qtr trend")[quarter]
        if list_trend in list_headers:
            df_financials = pd.DataFrame(
                columns=list_headers[
                    0 : list_headers.index(list_trend)
                ]
            )
        else:
              print("Error")


        table = content.findAll(
            "div", {"class": "element element--table table--fixed financials"}
        )

        # if not find_table:
        #     return df_financials

        rows = table[0].findAll(
            "tr", {"class": ["table__row is-highlighted", "table__row"]}
        )


        for a_row in rows:
            constructed_row = []
            financial_columns = a_row.findAll(
                "td", {"class": ["overflow__cell", "overflow__cell fixed--column"]}
            )

            if not financial_columns:
                continue

            for a_column in financial_columns:
                column_to_text = a_column.text.strip()
                if "\n" in column_to_text:
                    column_to_text = column_to_text.split("\n")[0]

                if column_to_text == "":
                    continue
                constructed_row.append(column_to_text)

            df_financials.loc[len(df_financials)] = constructed_row
        return df_financials 





















