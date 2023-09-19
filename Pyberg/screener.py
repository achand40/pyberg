import json 
import requests 
import pandas as pd 

class Screener(object): 
    def __init__(self):
            self.inst = True 
            self.parameters_dict = {
            "most_capitalized" : ("market_cap_basic", "desc"),
            "volume_leaders" : ("volume", "desc"),
            "top_gainers" : ("change", "desc"), 
            "top_losers" : ("change", "asc"),
            "all_time_high": ("name", "asc"), 
            "volume_leaders" : ("volume", "desc"),
            "high_dividend" : ("dividend_yield_recent", "desc")}
            self.url = 'https://scanner.tradingview.com/global/scan'
            self.headers = {
   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
   'Referer': 'https://www.tradingview.com/screener/'
}           
            f = open('payload.json')
            self.payload = json.load(f)
            
    def get_changing_vals(self, query): 
        """
        Input: The filter
        Output: A tuple of the changing values for the main query
        """
        return self.parameters_dict[query]
    
    def construct_query(self, changing_vals):
        """
        Input: The changing values 
        Output: The fully formed payload to request to 
        """
        payload = {
              "filter": [
                {"left": "type", "operation": "in_range", "right": ["NYSE", "NASDAQ"]},
                {"left": "sector", "operation": "in_range", "right": ["Finance", "Technology Services"]}
              ],
              "options": {"lang": "en"},
              "symbols": {"query": {"types": []}, "tickers": []},
              "columns": ["name", "close", "change", "volume", "Value.Traded",
                          "market_cap_basic", "price_earnings_ttm", 
                          "earnings_per_share_basic_ttm", "sector", "description"],
               "sort": {"sortBy": changing_vals[0], "sortOrder": changing_vals[1]},
            }
        payload = {"filter":[{"left":"type","operation":"in_range","right":["stock","dr","fund"]},
                             {"left":"subtype","operation":"in_range","right":["common","foreign-issuer","","etf","etf,odd","etf,otc","etf,cfd"]},
                             {"left":"exchange","operation":"in_range","right":["NYSE","NASDAQ","AMEX"]},{"left":"change","operation":"greater","right":0},
                             {"left":"close","operation":"in_range","right":[2,10000]},{"left":"is_primary","operation":"equal","right": True},
                             {"left":"active_symbol","operation":"equal","right":True}],"options":{"lang":"en"},"markets":["america"],"symbols":{"query":{"types":[]},"tickers":[]},"columns":["logoid","name","close","change","change_abs","Recommend.All","volume","Value.Traded","market_cap_basic","price_earnings_ttm","earnings_per_share_basic_ttm","number_of_employees","sector","description","type","subtype","update_mode","pricescale","minmov","fractional","minmove2","currency","fundamental_currency_code"],
                           "sort":{"sortBy": changing_vals[0],"sortOrder":changing_vals[1]},"range":[0,150]} 

    
        print(payload)
        return payload
    
    def make_query(self, query): 
        "Input: The specific filter for the screener"
        "Output: A JSON object from TradingView"
        payload = self.payload[query]
        response = requests.post(self.url, headers= self.headers, json= payload)
        return response.json()
    
    def format_data(self, data): 
        "Input: the TradingView JSON Object"
        "Output : A pandas DataFrame of the JSON Object"
        total_data = []
        for ticker in data: 
            info = ticker['d']
            symbol = info[1]
            price = info[2]
            change_pct = info[3]
            change = info[4]
            volume = info[6]
            volume_price = info[7]
            mkt_cap = info[8]
            eps = info[10]
            number_employees = info[11]
            sector = info[12]
            name = info[13]
            data = {"Symbol" : symbol, 
                               "Price" : price, 
                               "Change_Percentage" : change_pct, 
                               "Change_Absolute" : change, 
                               "Volume" : volume,
                            "Volume_Price" : volume_price, 
                            "Market_Cap"  : mkt_cap, 
                            "EPS" : eps, 
                            "Employee_Count" : number_employees, 
                            "Sector" : sector, 
                            "Name" : name}
            total_data.append(data)

        return pd.DataFrame(total_data)
        
    def get_data(self, query): 
        "Input : The filter to use from TradingView"
        "Output"
        req = self.make_query(query)
        data = self.format_data(req['data'])
        return data 

    
    
    
    
    