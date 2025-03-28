import sys
import requests
import random
import string
import json
import time
import os

class StockClient:
    def __init__(self, loops: int):
        server:str|None = os.environ.get('SRVURL')
        if server is None:
            server = 'http://localhost:8080'
        self.api_url: str = f'http://{server}:8080/stocks'
        self.stocks: list = []
        self.loops: int = loops if loops >= 0 else 10  # 0 - endless
        self.pause = 0.901

    def work(self):
        i = 0
        while self.loops == 0 or i < self.loops:  # 0 - endless loop
            self.get_all_stocks()
#            for z in range(0, self.pause):
#                pass
            time.sleep(self.pause)
            for j in range (0, len(self.stocks)):
                self.show_stock(j)
                self.update_stock(j)
#                for z in range(0, self.pause):
#                    pass
                time.sleep(self.pause)
            self.delete_stock()
            for k in range(0, 5):
                self.create_stock()
            i += 1
    

    def get_all_stocks(self):
        resp = requests.get(self.api_url, headers=self.__make_headers())
        self.stocks = json.loads(resp.text)

    
    def update_stock(self, index: int):
        if len(self.stocks) < index - 1 or index < 0:
            print('nothing to update')
            return
        else:
            stock = self.__make_stock()
            resp = requests.patch(f"{self.api_url}/{stock['isin']}", data=json.dumps({"stock":stock}), headers=self.__make_headers())


    def create_stock(self):
        stock = {}
        stock['stock'] = self.__make_stock()
        resp = requests.post(self.api_url, data=json.dumps(stock), headers=self.__make_headers())


    def show_stock(self, index: int):
        isin = self.__pick_stock(index)
        resp = requests.get(f"{self.api_url}/{isin}", headers=self.__make_headers())


    def delete_stock(self):
        isin = self.__randstr(6)
        resp = requests.delete(f"{self.api_url}/{isin}", headers=self.__make_headers())


    def __pick_stock(self, index: int) -> str:
        isin = ''
        if len(self.stocks) < index - 1 or index < 0:
            isin = self.__randstr(6)
        else:
            isin = self.stocks[index]['isin']
        return isin


    def __make_headers(self) -> dict:
        headers = {}
        headers['Content-Type'] = 'application/json'
        return headers


    def __make_stock(self) -> dict:
        stock = {}
        stock['isin'] = self.__randstr(6)
        stock['name'] = self.__randstr(10)
        stock['price'] = random.random() * 10000
        return stock

    
    def __randstr(self, length: int) -> str:
        characters = string.ascii_uppercase + string.digits
        return ''.join(random.choice(characters) for _ in range(length))


n = len(sys.argv)
num_loops = 1
if n > 1:
    num_loops = int(sys.argv[1])
s = StockClient(num_loops, tracer)
s.work()

