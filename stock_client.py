import sys
import requests
import random
import string
import json
import time
import os
import data_dict
import logging
import datetime
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    DEFAULT_COMPRESSION,
    DEFAULT_ENDPOINT,
    DEFAULT_TIMEOUT,
    DEFAULT_TRACES_EXPORT_PATH,
    OTLPSpanExporter,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
)
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

# logger = logging.getLogger(__name__)
# logging.basicConfig(level=logging.INFO)

log_dest:str|None = os.getenv("LOG_DEST")
if log_dest is None or log_dest == 'screen':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
else:
    logging.basicConfig(filename=f'{log_dest}_srv.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

dateformat:str='%F %T.%f'

class StockClient:
    def __init__(self, server:str|None, loops:int, create_stocks:int, hardwork_intens:int):
        if server is None:
            server = 'localhost:8080'
        self.api_url:str = f'http://{server}/stocks'
        self.hw_api:str = f'http://{server}/hardwork'
        self.stocks: list = []
        self.loops: int = loops if loops >= 0 else 10  # 0 - endless
        self.cr_stocks:int = create_stocks
        self.hw_int:int = hardwork_intens
        logging.info(f'INIT: {self.get_timestamp()}: connecting to server {server}')
        logging.info(f'INIT: {self.get_timestamp()}: loops to be executed {loops}')
        logging.info(f'INIT: {self.get_timestamp()}: Hard Work intensity: once per {hardwork_intens} loops')

    def get_timestamp(self)->str:
        return datetime.datetime.now().strftime(dateformat)[:-3]

    def work(self):
        i = 0
        while self.loops == 0 or i < self.loops:  # 0 - endless loop
            if i > self.hw_int and (i % self.hw_int == 1 or i % self.hw_int == 2):  # simulate hard work on the server every 100th iteration
                logging.info(f'STOCK WORKER: {self.get_timestamp()}: Setting HardWork: {i % self.hw_int == 1}')
                hw:dict = {'hw': i % self.hw_int == 1}
                _ = requests.post(self.hw_api, data=json.dumps(hw), headers=self.__make_headers())

            logging.info(f'STOCK WORKER: {self.get_timestamp()}: loop {i + 1} of {self.loops}')

            logging.info(f'STOCK WORKER: {self.get_timestamp()}: getting stocks')
            self.get_all_stocks()
            for j in range (0, len(self.stocks)):
                logging.info(f'STOCK WORKER: {self.get_timestamp()}: looping stocks: {j + 1} or {len(self.stocks)}')
                logging.info(f'STOCK WORKER: {self.get_timestamp()}: showing stock # {j + 1}')
                self.show_stock(isin=self.__pick_stock(index=j))
                logging.info(f'STOCK WORKER: {self.get_timestamp()}: updating stock # {j + 1}')
                self.update_stock(isin=self.__pick_stock(index=j))

            logging.warning(f'STOCK WORKER: {self.get_timestamp()}: showing a non existing stock')
            self.show_stock(isin='NONEXISTING')
            logging.warning(f'STOCK WORKER: {self.get_timestamp()}: updating a non existing stock')
            self.update_stock(isin='NONEXISTING')

            logging.info(f'STOCK WORKER: {self.get_timestamp()}: deleting a stock')
            self.delete_stock(isin=None)
            logging.warning(f'STOCK WORKER: {self.get_timestamp()}: deleting a non existing stock')
            self.delete_stock(isin='NONEXISTING')
            for k in range(0, self.cr_stocks):
                logging.info(f'STOCK WORKER: {self.get_timestamp()}: creating stock {k + 1} of {self.cr_stocks}')
                self.create_stock(isin=None)
            # call create for an existing stock
            logging.warning(f'STOCK WORKER: {self.get_timestamp()}: calling create for an existing stock')
            self.create_stock(isin=self.__pick_stock(index=-1))
            i += 1
    

    def get_all_stocks(self):
        with self.tracer.start_as_current_span("get_all_stocks") as span:
            logging.info(f'GET ALL STOCKS: {self.get_timestamp()}: getting all stocks')
            resp = requests.get(self.api_url, headers=self.__make_headers())
            self.stocks = json.loads(resp.text)

    
    def update_stock(self, isin:str|None):
        with self.tracer.start_as_current_span("update_stock") as parent:
            if isin is None:
                isin = self.__pick_stock(index=-1)
            logging.info(f'UPDATE STOCK: {self.get_timestamp()}: updating stock {isin}')
            stock = self.__make_stock(isin=isin)
            logging.info(f'UPDATE STOCK: {self.get_timestamp()}: patching stock {stock['isin']}')
            _ = requests.patch(f"{self.api_url}/{stock['isin']}", data=json.dumps({"stock":stock}), headers=self.__make_headers())


    def create_stock(self, isin:str|None):
        with self.tracer.start_as_current_span("create_stock") as parent:
            stock = {}
            stock['stock'] = self.__make_stock(isin=isin)
            logging.info(f'CREATE STOCK: {self.get_timestamp()}: created stock {stock['stock']['isin']}')
            _ = requests.post(self.api_url, data=json.dumps(stock), headers=self.__make_headers())


    def show_stock(self, isin:str|None):
        with self.tracer.start_as_current_span("show_stock") as parent:
            if isin is None:
                isin = self.__pick_stock(index=-1)
            logging.info(f'GET STOCK: {self.get_timestamp()}: showing stock {isin}')
            _ = requests.get(f"{self.api_url}/{isin}", headers=self.__make_headers())


    def delete_stock(self, isin:str|None):
        with self.tracer.start_as_current_span("delete_stock") as parent:
            if isin is None:
                isin = self.__pick_stock(index=-1)
            for s in self.stocks:
                if s['isin'] == isin:
                    self.stocks.remove(s)
                    break
            logging.info(f'DELETE STOCK: {self.get_timestamp()}: deleting stock {isin}')
            _ = requests.delete(f"{self.api_url}/{isin}", headers=self.__make_headers())


    def __pick_stock(self, index: int) -> str:
        with self.tracer.start_as_current_span("pick_stock") as child:
            isin = ''
            if index < 0:
                index = random.randint(0, len(self.stocks) - 1)
            if len(self.stocks) < index - 1 or index < 0:
                logging.error(f'STOCK PICKER: {self.get_timestamp()}: no stock to pick')
                isin = self.__randstr(6)
            else:
                isin = self.stocks[index]['isin']
                logging.info(f'STOCK PICKER: {self.get_timestamp()}: picked stock {isin}')
            return isin


    def __make_headers(self) -> dict:
        with self.tracer.start_as_current_span("make_header") as child:
            headers = {}
            headers['Content-Type'] = 'application/json'
            return headers


    def __make_stock(self, isin:str|None) -> dict:
        with self.tracer.start_as_current_span("make_stock") as child:
            stock = {}
            stock['isin'] = isin if isin is not None else self.__randstr(6)
            stock['name'] = self.__randstr(10)
            stock['price'] = random.random() * 10000
            stock['currency'] = list(data_dict.currencies.keys())[random.randint(0, len(data_dict.currencies) - 1)]
            logging.info(f'STOCK MAKER: {self.get_timestamp()}: generated stock {stock['isin']}')
            return stock

    
    def __randstr(self, length: int) -> str:
        with self.tracer.start_as_current_span("rand_str") as child:
            characters = string.ascii_uppercase + string.digits
            return ''.join(random.choice(characters) for _ in range(length))


# Service name is required for most backends
resource = Resource(attributes={
    SERVICE_NAME: "stock_client_python"
})
provider = TracerProvider(resource=resource)
otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:14499/otlp/v1/traces")
processor = BatchSpanProcessor(otlp_exporter)
provider.add_span_processor(processor)
# Sets the global default tracer provider
trace.set_tracer_provider(provider)

# Creates a tracer from the global tracer provider
tracer = trace.get_tracer("stock.client.python")

server:str|None = os.environ.get('SRVURL')
loops:str|None = os.environ.get('NUMLOOPS')
cr_stocks:str|None = os.environ.get('CREATESTOCKS')
hw_intens:str|None = os.environ.get('HARDWORK_INTENSITY')

num_loops:int = int(loops) if loops is not None else 10
create_stocks:int = int(cr_stocks) if cr_stocks is not None else 10
hardwork_intens:int = int(hw_intens) if hw_intens is not None else 100

s = StockClient(server=server, loops=num_loops, create_stocks=create_stocks, hardwork_intens=hardwork_intens)
s.work()
