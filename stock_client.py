import sys
import requests
import random
import string
import json
import time
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


class StockClient:
    def __init__(self, loops: int, tracer):
        self.api_url: str = 'http://localhost:8080/stocks'
        self.stocks: list = []
        self.loops: int = loops if loops >= 0 else 10  # 0 - endless
        self.tracer = tracer
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
        with self.tracer.start_as_current_span("get_all_stocks") as span:
            resp = requests.get(self.api_url, headers=self.__make_headers())
            self.stocks = json.loads(resp.text)

    
    def update_stock(self, index: int):
        with self.tracer.start_as_current_span("update_stock") as parent:
            if len(self.stocks) < index - 1 or index < 0:
                print('nothing to update')
                return
            else:
                stock = self.__make_stock()
                resp = requests.patch(f"{self.api_url}/{stock['isin']}", data=json.dumps({"stock":stock}), headers=self.__make_headers())


    def create_stock(self):
        with self.tracer.start_as_current_span("create_stock") as parent:
            stock = {}
            stock['stock'] = self.__make_stock()
            resp = requests.post(self.api_url, data=json.dumps(stock), headers=self.__make_headers())


    def show_stock(self, index: int):
        with self.tracer.start_as_current_span("show_stock") as parent:
            isin = self.__pick_stock(index)
            resp = requests.get(f"{self.api_url}/{isin}", headers=self.__make_headers())


    def delete_stock(self):
        with self.tracer.start_as_current_span("delete_stock") as parent:
            isin = self.__randstr(6)
            resp = requests.delete(f"{self.api_url}/{isin}", headers=self.__make_headers())


    def __pick_stock(self, index: int) -> str:
        with self.tracer.start_as_current_span("pick_stock") as child:
            isin = ''
            if len(self.stocks) < index - 1 or index < 0:
                isin = self.__randstr(6)
            else:
                isin = self.stocks[index]['isin']
            return isin


    def __make_headers(self) -> dict:
        with self.tracer.start_as_current_span("make_header") as child:
            headers = {}
            headers['Content-Type'] = 'application/json'
            return headers


    def __make_stock(self) -> dict:
        with self.tracer.start_as_current_span("make_stock") as child:
            stock = {}
            stock['isin'] = self.__randstr(6)
            stock['name'] = self.__randstr(10)
            stock['price'] = random.random() * 10000
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

n = len(sys.argv)
num_loops = 1
if n > 1:
    num_loops = int(sys.argv[1])
s = StockClient(num_loops, tracer)
s.work()

