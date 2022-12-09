from kafka import KafkaConsumer
import datetime
from utils.config import REALTIME_DATA
from utils.dblib import rw_session, MarketData

class DataParser:
    def __init__(self, mode: str="live", time_interval: int=10):
        self.mode=mode
        self.time_interval = time_interval
        self.live_receiver=None
        self.data = {}
        self._init_parser()
        
    def _init_parser(self):
        if self.mode == "live":
            self._init_data()
            self.live_receiver =  KafkaConsumer(REALTIME_DATA)
        else:
            pass 

    def _init_data(self):
        self.data = {
            "high":0,
            "low":float("inf"),
            "open":0,
            "close":0,
            "vol":0
        }

    def _live_parser(self):
        pre_time = datetime.datetime.utcnow()
        print("Parser starts")
        for msg in self.live_receiver:
            side, price, size, dt = msg.value.decode("utf-8") .split("|")
            #2022-12-07T15:13:31.776625Z
            price = float(price)
            size = float(size)

            cur_time = datetime.datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%fZ")

            if self.data["open"] == 0:
                self.data["open"] = price

            if (cur_time - pre_time).total_seconds() >= self.time_interval:
                pre_time = cur_time
                self.data["close"] = price
                self.data["vol"] += size
                with rw_session() as session:
                    print(self.data)
                    session.add(
                        MarketData(
                            insertUTC = cur_time,
                            high = self.data["high"],
                            low = self.data["low"],
                            open = self.data["open"],
                            close = self.data["close"],
                            vol = self.data["vol"]
                        )
                    )
                self._init_data()
            else:
                self.data["high"] = max(self.data["high"], price)
                self.data["low"] = min(self.data["low"], price)
                self.data["vol"] += size
   

if __name__ == "__main__":
    parser = DataParser()
    parser._live_parser()


        

 