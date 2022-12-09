import xgboost as xgb
import datetime
from parser import DataParser
from utils.dblib import rw_session, MarketData, Trade
from sqlalchemy import desc
import pandas as pd
from utils.signal_process import construct_features_and_labels
from utils.config import REALTIME_DATA
from kafka import KafkaConsumer, TopicPartition
import hashlib
import time
  

NUM_ROW = 60 

    
class LiveTrading:
    def __init__(self, cash: float=100000):
        self.cash = cash
        self.risk_ratio = 0.1
        self.fill_rate = 0.8
        self.position = 0
        self.cost_basis = 0
        self.fee = 12 / 10000.
        self.consumer = KafkaConsumer()
        self.model = self.load_model()
        self.trade_hist = []

    def load_status(self):
        with rw_session() as session:
            query = session.query(
                Trade.cash, Trade.position,
                ).order_by(
                desc(Trade.insertUTC)).limit(1)

            if query.first():
                for cash, position in query:
                    self.cash = cash
                    self.position = position
            

    def load_model(self, path: str = "/Users/rynn/Documents/GitHub/project/server_code/xgb_model.json"):
        xgb_model = xgb.XGBClassifier()
        xgb_model.load_model(path)

        return xgb_model

    def process_data(self):
        with rw_session() as session:
            query = session.query(MarketData).order_by(
                desc(MarketData.insertUTC)).limit(NUM_ROW)

            raw_data = pd.read_sql(query.statement, query.session.bind)


        raw_data=raw_data.rename(columns={
                "high":"High",
                "low":"Low",
                "open":"Open",
                "close":"Close",
                "vol":"Volume",
            })
        processed_data = construct_features_and_labels(raw_data, overwrite=True)
        processed_data = processed_data.drop(columns=['Open', 'Close', 'High', 'Low', 'Volume', 'insertUTC'])

        return processed_data

    def get_live_market_data(self):
        tp = TopicPartition(REALTIME_DATA, 0)
        self.consumer.assign([tp])
        self.consumer.poll()
        self.consumer.seek_to_end()
        for msg in self.consumer:
            side, price, size, dt = msg.value.decode("utf-8") .split("|")
            break

        return side, price, size, dt


    def process_signal(self, signal):
        side, price, size, dt = self.get_live_market_data()
        price = float(price)

        tradable_size = self.risk_ratio * self.fill_rate * float(size)

        executable_size = min(self.cash / price, tradable_size)

        if signal == 2 and executable_size > 0:
            self.position += executable_size
            self.cash -= executable_size * price
            if self.cost_basis == 0:
                self.cost_basis = price
            else:
                self.cost_basis = ((self.cost_basis * self.position) + price * executable_size) / self.position 
            realized_pnl = 0
        elif signal == 1 and executable_size > 0:
            self.position -= executable_size
            self.cash += executable_size * price
            realized_pnl = (price - self.cost_basis) * executable_size
            if self.cost_basis == 0:
                self.cost_basis = price
            else:
                self.cost_basis = ((self.cost_basis * self.position) - price * executable_size) / self.position 

        unrealized_pnl = (price - self.cost_basis) * self.position

        if executable_size > 0: 
            now = datetime.datetime.utcnow()
            exe_side = "B" if signal == 2 else "S"
            fee = round(executable_size * price * self.fee, 5)
            
            if len(self.trade_hist) > 5:
                _, _, executable_size = self.trade_hist.pop(0)

            print(f"side {exe_side} | size {executable_size} | position {self.position} | realized_pnl {realized_pnl} | fee {fee}")

            with rw_session() as session:
                session.add(
                    Trade(
                        insertUTC = now,
                        trade_id =  hashlib.md5(str(time.time()).encode('utf-8')).hexdigest(),
                        filled_qty = round(float(executable_size),8),
                        filled_price = price,
                        side = exe_side,
                        position = round(self.position, 8),
                        cash = round(self.cash, 8),
                        realized_pnl = round(realized_pnl, 8),
                        unrealized_pnl = round(unrealized_pnl, 8),
                        cost_basis=round(self.cost_basis, 8),
                        fee = fee,
                    )
                )
            self.trade_hist.append([now, exe_side, executable_size])

    def run(self):
        self.load_status()
        model = self.load_model()
        while True:
            data = self.process_data()
            signal = model.predict(data)[-1]
            if signal != 0 and len(self.trade_hist) <= 5:
                self.process_signal(signal)
            elif len(self.trade_hist) > 5:
                print("Closing position ")
                lag = (datetime.datetime.utcnow() - self.trade_hist[0][0]).total_seconds()
                if lag >= 5*10:
                    self.process_signal(1 if self.trade_hist[0][1] == "B" else 2)

            time.sleep(11)


if __name__ == "__main__":
    bot = LiveTrading()
    bot.run()

