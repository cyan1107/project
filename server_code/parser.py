from kafka import KafkaConsumer
from utils.config import REALTIME_DATA

class DataParser:
    def __init__(self, mode: str):
        self.mode=mode
        self.live_receiver=None
        
    def _init_parser(self):
        if self.mode == "live":
            self.live_receiver =  KafkaConsumer(REALTIME_DATA)
        else:
            pass 

    def _live_parser(self):
        

    def _batch_parser(self, path: str):
        pass 
