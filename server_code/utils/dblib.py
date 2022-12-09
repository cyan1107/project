from sqlalchemy import create_engine
from contextlib import contextmanager
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, DateTime, Float, String

Base = declarative_base()

class MarketData(Base):
    __tablename__ = "market_data"

    insertUTC = Column(DateTime, primary_key=True)
    high = Column(Float)
    low = Column(Float)
    open = Column(Float)
    close = Column(Float)
    vol = Column(Float)


class Trade(Base):
    __tablename__ = "trade_data"
    
    insertUTC = Column(DateTime, primary_key=True)
    trade_id = Column(String, primary_key=True)
    filled_qty = Column(Float) 
    filled_price = Column(Float) 
    side = Column(String) 

    cost_basis = Column(Float) 
    position = Column(Float) 
    cash = Column(Float) 
    realized_pnl = Column(Float) 
    unrealized_pnl = Column(Float) 
    fee = Column(Float) 

@contextmanager
def rw_session():
    engine = create_engine("mysql+pymysql://root:Yc890703!@localhost/trading")
    Session = sessionmaker(engine)
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
