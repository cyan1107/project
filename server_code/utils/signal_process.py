
import numpy as np
import pandas as pd

def calc_price_movement(df, n, a):
    """
    Calcuate the price movenment of next `n` data points given threshold `a`
    Movement is encoded into 1, 0, and -1
    """
    if 'PM_'+str(n)+'_'+str(a) in df.columns:
        return df
    else:
        price_movement = np.empty(len(df))
        price_movement[:] = np.nan

        for i in range(len(df)-n):
            future_mean_price = np.mean(df['Close'][i+1:i+n+1])
            if future_mean_price / df['Close'][i] - 1 > a:
                price_movement[i] = 1
            elif future_mean_price / df['Close'][i] - 1 < -a:
                price_movement[i] = -1
            else:
                price_movement[i] = 0

        df['PM_'+str(n)+'_'+str(a)] = price_movement

        return df

def calc_indicator_macd(df, short=12, long=26, overwrite=False):
    """
    Trend
    
    :return: MACD_Signal
    """
    if 'MACD_Signal' in df.columns and not overwrite:
        return df
    
    shortdf= df['Close'].ewm(span=short, adjust=False, min_periods=short).mean()
    longdf = df['Close'].ewm(span=long, adjust=False, min_periods=long).mean()
    
    df['MACD_Signal'] = np.where(shortdf - longdf > 0, 1, -1)
    
    return df


def calc_indicator_sma(df, short=5, mid=15, long=30, overwrite=False):
    """
    Trend
    
    :return: SMA Signal
    """
    if 'SMA_Signal' in df.columns and not overwrite:
        return df
    
    shortdf = df['Close'].rolling(window = short).mean()
    middf = df['Close'].rolling(window = mid).mean()
    longdf = df['Close'].rolling(window = long).mean()
    
    signal = np.zeros(len(df))
    
    for i in range(len(df)):
        if shortdf.iloc[i] > middf.iloc[i] > longdf.iloc[i] :
            signal[i] = 3
        if middf.iloc[i] > shortdf.iloc[i] > longdf.iloc[i] :
            signal[i] = 1
        if longdf.iloc[i] > shortdf.iloc[i] > middf.iloc[i] :
            signal[i] = -1
        if longdf.iloc[i] > middf.iloc[i] > shortdf.iloc[i] :
            signal[i] = -3
    
    df['SMA_Signal'] = signal
    
    return df


def calc_indicator_rsi(df, n=14, lb=30, ub=70, overwrite=False):
    """
    Momentum
    
    RSI
    
    :param n: the timestep used when calculating RSI, standard parameter is 14.
    
    :return: RSI_Signal
    """
    if 'RSI_Signal' in df.columns and not overwrite:
        return df
    
    close_delta = df['Close'].diff()
    up = close_delta.clip(lower=0)
    down = -1 * close_delta.clip(upper=0)
    ma_up = up.ewm(com = n - 1, adjust=True, min_periods = n).mean()
    ma_down = down.ewm(com = n - 1, adjust=True, min_periods = n).mean()
        
    rsi = ma_up / ma_down
    df2 = pd.DataFrame(index = df.index)
    df2['RSI'] = 100 - (100/(1 + rsi))
    
    df['RSI_Signal'] = np.where(df2['RSI']>ub, 1, np.where(df2['RSI']<lb, -1, 0))
    
    return df


def calc_indicator_kdj(df, n=14, m=3, overwrite=False):
    """
    Momentum
    
    KDJ
    
    :param n: # of observations in the window
    :param m: SMA window
    
    :return: KDJ_Signal
    """
    if 'KDJ_Signal' in df.columns and not overwrite:
        return df
    
    n_high = df['High'].rolling(n).max()
    n_low = df['Low'].rolling(n).min()
    
    K = (df['Close'] - n_low) / (n_high - n_low) * 100
    D = K.rolling(m).mean()
    J = K - D
    
    df['KDJ_Signal'] = np.where(J > 0, 1, np.where(J < 0, -1, 0))
    
    return df
    

def calc_indicator_abstr(df, n=14, overwrite=False):
    """
    Volatility
    
    Absolute True Range
    
    function that return df with the Absolute True Range
    absTR is a volatility indicator, the higher the absTR the higher the volatility
    df is the dataframe containing the price info of the stocks, n is the parameter of rolling average
    high_low = high - low, high_cp = high - previous close, low_cp = low - previous close
    
    :return: ABSTR_Signal
    """
    if 'ABSTR_Signal' in df.columns and not overwrite:
        return df
    
    high_low = df['High'] - df['Low']
    high_cp = np.abs(df['High'] - df['Close'].shift())
    low_cp = np.abs(df['Low'] - df['Close'].shift())

    # true range is the max of the three
    true_range_calc = pd.concat([high_low, high_cp, low_cp], axis=1)
    true_range = np.max(true_range_calc, axis = 1)

    # absTR is the rolling average of the true range and n can be changed as a parameter for optimization
    absolute_true_range = pd.DataFrame(index = df.index)
    absolute_true_range['absTR'] = true_range.rolling(n).mean()
    
    df['ABSTR_Signal'] = np.where(np.abs(df['Close'] - df['Close'].shift()) > absolute_true_range['absTR'], 1, 0)
    
    return df


def calc_indicator_bb(df, n=20, m=2, overwrite=False):
    """
    Volatility
    
    Bollinger Bands
    
    :param n: window for mean and std
    :param m: band width in terms of std
    
    :return: BB_Signal
    """
    if 'BB_Signal' in df.columns and not overwrite:
        return df
    
    midBB = df['Close'].rolling(n).mean()
    std = df['Close'].rolling(n).std()
    
    upperBB = midBB + m * std
    lowerBB = midBB - m * std
    
    pctB = (df['Close'] - lowerBB) / (upperBB - lowerBB)
    
    df['BB_Signal'] = pctB
    
    return df


def calc_indicator_pvi(df, n=60, overwrite=False):
    """
    Volume
    
    PVI
    
    :param n: SMA window
    
    :return: PVI_Signal
    """
    if 'PVI_Signal' in df.columns and not overwrite:
        return df
    
    pvi = np.zeros(len(df))
    pvi[0] = 1
    
    for i in range(1, len(df)):
        if df['Volume'][i] > df['Volume'][i-1]:
            pvi[i] = df['Close'][i] / df['Close'][i-1] * pvi[i-1]
        else:
            pvi[i] = pvi[i-1]
    
    pvi_seires = pd.Series(pvi)
    df['PVI_Signal'] = np.where(pvi_seires > pvi_seires.rolling(n).mean(), 1, 
                                np.where(pvi_seires < pvi_seires.rolling(n).mean(), -1, 0))
        
    return df


def calc_indicator_nvi(df, n=60, overwrite=False):
    """
    Volume
    
    NVI
    
    :param n: SMA window
    
    :return: NVI_Signal
    """
    if 'NVI_Signal' in df.columns and not overwrite:
        return df
    
    nvi = np.zeros(len(df))
    nvi[0] = 1
    
    for i in range(1, len(df)):
        if df['Volume'][i] < df['Volume'][i-1]:
            nvi[i] = df['Close'][i] / df['Close'][i-1] * nvi[i-1]
        else:
            nvi[i] = nvi[i-1]
    
    nvi_seires = pd.Series(nvi)
    df['NVI_Signal'] = np.where(nvi_seires > nvi_seires.rolling(n).mean(), 1, 
                                np.where(nvi_seires < nvi_seires.rolling(n).mean(), -1, 0))
        
    return df


def calc_indicator_xxxx(df, overwrite=False, **args):
    """
    Placeholder
    
    Calculate technical indicator 'xxxx' for a stock
    
    :param df: dataframe from the dictionary loaded from 'SP5T5.xlsx'
    :param **args: potential parameters of the indicator that can be tuned
    
    :return: modified dataframe containing a new column for 'xxxx'
    """
    
    return df


def construct_features_and_labels(data, overwrite=True, ta_params={
    'MACD_short': 10,
    'MACD_long': 30,
    
    'SMA_short': 5,
    'SMA_mid': 15,
    'SMA_long': 30,
    
    'RSI_n': 30,
    'RSI_lb': 40,
    'RSI_ub': 60,
    
    'KDJ_n': 30,
    'KDJ_m': 3,
    
    'ABSTR_n': 30,
    
    'BB_n': 60,
    'BB_m': 2,
    
    'PVI_n': 60,
    
    'NVI_n': 60,
}):
    """
    Construct features and labels for each stock in SP5T5
    
    Features: trend, momentum, volatility, volume
    Labels: future price movement in the next 1, 2, 5, 10, 30 minutes for two threshold levels
    """

    # Add feature: Trend
    calc_indicator_macd(data, 
                        short=ta_params['MACD_short'], long=ta_params['MACD_long'], 
                        overwrite=overwrite)
    calc_indicator_sma(data, 
                        short=ta_params['SMA_short'], mid=ta_params['SMA_mid'], long=ta_params['SMA_long'], 
                        overwrite=overwrite)
    
    # Add feature: Momentum
    calc_indicator_rsi(data, 
                        n=ta_params['RSI_n'], lb=ta_params['RSI_lb'], ub=ta_params['RSI_ub'], 
                        overwrite=overwrite)
    calc_indicator_kdj(data, 
                        n=ta_params['KDJ_n'], m=ta_params['KDJ_m'], 
                        overwrite=overwrite)
    
    # Add feature: Volatility
    calc_indicator_abstr(data, 
                            n=ta_params['ABSTR_n'], 
                            overwrite=overwrite)
    calc_indicator_bb(data, 
                        n=ta_params['BB_n'], m=ta_params['BB_m'], 
                        overwrite=overwrite)
    
    # Add feature: Volume
    calc_indicator_pvi(data, n=ta_params['PVI_n'], overwrite=overwrite)
    calc_indicator_nvi(data, n=ta_params['NVI_n'], overwrite=overwrite)
    
    
    return data
