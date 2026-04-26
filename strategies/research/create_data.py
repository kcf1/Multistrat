
import numpy as np
import pandas as pd
import optuna

import sklearn.datasets
import sklearn.metrics
from sklearn.model_selection import train_test_split
import xgboost as xgb
import sqlalchemy
from dotenv import load_dotenv
import os
from sklearn.decomposition import PCA
from datetime import timedelta,datetime


def combine_features(X,rescale=True):
    """
    Rescale features to have similar scale
    Combine features to form a single signal
    Rescale the combined signal to have the same scale as the original features
    """
    if rescale:
        deflator = [x.abs().expanding(20).mean() for x in X]
        rescaled = [x/d for x,d in zip(X,deflator)]
    else:
        deflator = [1 for _ in X]
        rescaled = [x for x in X]
    
    signal = 0
    scaler = 0
    for x,d in zip(rescaled,deflator):
        signal += x
        scaler += d

    combined = (signal * scaler) / len(X)
    return combined

def __combine_features(X,rescale=True):
    idx = X[0].index
    X = pd.DataFrame({i: X[i] for i in range(len(X))})
    X = X.ffill().dropna()
    if len(X) > 0:
        tr = X.iloc[:len(X)//2]
        pca = PCA(n_components=2)
        pca.fit(tr)
        X['pc1'] = pca.transform(X)[:,1]
        X = X.reindex(idx)
    else:
        X = X.reindex(idx)
        X['pc1'] = np.nan
    return X['pc1']
    
def get_mom_score(x):
    mom10m5 = (x['log_return'].ewm(span=10).sum() - x['log_return'].ewm(span=5).sum())/5
    mom20m10 = (x['log_return'].ewm(span=20).sum() - x['log_return'].ewm(span=10).sum())/10
    mom40m20 = (x['log_return'].ewm(span=40).sum() - x['log_return'].ewm(span=20).sum())/20

    signal = combine_features([mom10m5,mom20m10,mom40m20],rescale=True)
    return signal

def get_resrev_score(x):
    resrev5 = x['resid_return'].ewm(span=5).sum()/5
    resrev10 = x['resid_return'].ewm(span=10).sum()/10
    resrev20 = x['resid_return'].ewm(span=20).sum()/20

    signal = combine_features([resrev5,resrev10,resrev20],rescale=True)
    return signal

def get_trend_score(x):
    trend5m20 = (x['close'].ewm(span=5).mean() - x['close'].ewm(span=20).mean())/x['close'].diff().ewm(span=20).std()
    trend10m40 = (x['close'].ewm(span=10).mean() - x['close'].ewm(span=40).mean())/x['close'].diff().ewm(span=20).std()
    trend20m80 = (x['close'].ewm(span=20).mean() - x['close'].ewm(span=80).mean())/x['close'].diff().ewm(span=20).std()

    signal = combine_features([trend5m20,trend10m40,trend20m80],rescale=True)
    return signal

def get_breakout_score(x):
    breakout10 = ((x['close'] - (x['close'].rolling(10).max() - x['close'].rolling(10).min())/2) / (x['close'].rolling(10).max() - x['close'].rolling(10).min())).ewm(span=5).mean()
    breakout20 = ((x['close'] - (x['close'].rolling(20).max() - x['close'].rolling(20).min())/2) / (x['close'].rolling(20).max() - x['close'].rolling(20).min())).ewm(span=5).mean()
    breakout40 = ((x['close'] - (x['close'].rolling(40).max() - x['close'].rolling(40).min())/2) / (x['close'].rolling(40).max() - x['close'].rolling(40).min())).ewm(span=5).mean()

    signal = combine_features([breakout10,breakout20,breakout40],rescale=True)
    return signal
    
def get_vwaprev_score(x):
    vwaprev5 = (x['close'] - x['vwap'].ewm(span=5).mean()) / x['close'].diff().ewm(span=20).std()
    vwaprev10 = (x['close'] - x['vwap'].ewm(span=10).mean()) / x['close'].diff().ewm(span=20).std() 
    vwaprev20 = (x['close'] - x['vwap'].ewm(span=20).mean()) / x['close'].diff().ewm(span=20).std()

    signal = combine_features([vwaprev5,vwaprev10,vwaprev20],rescale=True)
    return signal

def get_drawdown_score(x):
    drawdown10 = (x['close'].rolling(10).max() - x['close'])/x['close'].rolling(10).max()
    drawdown20 = (x['close'].rolling(20).max() - x['close'])/x['close'].rolling(20).max()
    drawdown40 = (x['close'].rolling(40).max() - x['close'])/x['close'].rolling(40).max()

    signal = combine_features([drawdown10,drawdown20,drawdown40],rescale=True)
    return signal

def get_ddath_score(x):
    ddath = (x['close'].cummax() - x['close'])/x['close'].cummax()

    signal = ddath
    return signal

def get_maxret_score(x):
    maxret10 = x['log_return'].rolling(10).max()
    maxret20 = x['log_return'].rolling(20).max()
    maxret40 = x['log_return'].rolling(40).max()

    signal = combine_features([maxret10,maxret20,maxret40],rescale=True)
    return signal

def get_skew_score(x):
    skew10 = x['log_return'].rolling(10).skew()
    skew20 = x['log_return'].rolling(20).skew()
    skew40 = x['log_return'].rolling(40).skew()

    signal = combine_features([skew10,skew20,skew40],rescale=True)
    return signal

def get_vol_score(x):
    vol10 = x['log_return'].ewm(span=10).std()
    vol20 = x['log_return'].ewm(span=20).std()
    vol40 = x['log_return'].ewm(span=40).std()

    signal = combine_features([vol10,vol20,vol40],rescale=True)
    return signal

def get_relvol_score(x):
    relvol10 = x['log_return'].ewm(span=10).std() - x['log_return'].ewm(span=20).std()
    relvol20 = x['log_return'].ewm(span=20).std() - x['log_return'].ewm(span=40).std()
    relvol40 = x['log_return'].ewm(span=40).std() - x['log_return'].ewm(span=80).std()

    signal = combine_features([relvol10,relvol20,relvol40],rescale=True)
    return signal

def get_volvol_score(x):
    volvol10 = x['log_return'].ewm(span=10).std().ewm(span=20).std()
    volvol20 = x['log_return'].ewm(span=20).std().ewm(span=40).std()
    volvol40 = x['log_return'].ewm(span=40).std().ewm(span=80).std()

    signal = combine_features([volvol10,volvol20,volvol40],rescale=True)
    return signal

def get_volume_score(x):
    volume10 = x['log_volume'].ewm(span=10).mean()
    volume20 = x['log_volume'].ewm(span=20).mean()
    volume40 = x['log_volume'].ewm(span=40).mean() 

    signal = combine_features([volume10,volume20,volume40],rescale=True)
    return signal

def get_quotevol_score(x):
    quotevol10 = x['log_quote_volume'].ewm(span=10).mean()
    quotevol20 = x['log_quote_volume'].ewm(span=20).mean()
    quotevol40 = x['log_quote_volume'].ewm(span=40).mean() 

    signal = combine_features([quotevol10,quotevol20,quotevol40],rescale=True)
    return signal

def get_takervol_score(x):
    taker10 = x['taker_buy_base_volume'].ewm(span=10).mean()
    taker20 = x['taker_buy_base_volume'].ewm(span=20).mean()
    taker40 = x['taker_buy_base_volume'].ewm(span=40).mean()

    signal = combine_features([taker10,taker20,taker40],rescale=True)
    return signal

def get_takerratio_score(x):
    takerratio10 = (x['taker_buy_base_volume'] / x['volume']).ewm(span=10).mean()
    takerratio20 = (x['taker_buy_base_volume'] / x['volume']).ewm(span=20).mean()
    takerratio40 = (x['taker_buy_base_volume'] / x['volume']).ewm(span=40).mean()

    signal = combine_features([takerratio10,takerratio20,takerratio40],rescale=True)
    return signal

def get_retvolcor_score(x):
    retvolcor10 = x['log_return'].rolling(10).corr(x['log_volume'])
    retvolcor20 = x['log_return'].rolling(20).corr(x['log_volume'])
    retvolcor40 = x['log_return'].rolling(40).corr(x['log_volume'])

    signal = combine_features([retvolcor10,retvolcor20,retvolcor40],rescale=True)
    return signal

def get_revbetasq_score(x):
    revbetasq10 = (x['revbeta']**2).ewm(span=10).mean()
    revbetasq20 = (x['revbeta']**2).ewm(span=20).mean()
    revbetasq40 = (x['revbeta']**2).ewm(span=40).mean()

    signal = combine_features([revbetasq10,revbetasq20,revbetasq40],rescale=True)
    return signal

def get_betasq_score(x):
    betasq10 = (x['beta']**2).ewm(span=10).mean()
    betasq20 = (x['beta']**2).ewm(span=20).mean()
    betasq40 = (x['beta']**2).ewm(span=40).mean()

    signal = combine_features([betasq10,betasq20,betasq40],rescale=True)
    return signal

def load_data():
    # Load environment variables from .env
    load_dotenv()

    POSTGRES_HOST = "192.168.1.249"
    POSTGRES_PORT = "5432"
    POSTGRES_DB = os.getenv("POSTGRES_DB", "your_database")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "your_username")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "your_password")

    POSTGRES_URI = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

    # Create a SQLAlchemy engine using the POSTGRES_URI
    engine = sqlalchemy.create_engine(POSTGRES_URI)
    ohlcv_df = pd.read_sql("SELECT * FROM market_data.ohlcv", engine)
    ohlcv_df['ts'] = pd.to_datetime(ohlcv_df['open_time'])

    df = ohlcv_df.copy()
    #df['close_time'] = df.groupby('symbol',group_keys=False)['open_time'].shift(0)
    df['date'] = df['open_time'].dt.date
    df = df.groupby(['date','symbol']).agg({
        'close':'last',
        'volume':'sum',
        'quote_volume':'sum',
        'taker_buy_base_volume':'sum',
        'taker_buy_quote_volume':'sum'
    }).reset_index()
    df['ts'] = pd.to_datetime(df['date'])
    df = df[['symbol','ts','close','volume','quote_volume','taker_buy_base_volume','taker_buy_quote_volume']]

    df['log_close'] = np.log(df['close'])
    df['log_return'] = df.groupby('symbol')['log_close'].diff()
    df['fwd_return'] = df.groupby('symbol')['log_return'].shift(-1)
    df['log_vol'] = df.groupby('symbol',group_keys=False).apply(lambda x: x['log_return'].ewm(span=20, adjust=False).std())
    df['norm_return'] = df['log_return'] * (.90/(250)**0.5) / df['log_vol']
    df['norm_close'] = df.groupby('symbol')['norm_return'].cumsum()
    df['vol_weight'] = (.90/(250)**0.5) / df['log_vol']
    df['vol_weight_fwd'] = df.groupby('symbol')['vol_weight'].shift(-1)
    df['vol_weighted_return'] = df['fwd_return'] * df['vol_weight']
    df['vol_weighted_return_fwd'] = df['fwd_return'] * df['vol_weight_fwd']
    df['vol_weighted_return_rank'] = df.groupby(['ts'])['vol_weighted_return'].rank(pct=True)
    df['log_volume'] = np.log(df['volume'])
    df['log_quote_volume'] = np.log(df['quote_volume'])
    df['vwap'] = (
        df.groupby(['symbol'],group_keys=False)['quote_volume'].apply(lambda x: x.rolling(250).sum())
        /
        df.groupby(['symbol'],group_keys=False)['volume'].apply(lambda x: x.rolling(250).sum())
    )

    MEGA_TOP_4 = [
        "BTCUSDT",
        "ETHUSDT",
        "BNBUSDT",
        "XRPUSDT"
    ]
    mkt_df = df[df['symbol'].isin(MEGA_TOP_4)]
    mkt_idx = mkt_df.groupby('ts')['norm_return'].mean()
    mkt_idx.name = 'return'
    mkt_idx = pd.DataFrame(mkt_idx)
    mkt_idx['vol'] = mkt_idx['return'].ewm(span=20, adjust=False).std()
    mkt_idx = mkt_idx.add_prefix('mkt_')
    mkt_idx = mkt_idx.reset_index()
    mkt_idx = mkt_idx.dropna()

    df = df.merge(mkt_idx, on='ts', how='left')
    df['revbeta'] = df.groupby('symbol',group_keys=False).apply(lambda x: x['norm_return'].rolling(250).cov(x['mkt_return'])/x['norm_return'].rolling(250).var())
    df['beta'] = df.groupby('symbol',group_keys=False).apply(lambda x: x['norm_return'].rolling(250).cov(x['mkt_return'])/x['mkt_return'].rolling(250).var())
    df['resid_return'] = df['norm_return'] - df['beta'] * df['mkt_return']

    df['mom_score'] = df.groupby('symbol',group_keys=False).apply(get_mom_score)
    df['trend_score'] = df.groupby('symbol',group_keys=False).apply(get_trend_score)
    df['breakout_score'] = df.groupby('symbol',group_keys=False).apply(get_breakout_score)
    df['vwaprev_score'] = df.groupby('symbol',group_keys=False).apply(get_vwaprev_score)
    df['takerratio_score'] = df.groupby('symbol',group_keys=False).apply(get_takerratio_score)
    df['revbetasq_score'] = df.groupby('symbol',group_keys=False).apply(get_revbetasq_score)
    df['drawdown_score'] = df.groupby('symbol',group_keys=False).apply(get_drawdown_score)
    df['ddath_score'] = df.groupby('symbol',group_keys=False).apply(get_ddath_score)
    df['maxret_score'] = df.groupby('symbol',group_keys=False).apply(get_maxret_score)
    df['skew_score'] = df.groupby('symbol',group_keys=False).apply(get_skew_score)
    df['vol_score'] = df.groupby('symbol',group_keys=False).apply(get_vol_score)
    df['volume_score'] = df.groupby('symbol',group_keys=False).apply(get_volume_score)
    df['quotevol_score'] = df.groupby('symbol',group_keys=False).apply(get_quotevol_score)
    df['retvolcor_score'] = df.groupby('symbol',group_keys=False).apply(get_retvolcor_score)

    df['betasq_score'] = df.groupby('symbol',group_keys=False).apply(get_betasq_score)
    df['revbetasq_score'] = df.groupby('symbol',group_keys=False).apply(get_revbetasq_score)
    df['resrev_score'] = df.groupby('symbol',group_keys=False).apply(get_resrev_score)

    df = df.dropna()

    df['quotevol_rank'] = df.groupby(['ts'])['quotevol_score'].rank(pct=True)
    df['volume_rank'] = df.groupby(['ts'])['volume_score'].rank(pct=True)
    df['volume_bin'] = (df['quotevol_rank'] * 3).astype(int).clip(0,2)

    df['mom_rank'] = df.groupby(['ts','volume_bin'])['mom_score'].rank(pct=True)
    df['trend_rank'] = df.groupby(['ts','volume_bin'])['trend_score'].rank(pct=True)
    df['breakout_rank'] = df.groupby(['ts','volume_bin'])['breakout_score'].rank(pct=True)
    df['vwaprev_rank'] = df.groupby(['ts','volume_bin'])['vwaprev_score'].rank(pct=True)
    df['takerratio_rank'] = df.groupby(['ts','volume_bin'])['takerratio_score'].rank(pct=True)

    df['revbetasq_rank'] = df.groupby(['ts','volume_bin'])['revbetasq_score'].rank(pct=True)
    df['drawdown_rank'] = df.groupby(['ts','volume_bin'])['drawdown_score'].rank(pct=True)
    df['ddath_rank'] = df.groupby(['ts','volume_bin'])['ddath_score'].rank(pct=True)
    df['maxret_rank'] = df.groupby(['ts','volume_bin'])['maxret_score'].rank(pct=True)
    df['skew_rank'] = df.groupby(['ts','volume_bin'])['skew_score'].rank(pct=True)
    df['vol_rank'] = df.groupby(['ts','volume_bin'])['vol_score'].rank(pct=True)
    df['retvolcor_rank'] = df.groupby(['ts','volume_bin'])['retvolcor_score'].rank(pct=True)

    df['betasq_rank'] = df.groupby(['ts','volume_bin'])['betasq_score'].rank(pct=True)
    df['resrev_rank'] = df.groupby(['ts','volume_bin'])['resrev_score'].rank(pct=True)
    df['revbetasq_rank'] = df.groupby(['ts','volume_bin'])['revbetasq_score'].rank(pct=True)

    hi = df['ts'].max()
    lo = df['ts'].min()
    train_end = lo + (hi - lo) * 0.6
    valid_end = lo + (hi - lo) * 0.8

    train_df = df[df['ts'] < train_end]
    valid_df = df[(df['ts'] >= train_end) & (df['ts'] < valid_end)]
    test_df = df[df['ts'] >= valid_end]

    x_cols = ['mom_rank','trend_rank','breakout_rank','vwaprev_rank','resrev_rank','takerratio_rank','vol_rank','betasq_rank','volume_rank','quotevol_rank','maxret_rank','skew_rank']
    y_cols = ['ts','symbol','vol_weight','vol_weighted_return','fwd_return']

    train_x,train_y = train_df[x_cols],train_df[y_cols]
    valid_x,valid_y = valid_df[x_cols],valid_df[y_cols]
    test_x,test_y = test_df[x_cols],test_df[y_cols]

    return train_x, train_y, valid_x, valid_y, test_x, test_y