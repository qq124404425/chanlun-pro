#:  -*- coding: utf-8 -*-
from chanlun.exchange.exchange_binancespot import ExchangeBinanceSpot
from chanlun.exchange.exchange_db import ExchangeDB
import traceback
from tqdm.auto import tqdm
import datetime
import time

"""
同步数字货币行情到数据库中
"""

exchange = ExchangeDB('spot')
line_exchange = ExchangeBinanceSpot()

# codes = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'LINK/USDT', 'SOL/USDT', 'OP/USDT', 'MATIC/USDT', 'ARB/USDT', 'MKR/USDT', 'WLD/USDT']
codes = ['BTC/USDT', 'ETH/USDT']
sync_frequencys = ['d', '4h', '60m', '30m', '15m', '10m', '5m', '1m']
# sync_frequencys = ['w', 'd', '4h', '60m', '30m']

# TODO 同步各个周期的起始时间
f_start_time_maps = {
    'w': '2000-01-01 00:00:00',
    'd': '2000-01-01 00:00:00',
    '4h': '2000-01-01 00:00:00',
    '60m': '2000-01-01 00:00:00',
    '30m': '2000-01-01 00:00:00',
    '15m': '2000-01-01 00:00:00',
    '10m': '2000-01-01 00:00:00',
    '5m': '2000-01-01 00:00:00',
    '1m': '2023-10-01 00:00:00',
}

for code in tqdm(codes):
    try:
        for f in sync_frequencys:
            # 先删除数据
            start_date = datetime.datetime.strptime(f_start_time_maps[f], '%Y-%m-%d %H:%M:%S')
            exchange.del_klines_by_time(code, f, start_date)
            while True:
                try:
                    last_dt = exchange.query_last_datetime(code, f)
                    if last_dt is None:
                        klines = line_exchange.klines(
                            code, f, end_date=f_start_time_maps[f], args={'use_online': True}
                        )
                        if len(klines) == 0:
                            klines = line_exchange.klines(
                                code, f, start_date=f_start_time_maps[f], args={'use_online': True}
                            )
                    else:
                        klines = line_exchange.klines(code, f, start_date=last_dt, args={'use_online': True})

                    tqdm.write('Run code %s frequency %s klines len %s' % (code, f, len(klines)))
                    exchange.insert_klines(code, f, klines)
                    if len(klines) <= 1:
                        break
                    
                    # time.sleep(1)
                except Exception as e:
                    tqdm.write('执行 %s 同步K线异常' % code)
                    tqdm.write(traceback.format_exc())
                    break

    except Exception as e:
        tqdm.write('执行 %s 同步K线异常' % code)
        tqdm.write(e)
        tqdm.write(traceback.format_exc())
