import copy
import pickle
import time

from chanlun import fun
from chanlun.backtesting.base import (
    Strategy,
    Operation,
    POSITION,
    MarketDatas,
    Trader,
)
from chanlun.cl_interface import *
from chanlun.file_db import fdb
from chanlun.db import db


class BackTestTrader(Trader):
    """
    回测交易（可继承支持实盘）
    """

    def __init__(
        self,
        name,
        mode="signal",
        is_stock=True,
        is_futures=False,
        init_balance=100000,
        fee_rate=0.0005,
        max_pos=10,
        log=None,
    ):
        """
        交易者初始化
        :param name: 交易者名称
        :param mode: 执行模式 signal 测试信号模式，固定金额开仓；trade 实际买卖模式；real 线上实盘交易
        :param is_stock: 是否是股票交易（决定当日是否可以卖出）
        :param is_futures: 是否是期货交易（决定是否可以做空）
        :param init_balance: 初始资金
        :param fee_rate: 手续费比例
        """

        # 策略基本信息
        self.name = name
        self.mode = mode
        self.is_stock = is_stock
        self.is_futures = is_futures
        self.allow_mmds = None

        # 资金情况
        self.balance = init_balance if mode == "trade" else 0
        self.fee_rate = fee_rate
        self.fee_total = 0
        self.max_pos = max_pos

        # 是否打印日志
        self.log = log
        self.log_history = []

        # 时间统计
        self.use_times = {
            "strategy_close": 0,
            "strategy_open": 0,
            "execute": 0,
            "position_record": 0,
        }

        # 策略对象
        self.strategy: Strategy = None

        # 回测数据对象
        self.datas: MarketDatas = None

        # 当前持仓信息
        self.positions: Dict[str, POSITION] = {}
        self.positions_history: Dict[str, List[POSITION]] = {}
        # 持仓资金历史
        self.positions_balance_history: Dict[str, Dict[str, float]] = {}
        # 持仓盈亏记录
        self.hold_profit_history = {}
        # 资产历史
        self.balance_history: Dict[str, float] = {}

        # 代码订单信息
        self.orders = {}

        # 统计结果数据
        self.results = {
            "1buy": {"win_num": 0, "loss_num": 0, "win_balance": 0, "loss_balance": 0},
            "2buy": {"win_num": 0, "loss_num": 0, "win_balance": 0, "loss_balance": 0},
            "l2buy": {"win_num": 0, "loss_num": 0, "win_balance": 0, "loss_balance": 0},
            "3buy": {"win_num": 0, "loss_num": 0, "win_balance": 0, "loss_balance": 0},
            "l3buy": {"win_num": 0, "loss_num": 0, "win_balance": 0, "loss_balance": 0},
            "down_bi_bc_buy": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "down_xd_bc_buy": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "down_pz_bc_buy": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "down_qs_bc_buy": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "1sell": {"win_num": 0, "loss_num": 0, "win_balance": 0, "loss_balance": 0},
            "2sell": {"win_num": 0, "loss_num": 0, "win_balance": 0, "loss_balance": 0},
            "l2sell": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "3sell": {"win_num": 0, "loss_num": 0, "win_balance": 0, "loss_balance": 0},
            "l3sell": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "up_bi_bc_sell": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "up_xd_bc_sell": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "up_pz_bc_sell": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
            "up_qs_bc_sell": {
                "win_num": 0,
                "loss_num": 0,
                "win_balance": 0,
                "loss_balance": 0,
            },
        }

        # 记录持仓盈亏、资金历史的格式化日期形式
        self.record_dt_format = "%Y-%m-%d %H:%M:%S"

        # 在执行策略前，手动指定执行的开始时间
        self.begin_run_dt: datetime.datetime = None

        # 缓冲区的执行操作，用于在特定时间点批量进行开盘检测后，对要执行的开盘信号再次进行过滤筛选
        self.buffer_opts: List[Operation] = []

    def add_times(self, key, ts):
        if key not in self.use_times.keys():
            self.use_times[key] = 0

        self.use_times[key] += ts
        return True

    def set_strategy(self, _strategy: Strategy):
        """
        设置策略对象
        :param _strategy:
        :return:
        """
        self.strategy = _strategy

    def set_data(self, _data: MarketDatas):
        """
        设置数据对象
        """
        self.datas = _data

    def save_to_pkl(self, key: str):
        """
        将对象数据保存到 pkl 文件中
        """
        save_infos = {
            "name": self.name,
            "mode": self.mode,
            "is_stock": self.is_stock,
            "is_futures": self.is_futures,
            "allow_mmds": self.allow_mmds,
            "balance": self.balance,
            "fee_rate": self.fee_rate,
            "fee_total": self.fee_total,
            "max_pos": self.max_pos,
            "positions": self.positions,
            "positions_history": self.positions_history,
            "positions_balance_history": self.positions_balance_history,
            "hold_profit_history": self.hold_profit_history,
            "balance_history": self.balance_history,
            "orders": self.orders,
            "results": self.results,
        }
        if key is not None:
            fdb.cache_pkl_to_file(key, save_infos)
        return save_infos

    def load_from_pkl(self, key: str, save_infos: dict = None):
        """
        从 pkl 文件 中恢复之前的数据
        """
        if save_infos is None:
            save_infos = fdb.cache_pkl_from_file(key)
            if save_infos is None:
                return False
        self.name = save_infos["name"]
        self.mode = save_infos["mode"]
        self.is_stock = save_infos["is_stock"]
        self.is_futures = save_infos["is_stock"]
        self.allow_mmds = save_infos["allow_mmds"]
        self.balance = save_infos["balance"]
        self.fee_rate = save_infos["fee_rate"]
        self.fee_total = save_infos["fee_total"]
        self.max_pos = save_infos["max_pos"]
        self.results = save_infos["results"]

        self.positions = save_infos["positions"]
        self.positions_history = save_infos["positions_history"]
        self.positions_balance_history = save_infos["positions_balance_history"]
        self.hold_profit_history = save_infos["hold_profit_history"]
        self.balance_history = save_infos["balance_history"]
        self.orders = save_infos["orders"]

        return True

    def get_price(self, code):
        """
        回测中方法，获取股票代码当前的价格，根据最小周期 k 线收盘价
        """
        price_info = self.datas.last_k_info(code)
        return price_info

    def get_now_datetime(self):
        """
        获取当前时间
        """
        if self.mode != "signal":
            return datetime.datetime.now()
        # 回测时用回测的当前时间
        return self.datas.now_date

    # 运行的唯一入口
    def run(self, code, is_filter=False):
        # 如果设置开始执行时间，并且当前时间小于等于设置的时间，则不执行策略
        if (
            self.begin_run_dt is not None
            and self.begin_run_dt >= self.get_now_datetime()
        ):
            return True

        # 优先检查持仓情况
        for _open_uid, pos in self.positions.items():
            if pos.code != code or pos.balance == 0:
                continue
            _time = time.time()
            opts = self.strategy.close(
                code=code, mmd=pos.mmd, pos=pos, market_data=self.datas
            )
            self.add_times("strategy_close", time.time() - _time)

            if opts is False or opts is None:
                continue
            if isinstance(opts, Operation):
                opts.code = code
                opts = [opts]
            for _opt in opts:
                _opt.code = code
                if self.mode != "signal" and self.strategy.allow_close_uid is not None:
                    if _opt.close_uid not in self.strategy.allow_close_uid:
                        continue
                self.execute(code, _opt, pos)

        # 再执行检查机会方法
        poss = [
            _p
            for _uid, _p in self.positions.items()
            if _p.code == code and _p.balance != 0
        ]  # 只获取有持仓的记录

        _time = time.time()
        opts = self.strategy.open(code=code, market_data=self.datas, poss=poss)
        self.add_times("strategy_open", time.time() - _time)

        for opt in opts:
            opt.code = code
            if is_filter:
                # 如果是过滤模式，将操作记录到缓冲区，等待批量执行
                self.buffer_opts.append(opt)
            else:
                self.execute(code, opt, None)

        # 只保留有资金的持仓
        self.positions = {
            _uid: _p for _uid, _p in self.positions.items() if _p.balance != 0
        }

        return True

    def run_buffer_opts(self):
        """
        执行缓冲区的操作
        """
        for opt in self.buffer_opts:
            self.execute(opt.code, opt, None)
        self.buffer_opts = []

    # 运行结束，统一清仓
    def end(self):
        for _uid, pos in self.positions.items():
            if pos.balance > 0:
                self.execute(
                    pos.code,
                    Operation(
                        opt="sell",
                        mmd=pos.mmd,
                        msg="退出",
                        code=pos.code,
                        close_uid="clear",
                    ),
                    pos,
                )
        return True

    def update_position_record(self):
        """
        更新所有持仓的盈亏情况
        """
        record_dt = self.get_now_datetime().strftime(self.record_dt_format)

        total_hold_profit = 0
        total_hold_balance = 0
        for _uid, pos in self.positions.items():
            if pos.balance == 0:
                continue
            now_profit, hold_balance = self.position_record(pos)
            total_hold_profit += now_profit
            total_hold_balance += hold_balance

        # 记录时间下的总持仓盈亏
        self.hold_profit_history[record_dt] = total_hold_profit

        # 记录当前的持仓金额
        position_balance = {}
        for _uid, pos in self.positions.items():
            if pos.balance == 0:
                continue
            code_price = self.get_price(pos.code)
            if "buy" in pos.mmd:
                code_balance = pos.amount * code_price["close"]
            else:
                code_balance = -(pos.amount * code_price["close"])
            if pos.code not in position_balance.keys():
                position_balance[pos.code] = 0
            position_balance[pos.code] += code_balance
        position_balance["Cash"] = self.balance

        self.balance_history[record_dt] = (
            total_hold_profit + total_hold_balance + self.balance
        )
        self.positions_balance_history[record_dt] = position_balance

        return None

    def position_record(self, pos: POSITION) -> Tuple[float, float]:
        """
        持仓记录更新
        :param pos:
        :return: 返回持仓的总金额（包括持仓盈亏）
        """
        s_time = time.time()

        hold_balance = 0
        now_profit = 0
        price_info = self.get_price(pos.code)
        if pos.type == "做多":
            high_profit_rate = round(
                (
                    (price_info["high"] - pos.price)
                    / pos.price
                    * (pos.balance * pos.now_pos_rate)
                    + pos.profit
                )
                / pos.balance
                * 100,
                4,
            )
            low_profit_rate = round(
                (
                    (price_info["low"] - pos.price)
                    / pos.price
                    * (pos.balance * pos.now_pos_rate)
                    + pos.profit
                )
                / pos.balance
                * 100,
                4,
            )
            pos.max_profit_rate = max(pos.max_profit_rate, high_profit_rate)
            pos.max_loss_rate = min(pos.max_loss_rate, low_profit_rate)

            pos.profit_rate = round(
                (
                    (price_info["close"] - pos.price)
                    / pos.price
                    * (pos.balance * pos.now_pos_rate)
                    + pos.profit
                )
                / pos.balance
                * 100,
                4,
            )
            now_profit += pos.profit_rate / 100 * pos.balance
        if pos.type == "做空":
            high_profit_rate = round(
                (
                    (pos.price - price_info["low"])
                    / pos.price
                    * (pos.balance * pos.now_pos_rate)
                    + pos.profit
                )
                / pos.balance
                * 100,
                4,
            )
            low_profit_rate = round(
                (
                    (pos.price - price_info["high"])
                    / pos.price
                    * (pos.balance * pos.now_pos_rate)
                    + pos.profit
                )
                / pos.balance
                * 100,
                4,
            )
            pos.max_profit_rate = max(pos.max_profit_rate, high_profit_rate)
            pos.max_loss_rate = min(pos.max_loss_rate, low_profit_rate)

            pos.profit_rate = round(
                (
                    (pos.price - price_info["close"])
                    / pos.price
                    * (pos.balance * pos.now_pos_rate)
                    + pos.profit
                )
                / pos.balance
                * 100,
                4,
            )
            now_profit += pos.profit_rate / 100 * pos.balance

        hold_balance += pos.balance * pos.now_pos_rate

        self.add_times("position_record", time.time() - s_time)
        return now_profit, hold_balance

    def position_codes(self):
        """
        获取当前持仓中的股票代码
        """
        codes = list(
            set([_p.code for _uid, _p in self.positions.items() if _p.balance != 0])
        )
        return codes

    def hold_positions(self) -> List[POSITION]:
        """
        返回所有持仓记录
        """
        return [_p for _uid, _p in self.positions.items() if _p.balance != 0]

    # 做多买入
    def open_buy(self, code, opt: Operation, amount: float = None):
        if self.mode == "signal":
            use_balance = 100000 * min(1.0, opt.pos_rate)
            price = self.get_price(code)["close"]
            amount = round((use_balance / price) * 0.99, 4)
            return {"price": price, "amount": amount}
        else:
            if len(self.hold_positions()) >= self.max_pos:
                return False
            price = self.get_price(code)["close"]

            if amount is None:
                use_balance = (
                    self.balance / (self.max_pos - len(self.hold_positions()))
                ) * 0.99
                use_balance *= min(1.0, opt.pos_rate)
                amount = use_balance / price
            else:
                use_balance = price * amount

            if amount < 0:
                return False
            if use_balance > self.balance:
                self._print_log("%s - %s 做多开仓 资金余额不足" % (code, opt.mmd))
                return False

            fee = use_balance * self.fee_rate
            self.balance -= use_balance + fee
            self.fee_total += fee

            return {"price": price, "amount": amount}

    # 做空卖出
    def open_sell(self, code, opt: Operation, amount: float = None):
        if self.mode == "signal":
            use_balance = 100000 * min(1.0, opt.pos_rate)
            price = self.get_price(code)["close"]
            amount = round((use_balance / price) * 0.99, 4)
            return {"price": price, "amount": amount}
        else:
            if len(self.hold_positions()) >= self.max_pos:
                return False
            price = self.get_price(code)["close"]

            if amount is None:
                use_balance = (
                    self.balance / (self.max_pos - len(self.hold_positions()))
                ) * 0.99
                use_balance *= min(1.0, opt.pos_rate)
                amount = use_balance / price
            else:
                use_balance = price * amount

            if amount < 0:
                return False

            if use_balance > self.balance:
                self._print_log("%s - %s 做空开仓 资金余额不足" % (code, opt.mmd))
                return False

            fee = use_balance * self.fee_rate
            self.balance -= use_balance + fee
            self.fee_total += fee

            return {"price": price, "amount": amount}

    # 做多平仓
    def close_buy(self, code, pos: POSITION, opt: Operation):
        # 如果操作中设置了止损价格，则按照止损价格执行，否则按照最新价格执行
        if opt.loss_price != 0:
            price = opt.loss_price
        else:
            price = self.get_price(code)["close"]

        amount = pos.amount * opt.pos_rate

        if self.mode == "signal":
            net_profit = (price * amount) - (pos.price * amount)
            self.balance += net_profit
            return {"price": price, "amount": amount}
        else:
            hold_balance = price * amount
            fee = hold_balance * self.fee_rate
            self.balance += hold_balance - fee
            self.fee_total += fee
            return {"price": price, "amount": amount}

    # 做空平仓
    def close_sell(self, code, pos: POSITION, opt: Operation):
        # 如果操作中设置了止损价格，则按照止损价格执行，否则按照最新价格执行
        if opt.loss_price != 0:
            price = opt.loss_price
        else:
            price = self.get_price(code)["close"]

        amount = pos.amount * opt.pos_rate

        if self.mode == "signal":
            net_profit = (pos.price * amount) - (price * amount)
            self.balance += net_profit
            return {"price": price, "amount": amount}
        else:
            hold_balance = price * amount
            pos_balance = pos.price * amount
            profit = pos_balance - hold_balance
            fee = hold_balance * self.fee_rate
            self.balance += pos_balance + profit - fee
            self.fee_total += fee

            return {"price": price, "amount": amount}

    # 打印日志信息
    def _print_log(self, msg):
        self.log_history.append(msg)
        if self.log:
            self.log(msg)
        return

    # 执行操作
    def execute(self, code, opt: Operation, pos: POSITION = None):
        _time = time.time()
        try:
            # 如果是交易模式，将 close_uid 都修改为 clear ，使用 strategy 类中的 allow_close_uid 进行控制
            if self.mode != "signal":
                opt.close_uid = "clear"

            if pos is not None:
                if pos.balance == 0.0 or pos.now_pos_rate == 0.0:
                    return True

            opt_mmd = opt.mmd
            # 检查是否在允许做的买卖点上
            if self.allow_mmds is not None and opt_mmd not in self.allow_mmds:
                return True

            if opt.opt == "buy":
                # 检查当前是否有改持仓，如果持仓存在的话，则不进行操作
                if opt.open_uid in self.positions.keys():
                    pos = self.positions[opt.open_uid]
                    if pos.now_pos_rate >= 1:
                        return True
                else:
                    pos = POSITION(code=code, mmd=opt.mmd, open_uid=opt.open_uid)
                    self.positions[opt.open_uid] = pos

            res = None
            order_type = None

            # 买点，买入，开仓做多
            if "buy" in opt_mmd and opt.opt == "buy":
                # 开仓后，不同位置分仓买入的key
                if opt.key in pos.open_keys.keys():
                    return False
                # 修正错误的开仓比例
                opt.pos_rate = min(1.0 - pos.now_pos_rate, opt.pos_rate)

                res = self.open_buy(code, opt)
                if res is False:
                    return False

                pos.type = "做多"
                pos.price = res["price"]
                pos.amount += res["amount"]
                pos.balance += res["price"] * res["amount"]
                pos.loss_price = opt.loss_price
                pos.open_date = (
                    self.get_now_datetime().strftime("%Y-%m-%d")
                    if pos.open_date is None
                    else pos.open_date
                )
                pos.open_datetime = (
                    self.get_now_datetime()
                    if pos.open_datetime is None
                    else pos.open_datetime
                )
                pos.open_msg = opt.msg
                pos.info = opt.info
                pos.now_pos_rate += min(1.0, opt.pos_rate)
                pos.open_keys[opt.key] = opt.pos_rate

                # 本次开仓的记录
                pos.open_records.append(
                    {
                        "datetime": self.get_now_datetime(),
                        "price": res["price"],
                        "amount": res["amount"],
                        "open_msg": opt.msg,
                        "open_key": opt.key,
                        "open_uid": opt.open_uid,
                        "pos_rate": opt.pos_rate,
                    }
                )

                order_type = "open_long"

                self._print_log(
                    f"[{code} - {self.get_now_datetime()}] // {opt_mmd} 做多买入（{res['price']} - {res['amount']}），原因： {opt.msg}"
                )

            # 卖点，买入，开仓做空（期货）
            if self.is_futures and "sell" in opt_mmd and opt.opt == "buy":
                # 唯一key判断
                if opt.key in pos.open_keys.keys():
                    return False
                # 修正错误的开仓比例
                opt.pos_rate = min(1.0 - pos.now_pos_rate, opt.pos_rate)

                res = self.open_sell(code, opt)
                if res is False:
                    return False
                pos.type = "做空"
                pos.price = res["price"]
                pos.amount += res["amount"]
                pos.balance += res["price"] * res["amount"]
                pos.loss_price = opt.loss_price
                pos.open_date = (
                    self.get_now_datetime().strftime("%Y-%m-%d")
                    if pos.open_date is None
                    else pos.open_date
                )
                pos.open_datetime = (
                    self.get_now_datetime()
                    if pos.open_datetime is None
                    else pos.open_datetime
                )
                pos.open_msg = opt.msg
                pos.info = opt.info
                pos.now_pos_rate += min(1.0, opt.pos_rate)
                pos.open_keys[opt.key] = opt.pos_rate

                # 本次开仓的记录
                pos.open_records.append(
                    {
                        "datetime": self.get_now_datetime(),
                        "price": res["price"],
                        "amount": res["amount"],
                        "open_msg": opt.msg,
                        "open_key": opt.key,
                        "open_uid": opt.open_uid,
                        "pos_rate": opt.pos_rate,
                    }
                )

                order_type = "open_short"

                self._print_log(
                    f"[{code} - {self.get_now_datetime()}] // {opt_mmd} 做空卖出（{res['price']} - {res['amount']}），原因： {opt.msg}"
                )

            # 卖点，卖出，平仓做空（期货）
            if self.is_futures and "sell" in opt_mmd and opt.opt == "sell":
                # 判断当前是否有仓位
                if pos.now_pos_rate <= 0:
                    return False
                # 唯一key判断
                if opt.key in pos.close_keys.keys():
                    return False
                if opt.close_uid in pos.close_uid_profit.keys():
                    return False
                # 修正错误的平仓比例
                opt.pos_rate = (
                    pos.now_pos_rate
                    if pos.now_pos_rate < opt.pos_rate
                    else opt.pos_rate
                )

                if self.is_stock and pos.open_date == self.get_now_datetime().strftime(
                    "%Y-%m-%d"
                ):
                    # 股票交易，当日不能卖出
                    return False

                res = self.close_sell(code, pos, opt)
                if res is False:
                    return False

                sell_balance = res["price"] * res["amount"]
                hold_balance = pos.balance * opt.pos_rate

                # 做空收益：持仓金额 减去 卖出金额的 差价 - 手续费（双边手续费）
                fee_use = sell_balance * self.fee_rate * 2
                profit = hold_balance - sell_balance - fee_use
                profit_rate = round((profit / hold_balance) * 100, 2)

                self._print_log(
                    "[%s - %s] // %s 平仓做空（%s - %s） 盈亏：%s (%.2f%%)，原因： %s"
                    % (
                        code,
                        self.get_now_datetime(),
                        opt_mmd,
                        res["price"],
                        res["amount"],
                        profit,
                        profit_rate,
                        opt.msg,
                    )
                )
                # 本次平仓的记录
                pos.close_records.append(
                    {
                        "datetime": self.get_now_datetime(),
                        "price": res["price"],
                        "amount": res["amount"],
                        "close_msg": opt.msg,
                        "close_key": opt.key,
                        "close_uid": opt.close_uid,
                        "pos_rate": opt.pos_rate,
                    }
                )
                pos.close_uid_profit[opt.close_uid] = {
                    "close_datetime": self.get_now_datetime(),
                    "profit_rate": profit_rate,
                    "profit": profit,
                    "max_profit_rate": pos.max_profit_rate,
                    "max_loss_rate": pos.max_loss_rate,
                    "close_msg": opt.msg,
                }

                # 平仓的uid不是 clear，不进行实质性的平仓，只记录当前的盈亏情况
                if opt.close_uid == "clear":
                    self.fee_total += fee_use
                    pos.profit += profit
                    pos.now_pos_rate -= opt.pos_rate
                    pos.close_keys[opt.key] = opt.pos_rate
                    if pos.now_pos_rate <= 0:
                        if pos.profit > 0:
                            # 盈利
                            self.results[opt_mmd]["win_num"] += 1
                            self.results[opt_mmd]["win_balance"] += pos.profit
                        else:
                            # 亏损
                            self.results[opt_mmd]["loss_num"] += 1
                            self.results[opt_mmd]["loss_balance"] += abs(pos.profit)

                        profit_rate = round((pos.profit / pos.balance) * 100, 2)
                        pos.profit_rate = profit_rate
                        pos.close_msg = opt.msg
                        # 将持仓添加到历史持仓，并清空当前持仓的 balance
                        if pos.code not in self.positions_history.keys():
                            self.positions_history[pos.code] = []
                        self.positions_history[pos.code].append(copy.deepcopy(pos))
                        pos.balance = 0  # 删除这个持仓

                order_type = "close_short"

            # 买点，卖出，平仓做多
            if "buy" in opt_mmd and opt.opt == "sell":
                # 唯一key判断
                if opt.key in pos.close_keys.keys():
                    return False
                if opt.close_uid in pos.close_uid_profit.keys():
                    return False
                # 修正错误的平仓比例
                opt.pos_rate = (
                    pos.now_pos_rate
                    if pos.now_pos_rate < opt.pos_rate
                    else opt.pos_rate
                )

                if self.is_stock and pos.open_date == self.get_now_datetime().strftime(
                    "%Y-%m-%d"
                ):
                    # 股票交易，当日不能卖出
                    return False

                res = self.close_buy(code, pos, opt)
                if res is False:
                    return False

                sell_balance = res["price"] * res["amount"]
                hold_balance = pos.balance * opt.pos_rate
                # 做多收益：卖出金额 减去 持有金额的 差价， - 手续费（双边手续费）
                fee_use = sell_balance * self.fee_rate * 2
                profit = sell_balance - hold_balance - fee_use
                profit_rate = round((profit / hold_balance) * 100, 2)

                self._print_log(
                    "[%s - %s] // %s 平仓做多（%s - %s） 盈亏：%s  (%.2f%%)，原因： %s"
                    % (
                        code,
                        self.get_now_datetime(),
                        opt_mmd,
                        res["price"],
                        res["amount"],
                        profit,
                        profit_rate,
                        opt.msg,
                    )
                )
                # 本次平仓的记录
                pos.close_records.append(
                    {
                        "datetime": self.get_now_datetime(),
                        "price": res["price"],
                        "amount": res["amount"],
                        "close_msg": opt.msg,
                        "close_key": opt.key,
                        "close_uid": opt.close_uid,
                        "pos_rate": opt.pos_rate,
                    }
                )
                pos.close_uid_profit[opt.close_uid] = {
                    "close_datetime": self.get_now_datetime(),
                    "profit_rate": profit_rate,
                    "profit": profit,
                    "max_profit_rate": pos.max_profit_rate,
                    "max_loss_rate": pos.max_loss_rate,
                    "close_msg": opt.msg,
                }

                # 平仓的uid不是 clear，不进行实质性的平仓，只记录当前的盈亏情况
                if opt.close_uid == "clear":
                    pos.profit += profit
                    pos.now_pos_rate -= opt.pos_rate
                    pos.close_keys[opt.key] = opt.pos_rate
                    pos.close_datetime = self.get_now_datetime()

                    self.fee_total += fee_use

                    if pos.now_pos_rate <= 0:
                        if pos.profit > 0:
                            # 盈利
                            self.results[opt_mmd]["win_num"] += 1
                            self.results[opt_mmd]["win_balance"] += pos.profit
                        else:
                            # 亏损
                            self.results[opt_mmd]["loss_num"] += 1
                            self.results[opt_mmd]["loss_balance"] += abs(pos.profit)

                        profit_rate = round((pos.profit / pos.balance) * 100, 2)
                        pos.profit_rate = profit_rate
                        pos.close_msg = opt.msg
                        # 将持仓添加到历史持仓，并清空当前持仓的 balance
                        if pos.code not in self.positions_history.keys():
                            self.positions_history[pos.code] = []
                        self.positions_history[pos.code].append(copy.deepcopy(pos))
                        pos.balance = 0  # 删除这个持仓

                order_type = "close_long"

            if res:
                # 记录订单信息
                if code not in self.orders:
                    self.orders[code] = []
                self.orders[code].append(
                    {
                        "datetime": self.get_now_datetime(),
                        "type": order_type,
                        "price": res["price"],
                        "amount": res["amount"],
                        "info": opt.msg,
                        "open_uid": opt.open_uid,
                        "close_uid": opt.close_uid,
                    }
                )
                return True

            return False
        finally:
            self.add_times("execute", time.time() - _time)

    def order_draw_tv_mark(
        self,
        market: str,
        mark_label: str,
        close_uid: List[str] = None,
        start_dt: datetime = None,
    ):
        # 先删除所有的订单
        db.marks_del(market=market, mark_label=mark_label)
        order_colors = {
            "open_long": "red",
            "open_short": "green",
            "close_long": "green",
            "close_short": "red",
        }
        order_shape = {
            "open_long": "earningUp",
            "open_short": "earningDown",
            "close_long": "earningDown",
            "close_short": "earningUp",
        }
        for _code, _orders in self.orders.items():
            # print(f"Draw Mark {_code} : {len(_orders) / 2}")
            for _o in _orders:
                if close_uid is not None:
                    if "close_" in _o["type"] and _o["close_uid"] not in close_uid:
                        continue
                if start_dt is not None:
                    if fun.datetime_to_int(_o["datetime"]) < fun.datetime_to_int(
                        start_dt
                    ):
                        continue
                db.marks_add(
                    market,
                    _code,
                    _code,
                    "",
                    fun.datetime_to_int(_o["datetime"]),
                    mark_label,
                    _o["info"],
                    order_shape[_o["type"]],
                    order_colors[_o["type"]],
                )
        print("Done")
        return True
