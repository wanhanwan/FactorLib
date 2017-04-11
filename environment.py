from data_source.mongodb import mongoDB
from data_source.trade_calendar import trade_calendar
from data_source.base_data_source import base_data_source, sector

class Environment(object):
    _env = None

    def __init__(self, config):
        Environment._env = self
        self._config = config
        self._mongoDB = mongoDB()
        self._trade_calendar = trade_calendar()
        self._sector = sector(self._mongoDB, self._trade_calendar)
        self._data_source = base_data_source(self._sector)
        self.benchmark_return = None

    @classmethod
    def get_instance(cls):
        return Environment._env
    
    def _initialize(self):
        """初始化"""
        # 加载基准的收益率
        benchmark = self._config.benchmark
        benchmark_return = self._data_source.get_fix_period_return(
            benchmark,'1d', self._config.start_date, self._config.end_date,type='index').reset_index(level=1,drop=True)
        self.benchmark_return = benchmark_return        
