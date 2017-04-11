from .factor import factor

class panelFactor(factor):
    def __init__(self, name, axe, direction=1):
        super(panelFactor, self).__init__(name, axe)
        self.direction = direction
        self.grouping_info = {}
        self.group_return = {}
        self.long_short_return = None
        self.group_return_fig = None
        self.first_group_active_return = {}
        self.ic_series = None
        self.ic_fig = None

    # ------------------------数据操作------------------------------
    def drop_nan(self,inplace=False):
        return self.operator.drop_nan(self,inplace)

    def drop_untradable_stocks(self,inplace=False):
        return self.operator.drop_untradable_stocks(self,inplace)

    def grouping(self):
        return self.operator.grouping(self)

    def save_info(self):
        return self.operator.save_factor(self)
    
    def gen_stock_list(self):
        """生成股票列表"""
        return self.operator.gen_stock_list(self)

    # ------------------------因子表现------------------------------
    def get_ic(self):
        return self.measurer.get_ic(self)

    def get_grouping_return(self):
        return self.measurer.get_group_returns(self)

    def get_long_short_return(self):
        self.measurer.get_long_short_return(self)


    def plot_performance(self):
        return self.measurer.plot_performance(self)
    
    def get_first_group_active_return(self):
        """计算第一组的表现"""
        self.measurer.get_first_group_active_return(self)
        
        

    # ----------------------因子测试---------------------------------
    def start_back_test(self):
        self.load_data()                           # 加载数据
        self.drop_nan(inplace=True)                # 去掉缺失值
        self.drop_untradable_stocks(inplace=True)  # 去掉不可交易股票的因子值
        self.grouping()                            # 按照因子为股票分组
        self.get_grouping_return()                 # 计算因子分组收益率
        self.get_long_short_return()               # 分组多空收益率
        self.get_first_group_active_return()       # 第一组的超额收益率
        self.get_ic()                              # 计算因子的IC序列
        self.plot_performance()                    # 绘图
        self.save_info()                           # 存储因子信息


