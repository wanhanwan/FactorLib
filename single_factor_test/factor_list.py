"""因子构建方式
每一个因子是一个包含三个元素的tuple:
    第一个元素代表因子名称
    第二个元素代表在因子数据库中的字段名
    第三个元素代表因子方向:1 代表正向,-1代表负向

"""
# 估值类因子
bp = ('bp','bp', 1)
bp_minus_median = ('bp_minus_median', 'bp_minus_median', 1)
bp_standard = ('bp_standard', 'bp_standard', 1)
bp_div_median = ('bp_divide_median', 'bp_divide_median', 1)
relative_pe = ('relative_pe', 'relative_pe', -1)

# 反转类因子
reversal = ('reversal', 'reversal', 1)
factor_list = [bp_div_median]