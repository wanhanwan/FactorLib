from bokeh.plotting import curdoc, figure
from bokeh.layouts import widgetbox
from bokeh.models.widgets import Slider, Select
from bokeh.layouts import row, column

from data_source import h5
from utils.datetime_func import Datetime2DateStr, DateStr2Datetime
import numpy as np

def nix(value, lst):
    return [x for x in lst if x != value]

def factors_of_path(path, factor_dict):
    return factor_dict[factor_dict.path==path]['name'].tolist()

def factor_datetimes(factor):
    return [Datetime2DateStr(x) for x in factor.index.get_level_values(0).unique()]

# 设置数据
DEFAULT_FACTOR_PATH = '/stock_momentum/'
DEFAULT_FACTOR = factors_of_path(DEFAULT_FACTOR_PATH, h5.data_dict)[0]
paths = h5.data_dict['path'].unique().tolist()
data = h5.load_factor(DEFAULT_FACTOR, DEFAULT_FACTOR_PATH)
data_datetimes = factor_datetimes(data)

# 设置控件
binSlider = Slider(title="Bins", value=10, start=10, end=100, step=5)  # 控制分布图的bins
factorPathSelect = Select(title='factor path', value=DEFAULT_FACTOR_PATH, options=paths)
factorSelect = Select(title='factor name', value=DEFAULT_FACTOR,
                      options=factors_of_path(DEFAULT_FACTOR_PATH, h5.data_dict))
datesSelect = Select(title='datetimes', value=data_datetimes[0], options=nix(data_datetimes[0], data_datetimes))

def factorPathUpdate(attr, old, new):
    factorSelect.value = factors_of_path(new, h5.data_dict)[0]
    factorSelect.options = factors_of_path(new, h5.data_dict)
    
    global data, data_datetimes
    data = h5.load_factor(factors_of_path(new, h5.data_dict)[0], new)
    data_datetimes = factor_datetimes(data)
    datesSelect.options = data_datetimes
    update()
    
def factorUpdate(attr, old, new):
    global data, data_datetimes
    data = h5.load_factor(new, factorPathSelect.value)
    data_datetimes = factor_datetimes(data)
    datesSelect.options = data_datetimes    
    update()
def datesUpdate(attr, old, new):
    update()
def sliderUpdate(attr, old, new):
    update()

# 设置图像
fig = figure(plot_width=600, title='Linked Histograms')
h1 = fig.quad(bottom=0, top=[], left=[], right=[], line_color="#3A5785")

def update():
    bins = binSlider.value
    factor_data = data.loc[DateStr2Datetime(datesSelect.value), factorSelect.value].dropna().values
    hist, edge = np.histogram(factor_data, bins=bins)
    h1.data_source.data['top'] = hist
    h1.data_source.data['left'] = edge[:-1]
    h1.data_source.data['right'] = edge[1:]

binSlider.on_change('value', sliderUpdate)
factorPathSelect.on_change('value', factorPathUpdate)
factorSelect.on_change('value', factorUpdate)
datesSelect.on_change('value', datesUpdate)

weidgets = column(factorPathSelect, factorSelect, datesSelect, binSlider)
layout = row(weidgets, fig)

update()
curdoc().add_root(layout)