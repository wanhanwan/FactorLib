from data_source import mongo
from data_source.h5db import H5DB

h5 = H5DB("D:/data/h5")

library = 'stocks'
for lib, symbols in mongo.data_dict.items():
    if lib == 'stocks':
        continue    
    mongo.set_library(lib)
    for symbol in symbols:
        print("%s %s" % (lib, symbol))
        data = mongo.loadFactor(symbol)
        h5.save_factor(data, '/%s' % lib)