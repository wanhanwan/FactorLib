import numpy as np

records_array = np.array([1, 2, 3, 1, 1, 3, 4, 3, 2])
idx_sort = np.argsort(records_array)
sorted_records_array = records_array[idx_sort]
vals, idx_start, count = np.unique(sorted_records_array, return_counts=True,
                                return_index=True)

# sets of indices
res = np.split(idx_sort, idx_start[1:])
#filter them with respect to their size, keeping only items occurring more than once

vals = vals[count > 1]
res = filter(lambda x: x.size > 1, res)