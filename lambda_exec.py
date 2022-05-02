import pywren
import numpy as np

def my_function(x):
    return x + 7
my_function(3)

wrenexec = pywren.default_executor()

future = wrenexec.call_async(my_function, 3)

future.result()

futures = wrenexec.map(my_function, range(10))

pywren.get_all_results(futures)