
from aliyun.log.etl_core import *

a = 10
A_1, B_2 = 1,2

def p(fn): pass

@p
def sls_eu_my_logic(event):
    event["hello"] = "world"
    return event
