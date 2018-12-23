
from aliyun.log.etl_core import *

def sls_eu_my_logic(event):
    event["hello"] = "world"
    return event

