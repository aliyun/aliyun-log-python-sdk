
from aliyun.log.etl_core import *


@condition(True, pass_meta=False)
def sls_en_random_value(event):
    assert '__time__' not in event
    return dict( (k.upper(), v.upper()) for k, v in event.items() )


@condition({'IMEI': r'ABC|XYZ'})
def sls_en_change_time(event):
    assert '__time__' in event
    event['__time__'] = '800000'
    return event


@condition({'IMEI': r'ABC'}, restore_meta=True)
def sls_en_restore(event):
    assert '__time__' in event
    event['__time__'] = '900000'
    return event
