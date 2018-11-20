from aliyun.log.etl_core import *

# drop empty fields and remove _ for each fields
@condition(True, pass_meta=False)
def sls_en_remove_empty_fields(event):
    return dict( (k.strip('_'), str(v).strip('_') ) for k, v in event.items() if str(v) != '' )


# add 12 hours
def sls_eu_post_12_hours(event):
    return {'__time__': str(int(event['__time__'])+12*3600) }

