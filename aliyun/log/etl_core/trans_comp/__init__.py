from .trans_base import V
from .trans_regex import *
from .trans_lookup import *
from .trans_csv import *
from .trans_kv import *

__all__ = ['LOOKUP', 'REGEX', 'CSV', 'TSV', 'JSON', 'KV', 'V']

# field based
LOOKUP = trans_comp_lookup
REGEX = trans_comp_regex
CSV = trans_comp_csv
TSV = trans_comp_tsv
JSON = trans_comp_json
KV = trans_comp_kv
