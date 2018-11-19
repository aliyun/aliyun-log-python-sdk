from .trans_base import V
from .trans_regex import *
from .trans_lookup import *

__all__ = ['REGEX', 'CSV', 'TSV', 'JSON', 'KV', 'V']

# field based
REGEX = trans_comp_regex
CSV = trans_comp_csv
TSV = trans_comp_tsv
JSON = trans_comp_json
KV = trans_comp_kv
