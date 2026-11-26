import sys

import pytest

from aliyun.log.etl_core.restrict_config_parser import InvalidETLConfig, RestrictConfigParser


@pytest.mark.parametrize("literal", ["42", "1.5", "2j", "'text'", "u'text'", "b'bytes'", "True", "False", "None"])
def test_parse_etl_config_literal(literal):
    RestrictConfigParser().parse("KEEP_FIELDS_example = {'field': " + literal + "}")


@pytest.mark.skipif(sys.version_info[0] < 3, reason="Ellipsis is not a standalone expression in Python 2")
def test_reject_ellipsis_in_etl_config():
    with pytest.raises(InvalidETLConfig):
        RestrictConfigParser().parse("KEEP_FIELDS_example = {'field': ...}")
