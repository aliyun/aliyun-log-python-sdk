from aliyun.log.util import require_python3
import six
import pytest

@require_python3
def my_require_python3():
    print("test_require_python3")



def test_my_require_python3():
    if six.PY2:
        with pytest.raises(RuntimeError):
            my_require_python3()
    else:
        my_require_python3()
