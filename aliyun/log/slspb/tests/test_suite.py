import unittest
from tests.test_pb_parse import TestParsePB
from tests.test_pb_write import TestWritePB
from tests.test_pb_misc import TestPBMisc
if __name__ == '__main__':
    suite = unittest.TestSuite()

    tests = [TestParsePB("test_slspb_parsepb"),TestWritePB("test_slspb_writepb"),TestPBMisc("test_slspb_misc")]
    suite.addTests(tests)

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
