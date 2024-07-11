import unittest
from .test_pb_parse import TestParsePB
from .test_pb_write import TestWritePB
from .test_pb_misc import TestPBMisc
from .test_pb_compare import TestWritePB as TestPBCompare
from .test_pb_stress import TestWritePB as TestWritePBStress

if __name__ == '__main__':
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()

    suite.addTests(loader.loadTestsFromTestCase(TestParsePB))
    suite.addTests(loader.loadTestsFromTestCase(TestWritePB))
    suite.addTests(loader.loadTestsFromTestCase(TestPBMisc))
    suite.addTests(loader.loadTestsFromTestCase(TestPBCompare))
    # suite.addTests(loader.loadTestsFromTestCase(TestWritePBStress))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    if not result.wasSuccessful():
        exit(-1)
    exit(0)
