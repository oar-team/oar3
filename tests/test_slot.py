import unittest

class TestFoo(unittest.TestCase):

    def test_foo(self):
        print "yop"
        self.assertEqual(True, True)

if __name__ == '__main__':
    unittest.main()
