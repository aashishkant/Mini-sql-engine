import unittest
import subprocess

class TestRunner(unittest.TestCase):
    def test_run_tests(self):
        result = subprocess.run(['python', 'tests/test_simple.py'], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, "Test simple failed")

        result = subprocess.run(['python', '-m', 'unittest', 'discover', 'tests'], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, "Some tests failed")

if __name__ == '__main__':
    unittest.main()