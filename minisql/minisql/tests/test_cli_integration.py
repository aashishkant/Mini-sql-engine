import unittest
from mini_sql_engine.cli import main

class TestCLIIntegration(unittest.TestCase):
    
    def test_cli_help(self):
        """Test if the CLI help command works correctly."""
        result = main(['--help'])
        self.assertIn('usage:', result)

    def test_cli_version(self):
        """Test if the CLI version command returns the correct version."""
        result = main(['--version'])
        self.assertEqual(result, 'MiniSQL version 1.0.0')  # Replace with actual version

    def test_cli_invalid_command(self):
        """Test if the CLI handles invalid commands gracefully."""
        result = main(['invalid_command'])
        self.assertIn('Error:', result)

    # Add more integration tests as needed

if __name__ == '__main__':
    unittest.main()