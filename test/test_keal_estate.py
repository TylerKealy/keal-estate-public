import unittest
from keal_estate import app

class FlaskTestCase(unittest.TestCase):

    def setUp(self):
        # Create a test client
        self.app = app.test_client()
        self.app.testing = True 

    def test_home_status_code(self):
        # Sends HTTP GET request to the application
        result = self.app.get('/') 

        # Assert that the response status code is 200 (OK)
        self.assertEqual(result.status_code, 200)

    def test_home_data(self):
        # Sends HTTP GET request to the application
        result = self.app.get('/')  

        # Assert that the response data matches
        self.assertEqual(result.data, b'Expected data')

if __name__ == "__main__":
    unittest.main()
