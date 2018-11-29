import unittest

from customer_count import get_customer_ucount


N_UNIQUE_CUSTOMERS = 312


class TestSimilarItems(unittest.TestCase):

    def test_customer_ucount(self):
        self.assertEqual(get_customer_ucount(), N_UNIQUE_CUSTOMERS, msg='Number of unique customers differs from expected')
