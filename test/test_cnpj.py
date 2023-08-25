import unittest
import pydriva 

class TestValidateCNPJ(unittest.TestCase):
    def test_cnpj_simple(self):
        cnpj = '07752497000143'
        self.assertTrue(pydriva.cnpj.validate(cnpj))
        
    def test_cnpj_like_a_number(self):
        cnpj = 7752497000143
        self.assertTrue(pydriva.cnpj.validate(cnpj))

    def test_cnpj_with_simbols(self):
        cnpj = '07.752.497/0001-43'
        self.assertTrue(pydriva.cnpj.validate(cnpj))

    def test_cnpj_with_simbols_and_spaces(self):
        cnpj = ' 07.752.497/0001-43 '
        self.assertTrue(pydriva.cnpj.validate(cnpj))

class TestCleanCNPJ(unittest.TestCase):
    def test_cnpj_simple(self):
        cnpj = '07752497000143'
        self.assertEqual(pydriva.cnpj.clean(cnpj), '07752497000143')
    def test_cnpj_like_a_number(self):
        cnpj = 7752497000143
        self.assertEqual(pydriva.cnpj.clean(cnpj), '07752497000143')
    def test_cnpj_with_simbols(self):
        cnpj = '07.752.497/0001-43'
        self.assertEqual(pydriva.cnpj.clean(cnpj), '07752497000143')
    def test_cnpj_with_simbols_and_spaces(self):
        cnpj = ' 07.752.497/0001-43 '
        self.assertEqual(pydriva.cnpj.clean(cnpj), '07752497000143')

class TestExtractRaizCNPJ(unittest.TestCase):
    def test_cnpj_simple(self):
        cnpj = '07752497000143'
        self.assertEqual(pydriva.cnpj.extract_raiz_cnpj(cnpj), '07752497')
    def test_cnpj_like_a_number(self):
        cnpj = 7752497000143
        self.assertEqual(pydriva.cnpj.extract_raiz_cnpj(cnpj), '07752497')
    def test_cnpj_with_simbols(self):
        cnpj = '07.752.497/0001-43'
        self.assertEqual(pydriva.cnpj.extract_raiz_cnpj(cnpj), '07752497')
    def test_cnpj_with_simbols_and_spaces(self):
        cnpj = ' 07.752.497/0001-43 '
        self.assertEqual(pydriva.cnpj.extract_raiz_cnpj(cnpj), '07752497')

