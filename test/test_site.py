import unittest
import driva 

class TestValidateSite(unittest.TestCase):
    def test_site_simple(self):
        site = 'http://www.google.com'
        self.assertTrue(driva.site.validate(site))
    def test_site_without_schema(self):
        site = 'www.google.com'
        self.assertTrue(driva.site.validate(site))
    def test_site_with_path(self):
        site = 'http://www.google.com/path'
        self.assertTrue(driva.site.validate(site))
    def test_site_with_params(self):
        site = 'http://www.google.com?param=1'
        self.assertTrue(driva.site.validate(site))
    def test_site_with_params_and_path(self):
        site = 'http://www.google.com/path?param=1'
        self.assertTrue(driva.site.validate(site))
    def test_br_site(self):
        site = 'http://www.google.com.br'
        self.assertTrue(driva.site.validate(site))
    def test_invalid_tld(self):
        site = 'http://www.google.com.idontexist'
        self.assertFalse(driva.site.validate(site))

class TestExtractHost(unittest.TestCase):
    def test_site_simple(self):
        site = 'http://www.driva.com.br'
        self.assertEqual(driva.site.extract_host(site), 'driva.com.br')
    def test_site_without_schema(self):
        site = 'www.driva.com.br'
        self.assertEqual(driva.site.extract_host(site), 'driva.com.br')
    def test_site_only_domain(self):
        site = 'driva.com.br'
        self.assertEqual(driva.site.extract_host(site), 'driva.com.br')
    def test_site_with_subdomain(self):
        site = 'http://app.driva.com.br'
        self.assertEqual(driva.site.extract_host(site), 'app.driva.com.br')
    def test_complete_site(self):
        site = 'http://www.driva.com.br/path/to/file?param=1#hash'
        self.assertEqual(driva.site.extract_domain(site), 'driva.com.br')
    def test_invalid_site(self):
        site = 'http://www.driva.com.idontexist'
        self.assertEqual(driva.site.extract_host(site, raise_if_invalid=False), None)
    def test_without_tld(self):
        site = 'http://www.driva'
        self.assertEqual(driva.site.extract_host(site, raise_if_invalid=False), None)

class TestExtractDomain(unittest.TestCase):
    def test_site_simple(self):
        site = 'http://www.driva.com.br'
        self.assertEqual(driva.site.extract_domain(site), 'driva.com.br')
    def test_site_without_schema(self):
        site = 'www.driva.com.br'
        self.assertEqual(driva.site.extract_domain(site), 'driva.com.br')
    def test_site_only_domain(self):
        site = 'driva.com.br'
        self.assertEqual(driva.site.extract_domain(site), 'driva.com.br')
    def test_site_with_subdomain(self):
        site = 'http://app.driva.com.br'
        self.assertEqual(driva.site.extract_domain(site), 'driva.com.br')
    def test_complete_site(self):
        site = 'http://www.driva.com.br/path/to/file?param=1#hash'
        self.assertEqual(driva.site.extract_domain(site), 'driva.com.br')
    def test_invalid_site(self):
        site = 'http://www.driva.com.idontexist'
        self.assertEqual(driva.site.extract_domain(site, raise_if_invalid=False), None)
    def test_invalid_site_without_tld(self):
        site = 'http://www.driva'
        self.assertEqual(driva.site.extract_domain(site, raise_if_invalid=False), None)
    def test_from_blogspot(self):
        site = 'lupanza.blogspot.com.br'
        self.assertEqual(driva.site.extract_domain(site), 'lupanza.blogspot.com.br')

class TestExtractTLD(unittest.TestCase):
    def test_site_simple(self):
        site = 'http://www.driva.com.br'
        self.assertEqual(driva.site.extract_tld(site), 'com.br')
    def test_site_without_schema(self):
        site = 'www.driva.com.br'
        self.assertEqual(driva.site.extract_tld(site), 'com.br')
    def test_site_only_domain(self):
        site = 'driva.com.br'
        self.assertEqual(driva.site.extract_tld(site), 'com.br')
    def test_site_with_subdomain(self):
        site = 'http://app.driva.com.br'
        self.assertEqual(driva.site.extract_tld(site), 'com.br')
    def test_complete_site(self):
        site = 'http://www.driva.com.br/path/to/file?param=1#hash'
        self.assertEqual(driva.site.extract_tld(site), 'com.br')
    def test_com_site(self):
        site = 'http://www.google.com'
        self.assertEqual(driva.site.extract_tld(site), 'com')
    def test_mexican_site(self):
        site = 'http://www.google.com.mx'
        self.assertEqual(driva.site.extract_tld(site), 'com.mx')

class TestRemoveTld(unittest.TestCase):
    def test_site_simple(self):
        site = 'http://www.driva.com.br'
        self.assertEqual(driva.site.remove_tld(site), 'driva')
    def test_site_without_schema(self):
        site = 'www.driva.com.br'
        self.assertEqual(driva.site.remove_tld(site), 'driva')
    def test_site_only_domain(self):
        site = 'driva.com.br'
        self.assertEqual(driva.site.remove_tld(site), 'driva')
    def test_site_with_subdomain(self):
        site = 'http://app.driva.com.br'
        self.assertEqual(driva.site.remove_tld(site), 'driva')
    def test_complete_site(self):
        site = 'http://www.driva.com.br/path/to/file?param=1#hash'
        self.assertEqual(driva.site.remove_tld(site), 'driva')
    def test_com_site(self):
        site = 'http://www.google.com'
        self.assertEqual(driva.site.remove_tld(site), 'google')
    def test_mexican_site(self):
        site = 'http://www.google.com.mx'
        self.assertEqual(driva.site.remove_tld(site), 'google')
    def test_invalid_site(self):
        site = 'http://www.driva.com.idontexist'
        self.assertEqual(driva.site.remove_tld(site, raise_if_invalid=False), None)
    def test_without_tld(self):
        site = 'http://www.driva'
        self.assertEqual(driva.site.remove_tld(site, raise_if_invalid=False), None)


class TestSubdomains(unittest.TestCase):
    def test_www(self):
        site = 'http://www.driva.com.br'
        self.assertEqual(driva.site.extract_subdomain(site), '')
    def test_app(self):
        site = 'http://app.driva.com.br'
        self.assertEqual(driva.site.extract_subdomain(site), 'app')
    