import tld 

def validate(site: str) -> bool:
    res = tld.get_tld(site, fix_protocol=True, fail_silently=True)
    return res is not None

def extract_host(site: str, raise_if_invalid=True) -> str:
    if not validate(site):
        if raise_if_invalid:
            raise ValueError("Invalid site")
        return None
    res = tld.get_tld(site, fix_protocol=True, as_object=True)
    host = res.parsed_url.hostname
    if host.startswith("www."):
        host = host[4:]
    return host

def extract_domain(site: str, raise_if_invalid=True) -> str:
    if not validate(site):
        if raise_if_invalid:
            raise ValueError("Invalid site")
        return None
    res = tld.get_tld(site, fix_protocol=True, as_object=True)
    return res.fld

def extract_tld(site: str, raise_if_invalid=True) -> str:
    if not validate(site):
        if raise_if_invalid:
            raise ValueError("Invalid site")
        return None
    res = tld.get_tld(site, fix_protocol=True, as_object=True)
    return res.tld

def remove_tld(site: str, raise_if_invalid=True) -> str:
    if not validate(site):
        if raise_if_invalid:
            raise ValueError("Invalid site")
        return None
    res = tld.get_tld(site, fix_protocol=True, as_object=True)
    return res.domain