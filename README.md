
[ Exemplo ]
Exemplo antigo de uso:
    driva.clean.email(email:str) -> email:str
    driva.clean.domain(domain:str) -> domain:str
    driva.clean.phone(phone:str) -> phone:str

    driva.extract.domain(url:str) -> domain:str
    driva.extract.ddd(phone:str) -> ddd: str

    driva.validate.email(email:str) -> bool
    driva.validate.domain(domain:str) -> bool
    driva.validate.tld(domain:str) -> bool

exemplos de uso:
    #-------
    driva.cnpj.clean(cnpj:str) -> str
    driva.cnpj.extract_raiz(cnpj:str) -> str
    
    driva.site.check_valid(site:str) -> str
    driva.site.get_host(site:str) -> str
    driva.site.get_domain(site:str) -> str
    driva.site.get_tld(site:str) -> str

    