from typing import union
import re 

#TODO validar cnpj
def validate(cnpj:union[str,int]) -> bool:
    cnpj = clean(cnpj, validate=False)
    return True

def clean(cnpj:union[str,int], validate=True) -> str:
    if isinstance(cnpj, int):
        cnpj = str(cnpj)
        
    cnpj = cnpj.replace(".", "").replace("-", "").replace("/", "")
    cnpj = cnpj.strip()
    cnpj = cnpj.zfill(14)
    #check if only numbers
    if not re.match(r"^[0-9]*$", cnpj):
        raise ValueError("Formato de CNPJ inválido")

    #TODO checkar validade
    # if validate and self.validate(cnpj):
    #     raise ValueError("CNPJ inválido")

    return cnpj

def extract_raiz_cnpj(cnpj:str) -> str:
    cnpj = clean(cnpj)
    return cnpj[:8]
