from pydantic import BaseModel

class DadosComerciais(BaseModel):
    pais: str
    quantidade_kg: str
    valor_usd: str
    
    class Config:
        json_schema_extra = {
            "example": {
                "pais": "Brasil",
                "quantidade_kg": "1.000",
                "valor_usd": "$5.000,00"
            }
        }