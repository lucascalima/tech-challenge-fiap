from pydantic import BaseModel, Field

class Produto(BaseModel):
    categoria: str
    tipo_produto: str
    quantidade: str

    class Config:
        json_schema_extra = {
            "example": {
                "categoria": "Vinho Tinto",
                "tipo_produto": "Tinto",
                "quantidade": "139.320.884"
            }
        }