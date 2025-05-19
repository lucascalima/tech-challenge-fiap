import os
import logging
from fastapi import FastAPI
from app.produto_routes import router as produto_router
from app.importacao_routes import router as importacao_router
from app.exportacao_routes import router as exportacao_router

# Configura o logger do Uvicorn para capturar erros
default_logger = logging.getLogger("uvicorn.error")

app = FastAPI(
    title="Tech Challenge API",
    description="API para acessar dados da Embrapa",
    version="1.0",
    debug=True
)

# Roteadores com prefixos
app.include_router(produto_router, prefix="/api/produtos", tags=["Produtos"])
app.include_router(importacao_router, prefix="/api/dados/importacao", tags=["Importação"])
app.include_router(exportacao_router, prefix="/api/dados/exportacao", tags=["Exportação"])

@app.get("/", summary="Rota de sanity check")
def home():
    return {"status": "API rodando"}