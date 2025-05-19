from fastapi import FastAPI
from app.produto_routes import router as produto_router
from app.importacao_routes import router as importacao_router
from app.exportacao_routes import router as exportacao_router


def create_app():
    app = FastAPI(
        title="Tech Challenge API",
        description="API para acessar dados da Embrapa",
        version="1.0",
        contact={
            "name": "Alexandre Lima, Thiago Ramos, Lucas Caique de Lima, Eduardo Barbosa",
            "email": "ale_and5@hotmail.com"
        }
    )

    # Inclui os routers com prefixo /api
    app.include_router(produto_router, prefix="/api/produtos", tags=["Produtos"])
    app.include_router(importacao_router, prefix="/api/dados", tags=["Importação"])
    app.include_router(exportacao_router, prefix="/api/dados", tags=["Exportação"])

    return app
