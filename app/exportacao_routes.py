from fastapi import APIRouter, status, HTTPException
from typing import List
import requests
from app.model.dados_comerciais import DadosComerciais
from app.services import embrapa_service
from app.mongo import db
from pymongo import UpdateOne
from pymongo.errors import PyMongoError

router = APIRouter()

@router.get('/exportacao', response_model=List[DadosComerciais], summary="Dados de exportação da Embrapa")
async def get_exportacao():
    url = 'http://vitibrasil.cnpuv.embrapa.br/index.php?opcao=opt_06'
    response = requests.get(url)
    response.encoding = 'utf-8'
    dados_extraidos = embrapa_service.extrair_exportacao_importacao(response.text)
    registros = [DadosComerciais(**d) for d in dados_extraidos]

    operations = [
        UpdateOne(
            {"pais": r.pais, "quantidade_kg": r.quantidade_kg, "valor_usd": r.valor_usd},
            {"$setOnInsert": r.dict()},
            upsert=True
        ) for r in registros
    ]

    try:
        db.exportacao.bulk_write(operations, ordered=False)
    except PyMongoError as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    return registros
