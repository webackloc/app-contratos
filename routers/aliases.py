# routers/aliases.py
from fastapi import APIRouter
from routers.ultima_importacao import get_ultima_importacao  # ou a função equivalente

router = APIRouter()
@router.get("/dashboard/ultima_importacao")
def ultima_alias():
    return get_ultima_importacao()
