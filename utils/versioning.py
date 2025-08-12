"""
Módulo: Versionamento de Endpoints
Versão: 1.0.0
Data: 2025-08-11
Autor: Leonardo Muller

Fornece:
- @version("x.y.z"): anexa a versão ao callable do endpoint via atributo __version__
- set_version_header: dependência que lê a versão do endpoint e adiciona X-Endpoint-Version na resposta
"""

from functools import wraps
from typing import Callable, Any, Awaitable, Optional
from fastapi import Request, Response

def version(v: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator para marcar um endpoint com uma versão sem alterar a resposta.
    Ex.: @version("1.2.3")
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        setattr(func, "__version__", v)

        # Não precisamos necessariamente wrappar, mas envolvemos
        # para garantir que __wrapped__ exista (útil em introspecção).
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await func(*args, **kwargs)  # type: ignore[misc]

        # Se o endpoint original for síncrono, preserve também
        if not hasattr(func, "__call__") or func.__name__ == "async_wrapper":
            return async_wrapper

        # Detecta se é coroutine; se não for, cria wrapper síncrono
        import inspect
        if inspect.iscoroutinefunction(func):
            return async_wrapper  # type: ignore[return-value]

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        # Copia a versão para o wrapper
        setattr(sync_wrapper, "__version__", v)
        return sync_wrapper
    return decorator

async def set_version_header(request: Request, response: Response) -> None:
    """
    Dependência para ser aplicada no APIRouter (dependencies=[Depends(set_version_header)]).
    Lê a função do endpoint atual (request.scope['endpoint']) e, se tiver __version__,
    adiciona o header X-Endpoint-Version.
    """
    endpoint: Optional[Callable[..., Any]] = request.scope.get("endpoint")  # type: ignore[assignment]
    if endpoint is None:
        return
    ver = getattr(endpoint, "__version__", None)
    if ver:
        response.headers["X-Endpoint-Version"] = str(ver)
