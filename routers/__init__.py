# routers/__init__.py
from . import auth  # expõe "routers.auth"
# se você já tinha coisas aqui, mantenha.
__all__ = ["auth", "dashboard", "export"]  # opcional; inclua outros que já usa
