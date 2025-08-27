# routers/admin_users.py — v1.5.1 (27/08/2025)
# Administração de usuários (compat ampla + hardening) + Alterar Senha
# Hotfix:
#   - Define "templates = Jinja2Templates(...)" para evitar NameError
#   - Padroniza templates para "admin/..." com fallback para "Admin/..." (legado)
# Mantém todas as compatibilidades de v1.5.0

from datetime import datetime
from typing import Optional
import os

from fastapi import APIRouter, Request, Depends, Form, HTTPException
from fastapi.responses import RedirectResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.status import HTTP_303_SEE_OTHER, HTTP_400_BAD_REQUEST
from sqlalchemy.orm import Session
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from database import SessionLocal
from auth_models import User

__version__ = "1.5.1"

router = APIRouter(prefix="/admin", tags=["Admin"])

# ---------- templates ----------
templates = Jinja2Templates(directory="templates")

# Mapeia nomes amigáveis -> possíveis caminhos (preferindo "admin/...", com fallback "Admin/...")
_TEMPLATE_CANDIDATES = {
    "users_list": ("admin/users_list.html", "Admin/users_list.html"),
    "user_form": ("admin/user_form.html", "Admin/user_form.html"),
    "password_form": ("admin/password_form.html", "Admin/password_form.html", "Admin/password_form.htm"),
}

def pick_template(name: str) -> str:
    candidates = _TEMPLATE_CANDIDATES.get(name, ())
    base_dir = "templates"
    for rel in candidates:
        if os.path.exists(os.path.join(base_dir, rel)):
            return rel
    # se nenhum existir, devolve o primeiro (admin/...) para falhar de forma clara
    return candidates[0] if candidates else name

# ---------------- banco ----------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------------- configuração de compat ----------------
EMAIL_FIELDS = ("email", "user_email", "email_address", "mail")
ADMIN_BOOL_FIELDS = ("is_admin", "isAdmin", "admin")
ADMIN_ROLE_FIELDS = ("role", "perfil", "profile")
PASSWORD_FIELDS = ("password_hash", "hashed_password", "password", "senha_hash", "senha")
CREATED_FIELDS = ("created_at", "createdAt", "created_on", "data_criacao", "created")
USERNAME_FIELDS = ("username", "user_name", "login")

# ---------------- utils ----------------
def _hash_password(plain: str) -> str:
    # tenta hashers do projeto; cai para sha256
    try:
        from auth import get_password_hash  # type: ignore
        return get_password_hash(plain)
    except Exception:
        pass
    try:
        from security import hash_password  # type: ignore
        return hash_password(plain)
    except Exception:
        pass
    import hashlib
    return hashlib.sha256(plain.encode()).hexdigest()

def _to_bool(v) -> bool:
    return str(v).strip().lower() in {"1", "true", "on", "yes", "y", "sim"}

def _email_attr_name() -> Optional[str]:
    for f in EMAIL_FIELDS:
        if hasattr(User, f):
            return f
    return None

def _username_attr_name() -> Optional[str]:
    for f in USERNAME_FIELDS:
        if hasattr(User, f):
            return f
    return None

def _set_email(user_obj: User, email: Optional[str]) -> None:
    for f in EMAIL_FIELDS:
        if hasattr(user_obj, f):
            try:
                setattr(user_obj, f, (email or None))
                return
            except Exception:
                continue

def _set_username(user_obj: User, username: str) -> None:
    for f in USERNAME_FIELDS:
        if hasattr(user_obj, f):
            try:
                setattr(user_obj, f, username)
                return
            except Exception:
                continue
    # fallback “gentil”: cria atributo não mapeado, só para não quebrar
    try:
        setattr(user_obj, "username_alias", username)
    except Exception:
        pass

def _set_password(user_obj: User, hashed: str) -> None:
    for f in PASSWORD_FIELDS:
        if hasattr(user_obj, f):
            try:
                setattr(user_obj, f, hashed)
                return
            except Exception:
                continue
    setattr(user_obj, "password_hash", hashed)

def _set_created_now(user_obj: User) -> None:
    for f in CREATED_FIELDS:
        if hasattr(user_obj, f):
            try:
                setattr(user_obj, f, datetime.utcnow())
                return
            except Exception:
                continue

def is_user_admin(user_obj: User) -> bool:
    # campos booleanos primeiro
    for f in ADMIN_BOOL_FIELDS:
        if hasattr(user_obj, f):
            try:
                return bool(getattr(user_obj, f))
            except Exception:
                pass
    # campos string (role/perfil/profile)
    for f in ADMIN_ROLE_FIELDS:
        if hasattr(user_obj, f):
            try:
                v = getattr(user_obj, f)
            except Exception:
                v = None
            if isinstance(v, str) and v.lower() == "admin":
                return True
    # último recurso: atributo leve
    try:
        return bool(getattr(user_obj, "is_admin_flag"))
    except Exception:
        return False

def set_admin_flags(user_obj: User, admin_flag: bool) -> None:
    """
    Marca o usuário como admin:
    1) via role/perfil/profile (strings "admin"/"user")
    2) via booleanos (is_admin/isAdmin/admin), protegendo contra property sem setter
    3) último recurso: atributo leve 'is_admin_flag'
    """
    wrote = False

    # 1) strings
    for f in ADMIN_ROLE_FIELDS:
        if hasattr(user_obj, f):
            try:
                setattr(user_obj, f, "admin" if admin_flag else "user")
                wrote = True
            except Exception:
                pass

    # 2) booleanos
    for f in ADMIN_BOOL_FIELDS:
        if hasattr(user_obj, f):
            try:
                setattr(user_obj, f, bool(admin_flag))
                wrote = True
            except AttributeError:
                # property sem setter
                continue
            except Exception:
                continue

    # 3) último recurso
    if not wrote:
        try:
            setattr(user_obj, "is_admin_flag", bool(admin_flag))
        except Exception:
            pass

def _order_attr():
    for f in CREATED_FIELDS:
        if hasattr(User, f):
            return getattr(User, f)
    # fallback por id
    return getattr(User, "id")

# Helpers para template
def get_email(user_obj: User) -> Optional[str]:
    for f in EMAIL_FIELDS:
        if hasattr(user_obj, f):
            try:
                val = getattr(user_obj, f)
            except Exception:
                continue
            if val:
                return str(val)
    return None

# ---------------- guarda ----------------
def require_admin(request: Request, db: Session) -> User:
    uid = request.session.get("user_id")
    if not uid:
        raise RedirectResponse("/login?next=" + request.url.path, status_code=HTTP_303_SEE_OTHER)
    current = db.query(User).filter(User.id == uid).first()
    if not current:
        raise RedirectResponse("/login?next=" + request.url.path, status_code=HTTP_303_SEE_OTHER)
    if not is_user_admin(current):
        raise HTTPException(status_code=403, detail="Acesso restrito a administradores.")
    return current

# ---------------- rotas ----------------
@router.get("/users", response_class=HTMLResponse)
async def users_list(request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    users = db.query(User).order_by(_order_attr()).all()
    return templates.TemplateResponse(
        pick_template("users_list"),
        {
            "request": request,
            "users": users,
            "msg": request.query_params.get("msg"),
            "error": request.query_params.get("error"),
            "email_field": _email_attr_name(),
            "get_email": get_email,
            "is_user_admin": is_user_admin,
        },
    )

@router.get("/users/novo", response_class=HTMLResponse)
async def users_new_form(request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    return templates.TemplateResponse(pick_template("user_form"), {"request": request, "error": None})

@router.post("/users/novo")
async def users_create(
    request: Request,
    username: str = Form(...),
    email: Optional[str] = Form(None),
    password: str = Form(...),
    password_confirm: str = Form(...),
    is_admin: Optional[str] = Form(None),
    db: Session = Depends(get_db),
):
    require_admin(request, db)

    if password != password_confirm:
        return templates.TemplateResponse(
            pick_template("user_form"),
            {"request": request, "error": "As senhas não conferem."},
            status_code=HTTP_400_BAD_REQUEST,
        )

    # checa unicidade de username (se existir no modelo)
    username_attr_name = _username_attr_name()
    if username_attr_name:
        username_attr = getattr(User, username_attr_name)
        exists_user = db.query(User).filter(func.lower(username_attr) == username.lower()).first()
        if exists_user:
            return templates.TemplateResponse(
                pick_template("user_form"),
                {"request": request, "error": "Usuário já existe."},
                status_code=HTTP_400_BAD_REQUEST,
            )

    # checa unicidade de email (se existir e email fornecido)
    email_attr_name = _email_attr_name()
    if email and email_attr_name:
        email_attr = getattr(User, email_attr_name)
        exists_email = db.query(User).filter(func.lower(email_attr) == email.lower()).first()
        if exists_email:
            return templates.TemplateResponse(
                pick_template("user_form"),
                {"request": request, "error": "E-mail já em uso."},
                status_code=HTTP_400_BAD_REQUEST,
            )

    # cria
    user = User()
    _set_username(user, username.strip())
    _set_email(user, (email or "").strip() or None)
    _set_password(user, _hash_password(password))
    set_admin_flags(user, _to_bool(is_admin))
    _set_created_now(user)

    try:
        db.add(user)
        db.commit()
    except IntegrityError:
        db.rollback()
        return templates.TemplateResponse(
            pick_template("user_form"),
            {"request": request, "error": "Violação de unicidade. Verifique usuário e e-mail."},
            status_code=HTTP_400_BAD_REQUEST,
        )

    return RedirectResponse("/admin/users?msg=Usuário%20criado", status_code=HTTP_303_SEE_OTHER)

@router.post("/users/{user_id}/remover")
async def users_remove(user_id: int, request: Request, db: Session = Depends(get_db)):
    current = require_admin(request, db)

    target = db.query(User).filter(User.id == user_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Usuário não encontrado.")
    if target.id == current.id:
        return RedirectResponse(
            "/admin/users?error=Você%20não%20pode%20remover%20o%20próprio%20usuário",
            status_code=HTTP_303_SEE_OTHER,
        )

    # impedir remover o último admin
    if is_user_admin(target):
        admins_count = 0
        q = db.query(User)
        if hasattr(User, "is_admin"):
            admins_count = q.filter(getattr(User, "is_admin") == True).count()  # noqa: E712
        elif hasattr(User, "isAdmin"):
            admins_count = q.filter(getattr(User, "isAdmin") == True).count()  # noqa: E712
        elif hasattr(User, "admin"):
            admins_count = q.filter(getattr(User, "admin") == True).count()  # noqa: E712
        elif hasattr(User, "role"):
            admins_count = q.filter(getattr(User, "role") == "admin").count()
        elif hasattr(User, "perfil"):
            admins_count = q.filter(getattr(User, "perfil") == "admin").count()
        elif hasattr(User, "profile"):
            admins_count = q.filter(getattr(User, "profile") == "admin").count()

        if admins_count <= 1:
            return RedirectResponse(
                "/admin/users?error=Não%20é%20possível%20remover%20o%20último%20admin",
                status_code=HTTP_303_SEE_OTHER,
            )

    db.delete(target)
    db.commit()
    return RedirectResponse("/admin/users?msg=Usuário%20removido", status_code=HTTP_303_SEE_OTHER)

# ---------------- Alterar senha ----------------
@router.get("/users/{user_id}/senha", response_class=HTMLResponse)
async def password_form(user_id: int, request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuário não encontrado.")
    return templates.TemplateResponse(
        pick_template("password_form"),
        {
            "request": request,
            "user": user,
            "get_email": get_email,
            "error": request.query_params.get("error"),
        },
    )

@router.post("/users/{user_id}/senha")
async def password_update(
    user_id: int,
    request: Request,
    nova_senha: str = Form(...),
    confirmar_senha: str = Form(...),
    db: Session = Depends(get_db),
):
    require_admin(request, db)

    if nova_senha != confirmar_senha:
        return RedirectResponse(
            f"/admin/users/{user_id}/senha?error=As%20senhas%20não%20conferem",
            status_code=HTTP_303_SEE_OTHER,
        )

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuário não encontrado.")

    _set_password(user, _hash_password(nova_senha))
    db.commit()

    return RedirectResponse("/admin/users?msg=Senha%20atualizada", status_code=HTTP_303_SEE_OTHER)
