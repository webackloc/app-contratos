# routers/admin_users.py  — v1.0 (gestão de usuários sem quebrar nada)
from typing import Optional
import hashlib
from datetime import datetime

from fastapi import APIRouter, Request, Depends, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

# DB session
try:
    from database import SessionLocal
except Exception:
    SessionLocal = None

def get_db():
    if SessionLocal is None:
        raise RuntimeError("SessionLocal indisponível. Verifique o módulo database.")
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Descobrir o modelo de usuário
AuthUser = None
for name in ("User", "Usuario", "Users", "AuthUser"):
    try:
        from models import __dict__ as _MODELS_DICT  # type: ignore
        if name in _MODELS_DICT:
            AuthUser = _MODELS_DICT[name]
            break
    except Exception:
        pass

# Hash de senha (tenta importar do router de auth; se não, sha256 simples)
def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

try:
    from routers.auth import get_password_hash as _hash  # type: ignore
    get_password_hash = _hash
except Exception:
    get_password_hash = _sha256

# Heurísticas de colunas
def colnames(model) -> set[str]:
    try:
        return set(model.__table__.columns.keys())
    except Exception:
        return set()

def pick_username_col(cols: set[str]) -> Optional[str]:
    for c in ("username", "email", "login", "user", "nome_usuario"):
        if c in cols: return c
    return None

def pick_password_col(cols: set[str]) -> Optional[str]:
    for c in ("password_hash", "hashed_password", "password", "senha", "senha_hash"):
        if c in cols: return c
    return None

def pick_id_col(cols: set[str]) -> str:
    for c in ("id", "user_id", "id_usuario"):
        if c in cols: return c
    # fallback seguro — SQLAlchemy quase sempre tem 'id'
    return "id"

def pick_admin_col(cols: set[str]) -> Optional[str]:
    for c in ("is_admin", "admin", "is_superuser", "is_staff", "perfil_admin"):
        if c in cols: return c
    return None

def pick_active_col(cols: set[str]) -> Optional[str]:
    for c in ("is_active", "active", "ativo"):
        if c in cols: return c
    return None

router = APIRouter()

# Listagem / formulário (tudo em uma página)
@router.get("/users", response_class=HTMLResponse)
async def list_users(request: Request, db: Session = Depends(get_db)):
    if AuthUser is None:
        html = """
        <div style="padding:16px;font-family:system-ui">
          <h3>Gestão de Usuários</h3>
          <div class="alert alert-warning">Modelo de usuário não encontrado nos seus <code>models.py</code>.
          Nome esperado: <code>User</code> ou <code>Usuario</code>. Ajuste e recarregue.</div>
          <a class="btn btn-secondary" href="/">Voltar</a>
        </div>"""
        return HTMLResponse(html)

    cols = colnames(AuthUser)
    id_col = pick_id_col(cols)
    uname_col = pick_username_col(cols)
    isadmin_col = pick_admin_col(cols)
    active_col = pick_active_col(cols)

    users = db.query(AuthUser).all()

    # Monta tabela simples
    def cell(u, c): return getattr(u, c, "")
    rows = []
    for u in users:
        uid = cell(u, id_col)
        uname = cell(u, uname_col) if uname_col else "(sem coluna de username)"
        isadm = cell(u, isadmin_col) if isadmin_col else ""
        actv = cell(u, active_col) if active_col else ""
        rows.append(
            f"<tr><td>{uid}</td><td>{uname}</td><td>{isadm}</td><td>{actv}</td>"
            f"<td><form method='post' action='/admin/users/{uid}/delete' onsubmit='return confirm(\"Remover este usuário?\")'>"
            f"<button class='btn btn-sm btn-outline-danger'>Remover</button></form></td></tr>"
        )

    html = f"""
    <!doctype html><html lang="pt-br">
    <head>
      <meta charset="utf-8"/>
      <meta name="viewport" content="width=device-width,initial-scale=1"/>
      <title>Usuários</title>
      <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css">
    </head>
    <body class="container py-4">
      <h3>👤 Gestão de Usuários</h3>
      <div class="card mb-4">
        <div class="card-body">
          <form method="post" action="/admin/users">
            <div class="row g-3">
              <div class="col-md-4">
                <label class="form-label">Usuário (username/email)</label>
                <input required name="username" class="form-control" />
              </div>
              <div class="col-md-4">
                <label class="form-label">Senha</label>
                <input required type="password" name="password" class="form-control" />
              </div>
              <div class="col-md-2">
                <label class="form-label">Administrador?</label>
                <select name="is_admin" class="form-select">
                  <option value="0" selected>Não</option>
                  <option value="1">Sim</option>
                </select>
              </div>
              <div class="col-md-2 d-flex align-items-end">
                <button class="btn btn-primary w-100">Criar usuário</button>
              </div>
            </div>
          </form>
        </div>
      </div>

      <div class="table-responsive">
        <table class="table table-sm table-striped align-middle">
          <thead><tr>
            <th>ID</th><th>Usuário</th><th>Admin</th><th>Ativo</th><th>Ações</th>
          </tr></thead>
          <tbody>{''.join(rows) if rows else "<tr><td colspan='5' class='text-center text-muted'>Sem usuários cadastrados</td></tr>"}</tbody>
        </table>
      </div>
      <a class="btn btn-outline-secondary" href="/">Voltar</a>
    </body></html>
    """
    return HTMLResponse(html)

@router.post("/users")
async def create_user(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    is_admin: int = Form(0),
    db: Session = Depends(get_db),
):
    if AuthUser is None:
        raise HTTPException(status_code=500, detail="Modelo de usuário não encontrado (User/Usuario).")

    cols = colnames(AuthUser)
    uname_col = pick_username_col(cols)
    pwd_col = pick_password_col(cols)
    isadmin_col = pick_admin_col(cols)
    active_col = pick_active_col(cols)

    if not uname_col or not pwd_col:
        raise HTTPException(status_code=500, detail="Colunas de usuário/senha não identificadas no modelo.")

    # Duplicidade
    exists = db.query(AuthUser).filter(getattr(AuthUser, uname_col) == username.strip()).first()
    if exists:
        raise HTTPException(status_code=400, detail="Usuário já existe.")

    # Monta kwargs baseado nas colunas existentes
    kwargs = {uname_col: username.strip()}
    # senha
    hashed = get_password_hash(password)
    kwargs[pwd_col] = hashed
    # is_admin
    if isadmin_col:
        kwargs[isadmin_col] = bool(is_admin)
    # ativo
    if active_col:
        kwargs[active_col] = True
    # created_at, se existir
    if "created_at" in cols:
        kwargs["created_at"] = datetime.utcnow()

    # cria
    novo = AuthUser(**{k: v for k, v in kwargs.items() if k in cols})
    db.add(novo)
    db.commit()
    return RedirectResponse("/admin/users", status_code=303)

@router.post("/users/{user_id}/delete")
async def delete_user(user_id: int, db: Session = Depends(get_db)):
    if AuthUser is None:
        raise HTTPException(status_code=500, detail="Modelo de usuário não encontrado (User/Usuario).")

    cols = colnames(AuthUser)
    id_col = pick_id_col(cols)

    alvo = db.query(AuthUser).filter(getattr(AuthUser, id_col) == user_id).first()
    if not alvo:
        raise HTTPException(status_code=404, detail="Usuário não encontrado.")

    db.delete(alvo)
    db.commit()
    return RedirectResponse("/admin/users", status_code=303)
