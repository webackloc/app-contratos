#!/usr/bin/env python3
# scripts/create_user.py — v1.0.0 (11/08/2025)
import sys
from getpass import getpass

# garantir import da raiz do projeto
import os
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from database import SessionLocal
from auth_models import User
from security import hash_password

def main():
    db = SessionLocal()
    try:
        username = input("Usuário: ").strip()
        if not username:
            print("Usuário inválido.")
            return
        exists = db.query(User).filter(User.username == username).first()
        if exists:
            print("Usuário já existe.")
            return
        p1 = getpass("Senha: ")
        p2 = getpass("Repita a senha: ")
        if p1 != p2 or not p1:
            print("Senhas não conferem.")
            return
        user = User(username=username, password_hash=hash_password(p1), role="admin", is_active=True)
        db.add(user); db.commit()
        print("Usuário criado com sucesso.")
    finally:
        db.close()

if __name__ == "__main__":
    main()
