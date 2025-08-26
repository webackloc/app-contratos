
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
popular_codcli_cabecalhos.py
v0.1.1 (2025-08-25)

Script rápido para popular o campo cod_cli dos cabeçalhos de contratos
(ContratoCabecalho) a partir dos itens (Contrato). Por padrão roda em DRY-RUN
(sem aplicar). Use --apply para gravar no banco.

Regras:
- Para cada contrato (número), escolhe o cod_cli mais frequente entre os itens.
- Atualiza SOMENTE cabeçalhos cujo cod_cli está vazio (NULL ou '').
- Gera um CSV de backup com as alterações propostas/realizadas.

Uso:
  python popular_codcli_cabecalhos.py                 # dry-run (não grava)
  python popular_codcli_cabecalhos.py --apply         # aplica mudanças
  python popular_codcli_cabecalhos.py --limit 200     # limita a 200 cabeçalhos
  python popular_codcli_cabecalhos.py --apply --limit 500

Dependências:
- Projeto com módulos `database` (SessionLocal/engine) e `models` (Contrato, ContratoCabecalho).
- Alternativamente, defina DATABASE_URL (ex.: postgresql+psycopg2://...).
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Optional
from datetime import datetime  # <-- FIX: import necessário

# Tentativa 1: usar módulos do projeto
SessionLocal = None
Contrato = None
ContratoCabecalho = None
engine = None

try:
    import database as _db
    from models import Contrato as _Contrato, ContratoCabecalho as _ContratoCabecalho  # type: ignore
    SessionLocal = getattr(_db, "SessionLocal", None)
    engine = getattr(_db, "engine", None)
    Contrato = _Contrato
    ContratoCabecalho = _ContratoCabecalho
except Exception as e:
    print("[aviso] Não consegui importar 'database' e 'models' do projeto:", e, file=sys.stderr)

# Se não achou, tenta fallback leve com SQLAlchemy a partir de DATABASE_URL
if SessionLocal is None or Contrato is None or ContratoCabecalho is None:
    try:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker, declarative_base, mapped_column, Mapped
        from sqlalchemy import String, Integer, Column
        url = os.environ.get("DATABASE_URL")
        if not url:
            raise RuntimeError("Defina DATABASE_URL ou rode dentro do projeto (com database/models).")
        engine = create_engine(url, future=True)
        SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
        Base = declarative_base()

        # Modelos mínimos (ajuste nomes dos campos se necessário)
        class Contrato(Base):  # type: ignore
            __tablename__ = "contratos"
            id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
            contrato_num: Mapped[Optional[str]] = mapped_column(String, nullable=True)
            contrato_n: Mapped[Optional[str]] = mapped_column(String, nullable=True)
            cod_cli: Mapped[Optional[str]] = mapped_column(String, nullable=True)

        class ContratoCabecalho(Base):  # type: ignore
            __tablename__ = "contratos_cabecalho"
            id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
            contrato_num: Mapped[Optional[str]] = mapped_column(String, nullable=True)
            contrato_n: Mapped[Optional[str]] = mapped_column(String, nullable=True)
            cod_cli: Mapped[Optional[str]] = mapped_column(String, nullable=True)
        print("[info] Usando fallback com DATABASE_URL.")
    except Exception as e:
        print("[erro] Sem modelos e sem DATABASE_URL. Saindo.", file=sys.stderr)
        sys.exit(1)

from sqlalchemy.orm import Session
from sqlalchemy import func, String, cast

def _pick_attr(model: Any, *names: str):
    for n in names:
        if hasattr(model, n):
            return n, getattr(model, n)
    return None, None

def _is_blank(val: Any) -> bool:
    if val is None:
        return True
    s = str(val).strip()
    return s == ""

@dataclass
class Change:
    cab_id: int
    contrato_num: str
    antigo: Optional[str]
    novo: str

def coletar_melhor_codcli_por_contrato(sess: Session, item_num_col) -> dict[str, str]:
    """
    Retorna dict num_contrato -> cod_cli (mais frequente), apenas quando houver cod_cli válido.
    """
    rows = (
        sess.query(
            item_num_col.label("num"),
            getattr(Contrato, "cod_cli").label("cod"),
            func.count("*").label("cnt"),
        )
        .filter(
            getattr(Contrato, "cod_cli").isnot(None),
            cast(getattr(Contrato, "cod_cli"), String) != "",
            item_num_col.isnot(None),
            cast(item_num_col, String) != "",
        )
        .group_by(item_num_col, getattr(Contrato, "cod_cli"))
        .all()
    )
    best: dict[str, tuple[str, int]] = {}
    for r in rows:
        num = str(r.num).strip()
        cod = str(r.cod).strip()
        cnt = int(r.cnt or 0)
        cur = best.get(num)
        if cur is None or cnt > cur[1]:
            best[num] = (cod, cnt)
    return {k: v[0] for k, v in best.items()}

def popular(sess: Session, apply: bool, limit: Optional[int] = None) -> list[Change]:
    cab_num_name, cab_num_col = _pick_attr(ContratoCabecalho, "contrato_n", "contrato_num", "numero", "numero_contrato")
    item_num_name, item_num_col = _pick_attr(Contrato, "contrato_n", "contrato_num", "numero", "numero_contrato")
    if cab_num_col is None or item_num_col is None:
        raise RuntimeError("Não encontrei colunas de número de contrato nos modelos. Ajuste os nomes no script.")

    print(f"[info] Coluna de número (cabecalho): {cab_num_name}")
    print(f"[info] Coluna de número (itens):     {item_num_name}")

    melhor_por_num = coletar_melhor_codcli_por_contrato(sess, item_num_col)
    print(f"[info] Encontrados {len(melhor_por_num)} contratos com cod_cli candidato.")

    q = sess.query(ContratoCabecalho).all()
    changes: list[Change] = []
    for cab in q:
        num = getattr(cab, cab_num_name, None)
        if _is_blank(num):
            continue
        atual = getattr(cab, "cod_cli", None) if hasattr(cab, "cod_cli") else None
        if not _is_blank(atual):
            continue
        cand = melhor_por_num.get(str(num).strip())
        if not cand:
            continue
        changes.append(Change(cab_id=getattr(cab, "id"), contrato_num=str(num).strip(), antigo=atual, novo=cand))

    print(f"[info] Cabeçalhos elegíveis: {len(changes)}")

    # aplica limite
    to_apply = changes if limit is None else changes[: int(limit)]

    # backup CSV
    if to_apply:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_name = f"popular_codcli_backup_{ts}.csv"
        with open(csv_name, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["cabecalho_id", "contrato_num", "cod_cli_antigo", "cod_cli_novo"])
            for ch in to_apply:
                w.writerow([ch.cab_id, ch.contrato_num, ch.antigo or "", ch.novo])
        print(f"[ok] Backup gerado: {csv_name} ({len(to_apply)} linha(s))")

    # aplicar
    if apply and to_apply:
        for ch in to_apply:
            cab = sess.get(ContratoCabecalho, ch.cab_id)
            if not cab:
                continue
            setattr(cab, "cod_cli", ch.novo)
        sess.commit()
        print(f"[ok] Aplicado no banco: {len(to_apply)} cabeçalho(s) atualizados.")
    else:
        print("[dry-run] Nada foi gravado no banco. Use --apply para aplicar.")

    return changes

def main():
    parser = argparse.ArgumentParser(description="Popular cod_cli nos cabeçalhos a partir dos itens de contrato.")
    parser.add_argument("--apply", action="store_true", help="Aplica as alterações no banco (por padrão é dry-run).")
    parser.add_argument("--limit", type=int, default=None, help="Limita a quantidade de cabeçalhos a atualizar.")
    args = parser.parse_args()

    if SessionLocal is None:
        print("[erro] SessionLocal indisponível.", file=sys.stderr)
        sys.exit(1)

    sess: Session = SessionLocal()
    try:
        popular(sess, apply=args.apply, limit=args.limit)
    finally:
        sess.close()

if __name__ == "__main__":
    main()
