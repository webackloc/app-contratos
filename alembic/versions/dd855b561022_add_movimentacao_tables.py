"""add movimentacao tables + cols em contratos_logs; ajustar nullables
Compatível com SQLite (batch), Postgres e SQL Server.
v3 (2025-08-20): criação idempotente (só cria se não existir) de tabelas/índices/FK/colunas.
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers
revision: str = "dd855b561022"
down_revision = "20250814_mov_cols_and_indexes"
branch_labels = None
depends_on = None


def _table_exists(inspector, name: str) -> bool:
    try:
        return name in inspector.get_table_names()
    except Exception:
        return False


def _index_exists(inspector, table: str, index_name: str) -> bool:
    try:
        return any(ix.get("name") == index_name for ix in inspector.get_indexes(table))
    except Exception:
        return False


def _column_exists(inspector, table: str, col: str) -> bool:
    try:
        return any(c.get("name") == col for c in inspector.get_columns(table))
    except Exception:
        return False


def _fk_exists(inspector, table: str, fk_name: str) -> bool:
    try:
        return any(fk.get("name") == fk_name for fk in inspector.get_foreign_keys(table))
    except Exception:
        return False


def upgrade() -> None:
    bind = op.get_bind()
    insp = sa.inspect(bind)

    # ------------------------------
    # Tabela movimentacao_lotes (idempotente)
    # ------------------------------
    if not _table_exists(insp, "movimentacao_lotes"):
        op.create_table(
            "movimentacao_lotes",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("criado_em", sa.DateTime(), nullable=False),
            sa.Column("status", sa.String(length=32), nullable=False),  # ABERTO | PROCESSADO | PROCESSADO_COM_ERROS
            sa.Column("arquivo", sa.String(), nullable=True),
            sa.Column("total_itens", sa.Integer(), nullable=True),
            sa.Column("processado_em", sa.DateTime(), nullable=True),
        )
    if not _index_exists(insp, "movimentacao_lotes", "ix_mov_lotes_status"):
        op.create_index("ix_mov_lotes_status", "movimentacao_lotes", ["status"], unique=False)
    if not _index_exists(insp, "movimentacao_lotes", "ix_mov_lotes_criado_em"):
        op.create_index("ix_mov_lotes_criado_em", "movimentacao_lotes", ["criado_em"], unique=False)

    # ------------------------------
    # Tabela movimentacao_itens (idempotente)
    # ------------------------------
    if not _table_exists(insp, "movimentacao_itens"):
        op.create_table(
            "movimentacao_itens",
            sa.Column("id", sa.Integer(), primary_key=True),
            sa.Column("lote_id", sa.Integer(), sa.ForeignKey("movimentacao_lotes.id"), nullable=False),
            sa.Column("linha_idx", sa.Integer(), nullable=False),
            sa.Column("payload", sa.JSON(), nullable=True),
            sa.Column("erro_msg", sa.String(), nullable=True),
            sa.Column("criado_em", sa.DateTime(), nullable=False),
        )
    if not _index_exists(insp, "movimentacao_itens", "ix_mov_itens_lote_idx"):
        op.create_index("ix_mov_itens_lote_idx", "movimentacao_itens", ["lote_id", "linha_idx"], unique=False)

    # -------------------------------------------------
    # contratos_logs: novas colunas + FK cabecalho (idempotente)
    # -------------------------------------------------
    cols_to_add = []
    for name, col in (
        ("contrato_cabecalho_id", sa.Column("contrato_cabecalho_id", sa.Integer(), nullable=True)),
        ("cod_cli",               sa.Column("cod_cli", sa.String(), nullable=True)),
        ("ativo",                 sa.Column("ativo", sa.String(), nullable=True)),
        ("tp_transacao",          sa.Column("tp_transacao", sa.String(), nullable=True)),
        ("data_mov",              sa.Column("data_mov", sa.Date(), nullable=True)),
        ("status",                sa.Column("status", sa.String(length=16), nullable=True)),
        ("mensagem",              sa.Column("mensagem", sa.String(), nullable=True)),
    ):
        if not _column_exists(insp, "contratos_logs", name):
            cols_to_add.append(col)

    if cols_to_add or not _fk_exists(insp, "contratos_logs", "fk_contratos_logs_cabecalho"):
        with op.batch_alter_table("contratos_logs", recreate="auto") as batch:
            for col in cols_to_add:
                batch.add_column(col)
            if not _fk_exists(insp, "contratos_logs", "fk_contratos_logs_cabecalho"):
                batch.create_foreign_key(
                    "fk_contratos_logs_cabecalho",
                    "contratos_cabecalho",
                    ["contrato_cabecalho_id"],
                    ["id"],
                )

    # garantir índice em contratos_logs.mov_hash
    if not _index_exists(insp, "contratos_logs", "ix_contratos_logs_mov_hash"):
        try:
            op.create_index("ix_contratos_logs_mov_hash", "contratos_logs", ["mov_hash"], unique=False)
        except Exception:
            pass  # se outro nome existir, deixamos como está

    # -------------------------------------------------
    # Ajustes de NULLABLE (podemos executar mesmo se já estiverem nulos)
    # -------------------------------------------------
    with op.batch_alter_table("contratos", recreate="auto") as batch:
        batch.alter_column("serial", existing_type=sa.String(), nullable=True)
        batch.alter_column("contrato_n", existing_type=sa.String(), nullable=True)
        batch.alter_column("valor_mensal", existing_type=sa.Float(), nullable=True)
        batch.alter_column("meses_restantes", existing_type=sa.Integer(), nullable=True)
        batch.alter_column("valor_global_contrato", existing_type=sa.Float(), nullable=True)
        batch.alter_column("valor_presente_contrato", existing_type=sa.Float(), nullable=True)

    with op.batch_alter_table("contratos_logs", recreate="auto") as batch:
        batch.alter_column("contrato_id", existing_type=sa.Integer(), nullable=True)
        batch.alter_column("acao", existing_type=sa.String(), nullable=True)
        batch.alter_column("descricao", existing_type=sa.String(), nullable=True)


def downgrade() -> None:
    # Reverter ajustes de NULLABLE
    with op.batch_alter_table("contratos_logs", recreate="auto") as batch:
        batch.alter_column("descricao", existing_type=sa.String(), nullable=False)
        batch.alter_column("acao", existing_type=sa.String(), nullable=False)
        batch.alter_column("contrato_id", existing_type=sa.Integer(), nullable=False)

    with op.batch_alter_table("contratos", recreate="auto") as batch:
        batch.alter_column("valor_presente_contrato", existing_type=sa.Float(), nullable=False)
        batch.alter_column("valor_global_contrato", existing_type=sa.Float(), nullable=False)
        batch.alter_column("meses_restantes", existing_type=sa.Integer(), nullable=False)
        batch.alter_column("valor_mensal", existing_type=sa.Float(), nullable=False)
        batch.alter_column("contrato_n", existing_type=sa.String(), nullable=False)
        batch.alter_column("serial", existing_type=sa.String(), nullable=False)

    # contratos_logs: remover colunas/FK adicionadas nesta revisão (se existirem)
    with op.batch_alter_table("contratos_logs", recreate="auto") as batch:
        try:
            batch.drop_constraint("fk_contratos_logs_cabecalho", type_="foreignkey")
        except Exception:
            pass
        for col in ("mensagem", "status", "data_mov", "tp_transacao", "ativo", "cod_cli", "contrato_cabecalho_id"):
            try:
                batch.drop_column(col)
            except Exception:
                pass

    # Tabelas de movimentação (remover índices e tabelas, se existirem)
    try:
        op.drop_index("ix_mov_itens_lote_idx", table_name="movimentacao_itens")
    except Exception:
        pass
    try:
        op.drop_table("movimentacao_itens")
    except Exception:
        pass

    try:
        op.drop_index("ix_mov_lotes_criado_em", table_name="movimentacao_lotes")
    except Exception:
        pass
    try:
        op.drop_index("ix_mov_lotes_status", table_name="movimentacao_lotes")
    except Exception:
        pass
    try:
        op.drop_table("movimentacao_lotes")
    except Exception:
        pass

    # (não mexemos no índice ix_contratos_logs_mov_hash no downgrade, pois pode ser de revisão anterior)
