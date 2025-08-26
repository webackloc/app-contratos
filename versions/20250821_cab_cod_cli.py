"""add cod_cli em contratos_cabecalho; índice + backfill

Compat: SQLite (batch), PostgreSQL e SQL Server.

- Adiciona coluna cod_cli (String, nullable) em contratos_cabecalho
- Cria índice ix_contratos_cabecalho_cod_cli
- Backfill opcional a partir de contratos.cabecalho_id (quando existir)
"""
from alembic import op
import sqlalchemy as sa

revision = "20250821_cab_cod_cli"
down_revision = "dd855b561022"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    dialect = bind.dialect.name

    # 1) coluna (SQLite-safe)
    with op.batch_alter_table("contratos_cabecalho") as batch:
        batch.add_column(sa.Column("cod_cli", sa.String(), nullable=True))

    # 2) índice
    op.create_index(
        "ix_contratos_cabecalho_cod_cli",
        "contratos_cabecalho",
        ["cod_cli"],
        unique=False,
    )

    # 3) backfill (best effort)
    try:
        if dialect in ("sqlite", "sqlite+pysqlite"):
            op.execute(
                sa.text(
                    """
                    UPDATE contratos_cabecalho
                    SET cod_cli = (
                        SELECT cod_cli
                        FROM contratos
                        WHERE contratos.cabecalho_id = contratos_cabecalho.id
                        LIMIT 1
                    )
                    WHERE cod_cli IS NULL
                    """
                )
            )
        elif dialect in ("postgresql", "postgres"):
            op.execute(
                sa.text(
                    """
                    UPDATE contratos_cabecalho cc
                    SET cod_cli = sub.cod_cli
                    FROM (
                        SELECT cabecalho_id AS id,
                               (SELECT cod_cli
                                  FROM contratos c2
                                 WHERE c2.cabecalho_id = c1.cabecalho_id
                                 LIMIT 1) AS cod_cli
                          FROM contratos c1
                         GROUP BY cabecalho_id
                    ) AS sub
                    WHERE cc.id = sub.id
                      AND cc.cod_cli IS NULL
                    """
                )
            )
        elif dialect.startswith("mssql"):
            op.execute(
                sa.text(
                    """
                    UPDATE cc
                    SET cc.cod_cli = sub.cod_cli
                    FROM contratos_cabecalho cc
                    CROSS APPLY (
                        SELECT TOP 1 c.cod_cli
                        FROM contratos c
                        WHERE c.cabecalho_id = cc.id
                    ) AS sub
                    WHERE cc.cod_cli IS NULL
                    """
                )
            )
    except Exception:
        # Se falhar, seguimos só com a coluna criada.
        pass


def downgrade():
    op.drop_index("ix_contratos_cabecalho_cod_cli", table_name="contratos_cabecalho")
    with op.batch_alter_table("contratos_cabecalho") as batch:
        batch.drop_column("cod_cli")
