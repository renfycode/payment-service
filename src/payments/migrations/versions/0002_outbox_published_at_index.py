"""Индекс по published_at для очистки опубликованных событий outbox.

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # CONCURRENTLY не блокирует запись в outbox (создание платежей) на время построения
    # индекса; такая операция не может выполняться внутри транзакции.
    with op.get_context().autocommit_block():
        op.create_index(
            "ix_outbox_published_at",
            "outbox",
            ["published_at"],
            postgresql_where=sa.text("published_at IS NOT NULL"),
            postgresql_concurrently=True,
            if_not_exists=True,
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.drop_index(
            "ix_outbox_published_at",
            table_name="outbox",
            postgresql_concurrently=True,
            if_exists=True,
        )
