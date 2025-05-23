"""refactor FeedbackStorage column names

Revision ID: 49448641ee38
Revises: 297a29427d44
Create Date: 2025-05-13 23:00:25.690981

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '49448641ee38'
down_revision: Union[str, None] = '297a29427d44'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('FeedbackStorage',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('date', sa.Text(), nullable=True),
    sa.Column('shift', sa.Text(), nullable=True),
    sa.Column('floor', sa.Text(), nullable=True),
    sa.Column('game', sa.Text(), nullable=True),
    sa.Column('gp_name_surname', sa.Text(), nullable=True),
    sa.Column('sm_name_surname', sa.Text(), nullable=True),
    sa.Column('reason', sa.Text(), nullable=True),
    sa.Column('total', sa.Integer(), nullable=True),
    sa.Column('proof', sa.Text(), nullable=True),
    sa.Column('explanation_of_the_reason', sa.Text(), nullable=True),
    sa.Column('action_taken', sa.Text(), nullable=True),
    sa.Column('forwarded_feedback', sa.Text(), nullable=True),
    sa.Column('comment_after_forwarding', sa.Text(), nullable=True),
    sa.PrimaryKeyConstraint('id')
    )
    op.drop_table('feedbackstorage')
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('feedbackstorage',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('date', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('shift', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('floor', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('game', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('gp_name_surname', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('sm_name_surname', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('reason', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('total', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('proof', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('explanation_of_the_reason', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('action_taken', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('forwarded_feedback', sa.TEXT(), autoincrement=False, nullable=True),
    sa.Column('comment_after_forwarding', sa.TEXT(), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('id', name='feedbackstorage_pkey')
    )
    op.drop_table('FeedbackStorage')
    # ### end Alembic commands ###
