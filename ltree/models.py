from sqlalchemy_utils import LtreeType, Ltree

from sqlalchemy import (
    Column,
    Text,
    Index,
    UniqueConstraint,
    func,
    select,
)
from sqlalchemy.orm import (
    column_property,
    declared_attr,
)
from sqlalchemy.ext.hybrid import (
    hybrid_property,
)

__all__ = (
    'OLtreeMixin',
)


class OLtreeMixin:
    path = Column(LtreeType, nullable=False)
    __table_args__ = (
        UniqueConstraint('path', deferrable=True, initially='immediate'),
    )

    @declared_attr
    def parent(self):
        return column_property(func.subpath(self.path, 0, -1))

    def __repr__(self):
        return f"{self.__class__.__name__}(id={self.id!r}, name={self.name!r}, path={self.path!r})"  # pylint: disable=no-member
