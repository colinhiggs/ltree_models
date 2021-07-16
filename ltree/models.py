from sqlalchemy_utils import LtreeType, Ltree

from sqlalchemy import (
    Column,
    Text,
    Index,
    UniqueConstraint,
)

__all__ = (
    'class_factory',
)


def _repr(self):
    return f"{self.__class__.__name__}(id={self.id!r}, name={self.name!r}, path={self.path!r})"


def class_factory(base, id_type, tablename='oltree_nodes'):
    if not isinstance(id_type, Column):
        id_type = Column(id_type, primary_key=True)
    class_attrs = {
        '__tablename__': tablename,
        'id': id_type,
        'name': Column(Text, nullable=False),
        'path': Column(LtreeType, nullable=False),
        '__table_args__': (
            UniqueConstraint('path', deferrable=True, initially='immediate'),
        ),
        '__repr__': _repr,
    }

    LtreeNode = type('LtreeNode', (base, ), class_attrs)
    Index(f'{tablename}_path_idx', LtreeNode.path, postgresql_using='gist')

    return LtreeNode
