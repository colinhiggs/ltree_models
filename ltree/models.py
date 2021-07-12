from sqlalchemy_utils import LtreeType, Ltree

from sqlalchemy import (
    Column,
    Text,
)

__all__ = (
    'class_factory',
)

class NodeBase:
    def __repr__(self):
       return f"{self.__class__.__name__}(id={self.id!r}, name={self.name!r}, path={self.path!r})"


def class_factory(base, id_type, tablename = 'oltree_nodes'):
    if not isinstance(id_type, Column):
        id_type = Column(id_type, primary_key=True)
    class_attrs = {
        '__tablename__': tablename,
        'id': id_type,
        'name': Column(Text, nullable=False),
        'path': Column(LtreeType, nullable=False, unique=True),
    }

    LtreeNode = type('LtreeNode', (base, NodeBase), class_attrs)

    return LtreeNode
