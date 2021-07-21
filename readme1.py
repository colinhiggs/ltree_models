import ltree
import testing.postgresql
from sqlalchemy import (
  Column,
  create_engine,
  Integer,
  Text,
)
from sqlalchemy.ext.declarative import (
    declarative_base
)
from sqlalchemy.orm import (
    Session,
)

Base = declarative_base()
class UnorderedNode(Base, ltree.LtreeMixin):
    __tablename__ = 'ltree_nodes'
    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)

class OrderedNode(Base, ltree.OLtreeMixin):
    __tablename__ = 'oltree_nodes'
    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)

# Create a new postgresql database in /tmp
db = testing.postgresql.Postgresql()
engine = create_engine(db.url(), echo=False)
ltree.add_ltree_extension(engine)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)
unordered_builder = ltree.LtreeBuilder(engine, UnorderedNode)
unordered_builder.populate(2, 3, unordered_builder.path_chooser_sequential)
unordered_builder.print_tree()
