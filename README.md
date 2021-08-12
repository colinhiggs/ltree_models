# SQLAlchemy Ltree Model Mixins

Mixin classes, utilities, and database functions for working with ltree based
trees in sqlalchemy and postgresql.

## Synopsis

```python
import ltree_models
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
class UnorderedNode(Base, ltree_models.LtreeMixin):
    __tablename__ = 'ltree_nodes'
    id = Column(Integer, primary_key=True)

class OrderedNode(Base, ltree_models.OLtreeMixin):
    __tablename__ = 'oltree_nodes'
    id = Column(Integer, primary_key=True)

# Create a new postgresql database in /tmp
db = testing.postgresql.Postgresql()
engine = create_engine(db.url(), echo=False)
ltree_models.add_ltree_extension(engine)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

# Build and print an unordered ltree
unordered_builder = ltree_models.LtreeBuilder(engine, UnorderedNode)
unordered_builder.populate(2, 3, unordered_builder.path_chooser_sequential)
with Session(engine, future=True) as s:
    unordered_builder.print_tree(s, with_name_path=True)

# Output:
# UnorderedNode(id=1, node_name='r', path=Ltree('r')) r
# UnorderedNode(id=2, node_name='r.0', path=Ltree('r.0')) r/r.0
# UnorderedNode(id=3, node_name='r.0.0', path=Ltree('r.0.0')) r/r.0/r.0.0
# UnorderedNode(id=4, node_name='r.0.1', path=Ltree('r.0.1')) r/r.0/r.0.1
# UnorderedNode(id=5, node_name='r.0.2', path=Ltree('r.0.2')) r/r.0/r.0.2
# UnorderedNode(id=6, node_name='r.1', path=Ltree('r.1')) r/r.1
# UnorderedNode(id=7, node_name='r.1.0', path=Ltree('r.1.0')) r/r.1/r.1.0
# UnorderedNode(id=8, node_name='r.1.1', path=Ltree('r.1.1')) r/r.1/r.1.1
# UnorderedNode(id=9, node_name='r.1.2', path=Ltree('r.1.2')) r/r.1/r.1.2
# UnorderedNode(id=10, node_name='r.2', path=Ltree('r.2')) r/r.2
# UnorderedNode(id=11, node_name='r.2.0', path=Ltree('r.2.0')) r/r.2/r.2.0
# UnorderedNode(id=12, node_name='r.2.1', path=Ltree('r.2.1')) r/r.2/r.2.1
# UnorderedNode(id=13, node_name='r.2.2', path=Ltree('r.2.2')) r/r.2/r.2.2

# Build and print an ordered ltree
ordered_builder = ltree_models.OLtreeBuilder(engine, OrderedNode)
ordered_builder.populate(2, 3, ordered_builder.path_chooser_balanced)
with Session(engine, future=True) as s:
    ordered_builder.print_tree(s, with_name_path=True)

# Output:
# OrderedNode(id=1, node_name='r', path=Ltree('r')) r
# OrderedNode(id=2, node_name='r.0', path=Ltree('r.2500000000000000')) r/r.0
# OrderedNode(id=3, node_name='r.0.0', path=Ltree('r.2500000000000000.2500000000000000')) r/r.0/r.0.0
# OrderedNode(id=4, node_name='r.0.1', path=Ltree('r.2500000000000000.5000000000000000')) r/r.0/r.0.1
# OrderedNode(id=5, node_name='r.0.2', path=Ltree('r.2500000000000000.7500000000000000')) r/r.0/r.0.2
# OrderedNode(id=6, node_name='r.1', path=Ltree('r.5000000000000000')) r/r.1
# OrderedNode(id=7, node_name='r.1.0', path=Ltree('r.5000000000000000.2500000000000000')) r/r.1/r.1.0
# OrderedNode(id=8, node_name='r.1.1', path=Ltree('r.5000000000000000.5000000000000000')) r/r.1/r.1.1
# OrderedNode(id=9, node_name='r.1.2', path=Ltree('r.5000000000000000.7500000000000000')) r/r.1/r.1.2
# OrderedNode(id=10, node_name='r.2', path=Ltree('r.7500000000000000')) r/r.2
# OrderedNode(id=11, node_name='r.2.0', path=Ltree('r.7500000000000000.2500000000000000')) r/r.2/r.2.0
# OrderedNode(id=12, node_name='r.2.1', path=Ltree('r.7500000000000000.5000000000000000')) r/r.2/r.2.1
# OrderedNode(id=13, node_name='r.2.2', path=Ltree('r.7500000000000000.7500000000000000')) r/r.2/r.2.2
```
