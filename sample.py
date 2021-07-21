import logging
import ltree
import sys
import testing.postgresql

from itertools import islice
from sqlalchemy_utils import LtreeType, Ltree
from sqlalchemy import (
    Column, Integer, String, Text,
    Index,
    Sequence,
    create_engine,
    text,
    and_,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (
    Query as BaseQuery,
    Session,
    aliased,
)
from sqlalchemy.sql import (
    select,
    func,
)

logging.basicConfig()   # log messages to stdout
logging.getLogger('sqlalchemy.dialects.postgresql').setLevel(logging.INFO)

Base = declarative_base()

id_type = Integer
class Node(Base, ltree.OLtreeMixin):
    __tablename__ = 'oltree_nodes'
    id = Column(id_type, primary_key=True)
    name = Column(Text, nullable=False)

class LNode(Base, ltree.LtreeMixin):
    __tablename__ = 'ltree_nodes'
    id = Column(id_type, primary_key=True)
    name = Column(Text, nullable=False)


# Index(f'{Node.__tablename__}_path_idx', Node.path, postgresql_using='gist')

db = testing.postgresql.Postgresql(
    base_dir='.',
    port=8654,
)

# Required to be able to use ltree objects directly in queries and functions.
# See https://github.com/kvesteri/sqlalchemy-utils/issues/430
import psycopg2
psycopg2.extensions.register_adapter(
    Ltree, lambda ltree: psycopg2.extensions.QuotedString(str(ltree))
)

engine = create_engine(db.url(), echo=False)
ltree.add_ltree_extension(engine)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)
# ltree.add_oltree_functions(engine, max_digits=16, step_digits=8)
tree_builder = ltree.LtreeBuilder(engine, Node, max_digits=6, step_digits=3)

# tree_builder.populate(2, 3, tree_builder.path_chooser_free_path)
tree_builder.populate(2, 3)

ses = Session(engine)
q = ses.query(Node).order_by(Node.path.desc()).limit(10)
for node in reversed(q.all()):
    print(node, [str(n.path) for n in node.ancestors], node.parent_path)
ses.close()

with Session(engine) as s:
    engine.echo=True
    for node in s.execute(select(Node).where(Node.parent_path==Ltree('r'))).scalars().all():
        print(node)

    # print('*************************************************************')
    # pathlag = select(
    #     Node.path, func.lag(Node.path).over(order_by=Node.path).label('lag')
    # ).filter(
    #     func.subpath(Node.path, 0, -1) == 'r'
    # ).subquery()
    # res = s.execute(
    #     select(pathlag).filter(pathlag.c.path == Ltree('r.500000'))
    # ).all()
    # print(res)
    #
    item = s.execute(select(Node).where(Node.path==Ltree('r.500000'))).scalar_one()
    print(item)
    print(item.previous_sibling.path, item.next_sibling.path, item.parent.path)
    # item.relative_position=[item.next_sibling.path, 'r.750000.240000']
    item.previous_sibling_path=item.next_sibling.path + '__LAST__'
    s.commit()
    # n2 = aliased(Node)
    # res = s.execute(
    #     select(Node).filter(Node.previous_sibling == item)
    # ).all()
    # print(res)

tree_builder.print_tree()

with Session(engine, future=True) as s:
    seq = Sequence('path_id_seq')
    lroot = LNode(name='r', path=Ltree('r'))
    c1 = LNode(name='r.1', path=lroot.path + Ltree(str(LNode.next_path_id(s))))
    c2 = LNode(name='r.2')
    s.add(lroot)
    s.add(c1)
    s.add(c2)
    s.commit()
    res = s.execute(select(LNode).order_by(LNode.path)).scalars().all()
    for node in res:
        print(node.name, node.path)
    c2.parent_path = lroot.path
    c1.parent_path = c2.path
    s.commit()
    res = s.execute(select(LNode).order_by(LNode.path)).scalars().all()
    for node in res:
        print(node.name, node.path)

print(db.url())
input('enter to quit...')
db.stop()
