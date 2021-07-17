import logging
import ltree
import sys
import testing.postgresql

from itertools import islice
from sqlalchemy_utils import LtreeType, Ltree
from sqlalchemy import (
    Column, Integer, String, Text,
    Index,
    create_engine,
    text,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (
    Query as BaseQuery,
    Session
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

Index(f'{Node.__tablename__}_path_idx', Node.path, postgresql_using='gist')

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
ltree.add_oltree_functions(engine, max_digits=16, step_digits=8)

# print(db.url())
# input('enter to quit...')

ses = Session(engine)
root = Node(name='root', path=Ltree('r'))
ses.add(root)
ses.commit()
for i in range(10):
    ses.add(Node(name=str(i), path=func.oltree_free_path_rebalance(root.path, '__FIRST__')))
ses.commit()
ses.close()

ses = Session(engine)
q = ses.query(Node).order_by(Node.path.desc()).limit(10)
for node in reversed(q.all()):
    print(node, node.parent_path)
ses.close()


print(db.url())
input('enter to quit...')
db.stop()
