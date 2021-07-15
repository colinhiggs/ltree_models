import logging
import ltree
import sys
import testing.postgresql

from itertools import islice
from sqlalchemy_utils import LtreeType, Ltree
from sqlalchemy import (
    Column, Integer, String,
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

Node = ltree.class_factory(Base, Column(Integer, primary_key=True))

db = testing.postgresql.Postgresql(
    base_dir='.',
    port=8654,
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
for i in range(10000):
    ses.add(Node(name=str(i), path=func.oltree_free_path_rebalance('r')))
ses.commit()
ses.close()

ses = Session(engine)
q = ses.query(Node).order_by(Node.path.desc()).limit(10)
for node in reversed(q.all()):
    print(node)
ses.close()


print(db.url())
input('enter to quit...')
db.stop()
