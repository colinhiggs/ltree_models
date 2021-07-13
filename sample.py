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
)

engine = create_engine(db.url(), echo=False)
ltree.add_ltree_extension(engine)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)
with engine.begin() as con:
    con.execute(ltree.free_path_text(max_digits=6, step_digits=3))
    con.execute(ltree.rebalance_text(max_digits=6, step_digits=3))
ses = Session(engine)

root = Node(name='root', path=Ltree('r'))
ses.add(root)
for i in range(1000):
    try:
        ses.add(Node(name=str(i), path=func.oltree_free_path('r')))
        ses.commit()
    except Exception as e:
        ses.close()
        with engine.begin() as con:
            con.execute(text("CALL oltree_rebalance('r')"))
        ses = Session(engine)
        ses.add(Node(name=str(i), path=func.oltree_free_path('r')))
        ses.commit()
# a = Node(name='A', path=Ltree('r.001000'))
# ses.add(a)
# b = Node(name='B', path=Ltree('r.002000'))
# ses.add(b)
# ses.commit()
ses.close()
ses = Session(engine)
q = ses.query(Node).order_by(Node.path.desc()).limit(10)
for node in reversed(q.all()):
    print(node)
ses.close()


print(db.url())
input('enter to quit...')
db.stop()
