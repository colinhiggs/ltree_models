import ltree
import sys
import testing.postgresql

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
from statistics import mean

Base = declarative_base()

Node = ltree.class_factory(Base, Column(Integer, primary_key=True))

db = testing.postgresql.Postgresql(
    base_dir='.',
)

def half_way(left, right):
    left = list(map(int, left.split('_')))
    right = list(map(int, right.split('_')))
    print(left, right)
    avg = list(map(mean, zip(left, right)))
    print(avg)


engine = create_engine(db.url(), echo=True)
ltree.add_ltree_extension(engine)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)
ses = Session(engine)

top = Node(name='Top', _path=Ltree('001000'))
ses.add(top)
a = Node(name='A', _path=Ltree('001000.001000'))
ses.add(a)
b = Node(name='B', _path=Ltree('001000.002000'))
ses.add(b)

ses.commit()

q = ses.query(Node)

for node in q.all():
    print(node)

db.stop()
