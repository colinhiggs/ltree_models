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
ses = Session(engine)

root = Node(name='root', path=Ltree('r'))
ses.add(root)
for i in range(510):
    ses.add(Node(name=str(i), path=func.oltree_free_path('r')))
# a = Node(name='A', path=Ltree('r.001000'))
# ses.add(a)
# b = Node(name='B', path=Ltree('r.002000'))
# ses.add(b)
ses.commit()
q = ses.query(Node).order_by(Node.path.desc()).limit(10)
for node in reversed(q.all()):
    print(node)
ses.close()

with engine.begin() as con:
    res = con.execute(text(
"""
WITH ordinals AS (
    SELECT
        row_number() OVER (ORDER BY path) as row,
        path,
        subpath(path,-1)::text::numeric as index
    FROM oltree_nodes
    WHERE 'r' @> path and path != 'r'
), max_ordinal AS (
    SELECT max(row) from ordinals
)
-- SELECT row, index, 'r' || to_char(round(ordinals.row * (1000000 / (max_ordinal.max+1))), 'FM000000')::ltree FROM ordinals, max_ordinal
-- ORDER BY index DESC LIMIT 10
UPDATE oltree_nodes
SET path = 'r' || to_char(round(ordinals.row * (1000000 / (max_ordinal.max+1))), 'FM000000')::ltree
FROM ordinals, max_ordinal
WHERE oltree_nodes.path = ordinals.path
RETURNING oltree_nodes.id, oltree_nodes.path, ordinals.path
"""
    ))
    for row in islice(res, 0, 10):
        print(row)

ses = Session(engine)
q = ses.query(Node).order_by(Node.path.desc()).limit(10)
for node in reversed(q.all()):
    print(node)


print(db.url())
# input('enter to quit...')
db.stop()
