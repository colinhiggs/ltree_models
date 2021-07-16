import ltree
import os
import psycopg2
import sqlalchemy
import testing.postgresql
import unittest

from sqlalchemy import (
    create_engine,
    engine,
    Integer,
    text,
    select,
    func,
)
from sqlalchemy_utils import (
    LtreeType,
    Ltree,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.compiler import compiles #data migrations tool used with SQLAlchemy to make database schema changes
from sqlalchemy.orm import (
    Session,
    sessionmaker,
)
from sqlalchemy.schema import DropTable
from sqlalchemy.sql.functions import GenericFunction

debugging = os.environ.get('DEBUGGING')

# Required to be able to use ltree objects directly in queries and functions.
# See https://github.com/kvesteri/sqlalchemy-utils/issues/430
import psycopg2
psycopg2.extensions.register_adapter(
    Ltree, lambda ltree: psycopg2.extensions.QuotedString(str(ltree))
)

# drops tables with cascade
@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"


def balanced_paths(self, parent, i, n_children):
    step = round(((self.max_number + 1) / (n_children + 1)))
    return parent.path + Ltree(f'{(step * (i + 1)):0{self.max_digits}d}')


def free_path_rebalance_paths(self, parent, i, n_children):
    return func.oltree_free_path_rebalance(parent.path)


# class oltree_free_path_rebalance(GenericFunction):
#     type = Ltree


def setUpModule():
    '''Create a test DB and import data.'''
    # Create a new database somewhere in /tmp
    global db
    global engine
    global Base
    global Node
    db = testing.postgresql.Postgresql()
    engine = create_engine(db.url(), future=True)
    ltree.add_ltree_extension(engine)
    Base = declarative_base()
    Node = ltree.class_factory(Base, Integer)


def tearDownModule():
    '''Throw away test DB.'''
    global db
    db.stop()


class DBBase(unittest.TestCase):
    def setUp(self):
        global db
        global engine
        global Base
        global Node
        self.prefix = 'oltree'
        self.Node = Node
        self.engine = engine
        Base.metadata.create_all(engine)
        self.set_digits()

    def tearDown(self):
        global Base
        # global engine
        Base.metadata.drop_all(self.engine)

    def set_digits(self,
        max_digits=ltree.DEFAULT_MAX_DIGITS,
        step_digits=ltree.DEFAULT_STEP_DIGITS
        ):
        self.max_digits = max_digits
        self.max_number = 10 ** max_digits - 1
        self.step_digits = step_digits
        self.step_number = 10 ** step_digits
        ltree.add_oltree_functions(
            engine, max_digits=max_digits, step_digits=step_digits
        )

    def recursive_add_children(
        self, session,
        parent, depth, n_children,
        path_chooser=balanced_paths
        ):
        if depth <= 0:
            return
        for i in range(n_children):
            node = self.Node(
                name=f'{parent.name}.{str(i)}',
                path=path_chooser(self, parent, i, n_children)
            )
            session.add(node)
            self.recursive_add_children(session, node, depth - 1, n_children, path_chooser=path_chooser)

    def populate(self, depth, n_children, path_chooser=balanced_paths):
        with Session(self.engine, future=True) as s:
            root = self.Node(name='root', path=Ltree('r'))
            s.add(root)
            self.recursive_add_children(s, root, depth, n_children, path_chooser)
            s.commit()

    def print_tree(self):
        with Session(self.engine, future=True) as s:
            for o in s.execute(select(self.Node).order_by(self.Node.path)).scalars().all():
                print(o)


@unittest.skipUnless(debugging, 'Not debugging')
class Debugging(DBBase):

    def test_print_tree(self):
        self.set_digits(4,2)
        self.populate(2, 3, balanced_paths)
        self.print_tree()

@unittest.skipIf(debugging, 'debugging')
class DBFunctions(DBBase):

    def test_free_path(self):
        self.set_digits(4,2)
        self.populate(1,3)
        with Session(self.engine, future=True) as s:
            root = s.execute(
                select(self.Node).where(self.Node.path==Ltree('r'))
            ).scalar_one()
            children = s.execute(
                select(self.Node).where(func.nlevel(self.Node.path)==2)
            ).scalars().all()
            path_after = s.execute(func.oltree_free_path(root.path)).scalar_one()
            self.assertEqual(path_after, Ltree('r.7600'))
            path_before = s.execute(func.oltree_free_path(root.path, '__FIRST__')).scalar_one()
            self.assertEqual(path_before, Ltree('r.2400'))
            path_between = s.execute(func.oltree_free_path(root.path, 'r.5000')).scalar_one()
            self.assertEqual(path_between, Ltree('r.6250'))
            # Deliberately create nodes with no adjacent spaces.
            s.add(self.Node(name='r.last', path=Ltree('r.9999')))
            s.add(self.Node(name='r.first', path=Ltree('r.0000')))
            s.add(self.Node(name='r.1_plus_1', path=Ltree('r.5001')))
            s.commit()
            try:
                s.execute(func.oltree_free_path(root.path)).scalar_one()
            except sqlalchemy.exc.DataError as e:
                s.rollback()
            try:
                s.execute(func.oltree_free_path(root.path, '__FIRST__')).scalar_one()
            except sqlalchemy.exc.DataError as e:
                s.rollback()
            try:
                s.execute(func.oltree_free_path(root.path, 'r.5000')).scalar_one()
            except sqlalchemy.exc.DataError as e:
                s.rollback()

    def test_rebalance(self):
        self.set_digits(4,2)
        self.populate(1,3)
