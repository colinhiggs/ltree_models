import ltree
import os
import psycopg2
import sqlalchemy
import testing.postgresql
import unittest

from sqlalchemy import (
    create_engine,
    Column,
    engine,
    Index,
    Integer,
    text,
    Text,
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

id_type = Integer
Base = declarative_base()
class Node(Base, ltree.OLtreeMixin):
    __tablename__ = 'oltree_nodes'
    id = Column(id_type, primary_key=True)
    name = Column(Text, nullable=False)
Node.add_path_index()

# drops tables with cascade
@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"


def setUpModule():
    '''Create a test DB and import data.'''
    # Create a new database somewhere in /tmp
    global db
    global engine

    db = testing.postgresql.Postgresql()
    engine = create_engine(db.url(), future=True)
    ltree.add_ltree_extension(engine)

    # Node = ltree.class_factory(Base, Integer)


def tearDownModule():
    '''Throw away test DB.'''
    global db
    db.stop()


class DBBase(unittest.TestCase):
    def setUp(self):
        global db
        global engine
        self.prefix = 'oltree'
        self.Node = Node
        self.engine = engine
        Base.metadata.create_all(engine)
        self.tree_builder = ltree.LtreeBuilder(
            engine, Node, max_digits=6, step_digits=3
        )

    def tearDown(self):
        Base.metadata.drop_all(self.engine)


@unittest.skipUnless(debugging, 'Not debugging')
class Debugging(DBBase):

    def test_print_tree(self):
        self.set_digits(4,2)
        self.populate(2, 3, balanced_paths)
        self.print_tree()

@unittest.skipIf(debugging, 'debugging')
class DBFunctions(DBBase):

    def test_free_path_not_full(self):
        '''
        Should successfully find paths when there is space.
        '''
        self.tree_builder.set_digits(4,2)
        self.tree_builder.populate(1,3)
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


    def test_free_path_full(self):
        '''
        Should fail to find paths when there is no space at the specified point.
        '''
        self.tree_builder.set_digits(4,2)
        self.tree_builder.populate(1,3)
        with Session(self.engine, future=True) as s:
            root = s.execute(
                select(self.Node).where(self.Node.path==Ltree('r'))
            ).scalar_one()
            # Deliberately create nodes with no adjacent spaces.
            s.add(self.Node(name='r.last', path=Ltree('r.9999')))
            s.add(self.Node(name='r.first', path=Ltree('r.0000')))
            s.add(self.Node(name='r.1_plus_1', path=Ltree('r.5001')))
            s.commit()
            try:
                s.execute(func.oltree_free_path(root.path)).scalar_one()
            except sqlalchemy.exc.DataError as e:
                s.rollback()
            else:
                raise Exception('Should have run out of space.')
            try:
                s.execute(func.oltree_free_path(root.path, '__FIRST__')).scalar_one()
            except sqlalchemy.exc.DataError as e:
                s.rollback()
            else:
                raise Exception('Should have run out of space.')
            try:
                s.execute(func.oltree_free_path(root.path, 'r.5000')).scalar_one()
            except sqlalchemy.exc.DataError as e:
                s.rollback()
            else:
                raise Exception('Should have run out of space.')


    def test_rebalance_not_full(self):
        '''
        Should spread out ordinals which were originally sequential.
        '''
        self.tree_builder.set_digits(2,1)
        # populate with sequential ordinals (0,1,2).
        self.tree_builder.populate(1,3, path_chooser=self.tree_builder.path_chooser_sequential)
        with Session(self.engine, future=True) as s:
            # Rebalance.
            s.execute(text("CALL oltree_rebalance(:path)"), {'path': 'r'})
            s.commit()
        self.assertEqual([str(o.path) for o in self.tree_builder.all_nodes()], ['r', 'r.25', 'r.50', 'r.75'])

    def test_rebalance_full(self):
        '''
        Should result in a sqlalchemy.exc.DataError when rebalancing full branch.
        '''
        self.tree_builder.set_digits(2,1)
        # Fill the tree and try to rebalance. Should result in an error.
        self.tree_builder.populate(1,100, path_chooser=self.tree_builder.path_chooser_sequential)
        with Session(self.engine, future=True) as s:
            try:
                s.execute(text("CALL oltree_rebalance(:path)"), {'path': 'r'})
            except sqlalchemy.exc.DataError as e:
                s.rollback()
            else:
                raise Exception('Should have run out of space.')
