import logging
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
# logging.basicConfig()   # log messages to stdout
# logging.getLogger('sqlalchemy.dialects.postgresql').setLevel(logging.INFO)

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

    def test_noretry_free_path_not_full(self):
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
            path_after = s.execute(func.oltree_noretry_free_path(root.path + '__LAST__')).scalar_one()
            self.assertEqual(path_after, Ltree('r.7600'))
            path_before = s.execute(func.oltree_noretry_free_path(root.path + '__FIRST__')).scalar_one()
            self.assertEqual(path_before, Ltree('r.2400'))
            path_between = s.execute(func.oltree_noretry_free_path(root.path + '5000')).scalar_one()
            self.assertEqual(path_between, Ltree('r.6250'))

    def test_noretry_free_path_full(self):
        '''
        Should rebalance and find paths when there is no space at the specified point.
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
            path_after = s.execute(func.oltree_free_path(root.path + '__LAST__')).scalar_one()
            path_before = s.execute(func.oltree_free_path(root.path + '__FIRST__')).scalar_one()
            path_between = s.execute(func.oltree_free_path(root.path + '5000')).scalar_one()
            # print(path_after, path_before, path_between)

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

    def test_free_path_full(self):
        '''
        Should rebalance to find free spaces.
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
                s.execute(func.oltree_noretry_free_path(root.path + '__LAST__')).scalar_one()
            except sqlalchemy.exc.DataError as e:
                s.rollback()
            else:
                raise Exception('Should have run out of space.')
            try:
                s.execute(func.oltree_noretry_free_path(root.path + '__FIRST__')).scalar_one()
            except sqlalchemy.exc.DataError as e:
                s.rollback()
            else:
                raise Exception('Should have run out of space.')
            try:
                s.execute(func.oltree_noretry_free_path(root.path + '5000')).scalar_one()
            except sqlalchemy.exc.DataError as e:
                s.rollback()
            else:
                raise Exception('Should have run out of space.')


@unittest.skipIf(debugging, 'debugging')
class OLtreeMixin(DBBase):
    def test_parent_path(self):
        # self.tree_builder.set_digits(4,2)
        # self.tree_builder.populate(1,2)
        with Session(self.engine, future=True) as s:
            root = Node(name='r', path=Ltree('r'))
            s.add(root)
            child = Node(name='r.1', path=Ltree('r.50'))
            grandchild = Node(name='r.1.1', path=Ltree('r.50.50'))
            other = Node(name='r.2.1', path=Ltree('r.60.50'))
            s.add(child)
            s.add(grandchild)
            s.add(other)
            s.commit()
            # print(child.parent_path, grandchild.parent_path, other.parent_path)
            self.assertEqual(child.parent_path, 'r')
            self.assertEqual(grandchild.parent_path, 'r.50')
            self.assertEqual(other.parent_path, 'r.60')

    def test_parent(self):
        with Session(self.engine, future=True) as s:
            root = Node(name='r', path=Ltree('r'))
            s.add(root)
            child = Node(name='r.1', path=Ltree('r.50'))
            grandchild = Node(name='r.1.1', path=Ltree('r.50.50'))
            other = Node(name='r.2.1', path=Ltree('r.60.50'))
            s.add(child)
            s.add(grandchild)
            s.add(other)
            s.commit()
            # print(child.parent, grandchild.parent, other.parent)
            self.assertEqual(child.parent.path, 'r')
            self.assertEqual(grandchild.parent.path, 'r.50')
            self.assertIs(other.parent, None)

    def test_children(self):
        with Session(self.engine, future=True) as s:
            root = Node(name='r', path=Ltree('r'))
            s.add(root)
            child = Node(name='r.1', path=Ltree('r.50'))
            child2 = Node(name='r.2', path=Ltree('r.70'))
            grandchild = Node(name='r.1.1', path=Ltree('r.50.50'))
            s.add(child)
            s.add(child2)
            s.add(grandchild)
            s.commit()
            self.assertEqual(
                {child.path for child in root.children},
                {'r.50', 'r.70'}
            )

    def test_previous_next_sibling(self):
        self.tree_builder.set_digits(4,2)
        self.tree_builder.populate(1,3)
        with Session(self.engine, future=True) as s:
            first = s.execute(select(Node).where(Node.path==Ltree('r.2500'))).scalar_one()
            middle = s.execute(select(Node).where(Node.path==Ltree('r.5000'))).scalar_one()
            last = s.execute(select(Node).where(Node.path==Ltree('r.7500'))).scalar_one()
            self.assertIs(first.previous_sibling, None)
            self.assertIs(middle.previous_sibling, first)
            self.assertIs(middle.next_sibling, last)
            self.assertIs(last.next_sibling, None)

    def test_previous_sibling_path_getter(self):
        self.tree_builder.set_digits(4,2)
        self.tree_builder.populate(1,3)
        with Session(self.engine, future=True) as s:
            first = s.execute(select(Node).where(Node.path==Ltree('r.2500'))).scalar_one()
            middle = s.execute(select(Node).where(Node.path==Ltree('r.5000'))).scalar_one()
            last = s.execute(select(Node).where(Node.path==Ltree('r.7500'))).scalar_one()
            self.assertIs(first.previous_sibling_path, None)
            self.assertEqual(middle.previous_sibling_path, first.path)

    def test_previous_sibling_path_setter(self):
        self.tree_builder.set_digits(4,2)
        self.tree_builder.populate(2,3)
        with Session(self.engine, future=True) as s:
            first = s.execute(select(Node).where(Node.path==Ltree('r.2500'))).scalar_one()
            middle = s.execute(select(Node).where(Node.path==Ltree('r.5000'))).scalar_one()
            last = s.execute(select(Node).where(Node.path==Ltree('r.7500'))).scalar_one()
            # reorder with first now in the middle
            first.previous_sibling_path = middle.path
            self.assertIs(first.previous_sibling, middle)
            self.assertIs(first.next_sibling, last)
            s.rollback()
            # reorder with middle now first
            middle.previous_sibling_path = middle.parent.path + '__FIRST__'
            self.assertIs(middle.previous_sibling, None)
            self.assertIs(middle.next_sibling, first)
            s.rollback()
            # reorder with middle now last
            middle.previous_sibling_path = middle.parent.path + '__LAST__'
            self.assertIs(middle.previous_sibling, last)
            self.assertIs(middle.next_sibling, None)
            s.rollback()
