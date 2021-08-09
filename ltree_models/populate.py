import ltree_models

from sqlalchemy import (
    select,
    func,
)
from sqlalchemy.orm import (
    Session,
)
from sqlalchemy_utils import (
    LtreeType,
    Ltree,
)

__all__ = (
    'LtreeBuilder',
    'OLtreeBuilder',
)


class LtreeBuilder:

    def __init__(
        self,
        engine, node_class,
    ):
        self.engine = engine
        self.Node = node_class

    def path_chooser_sequential(self, parent, i, n_children):
        return parent.path + Ltree(str(i))

    default_path_chooser = path_chooser_sequential

    def recursive_add_children(
        self, session,
        parent, depth, n_children,
        path_chooser=None
    ):
        path_chooser = path_chooser or self.default_path_chooser
        if depth <= 0:
            return
        for i in range(n_children):
            node = self.Node(
                node_name=f'{parent.node_name}.{str(i)}',
                path=path_chooser(parent, i, n_children)
            )
            session.add(node)
            session.commit()
            self.recursive_add_children(session, node, depth - 1, n_children, path_chooser=path_chooser)

    def populate(self, depth, n_children, path_chooser=None):
        path_chooser = path_chooser or self.default_path_chooser
        with Session(self.engine, future=True) as s:
            root = self.Node(node_name='r', path=Ltree('r'))
            s.add(root)
            self.recursive_add_children(s, root, depth, n_children, path_chooser)
            s.commit()

    def all_nodes(self, session=None):
        query = select(self.Node).order_by(self.Node.path)
        if session:
            return session.execute(query).scalars().all()
        with Session(self.engine, future=True) as s:
            return s.execute(query).scalars().all()

    def print_tree(self, session=None, with_name_path=False):
        for o in self.all_nodes(session=session):
            if with_name_path:
                print(o, o.name_path)
            else:
                print(o)


class OLtreeBuilder(LtreeBuilder):

    def path_chooser_balanced(self, parent, i, n_children):
        step = round(((self.max_number + 1) / (n_children + 1)))
        return parent.path + Ltree(f'{(step * (i + 1)):0{self.max_digits}d}')

    def path_chooser_free_path(self, parent, i, n_children):
        return func.oltree_free_path(parent.path + '__LAST__')

    default_path_chooser = path_chooser_balanced

    def __init__(
        self,
        engine, node_class,
        max_digits=ltree_models.DEFAULT_MAX_DIGITS,
        step_digits=ltree_models.DEFAULT_STEP_DIGITS
    ):
        super().__init__(engine, node_class)
        self.set_digits(max_digits, step_digits)

    def set_digits(
        self,
        max_digits=ltree_models.DEFAULT_MAX_DIGITS,
        step_digits=ltree_models.DEFAULT_STEP_DIGITS
    ):
        self.max_digits = max_digits
        self.max_number = 10 ** max_digits - 1
        self.step_digits = step_digits
        self.step_number = 10 ** step_digits
        ltree_models.add_oltree_functions(
            self.engine, max_digits=max_digits, step_digits=step_digits
        )
