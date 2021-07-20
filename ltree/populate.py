import ltree

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
)


class LtreeBuilder:

    def __init__(
        self,
        engine, node_class,
        max_digits=ltree.DEFAULT_MAX_DIGITS,
        step_digits=ltree.DEFAULT_STEP_DIGITS
    ):
        self.engine = engine
        self.Node = node_class
        self.set_digits(max_digits, step_digits)

    def set_digits(
        self,
        max_digits=ltree.DEFAULT_MAX_DIGITS,
        step_digits=ltree.DEFAULT_STEP_DIGITS
    ):
        self.max_digits = max_digits
        self.max_number = 10 ** max_digits - 1
        self.step_digits = step_digits
        self.step_number = 10 ** step_digits
        ltree.add_oltree_functions(
            self.engine, max_digits=max_digits, step_digits=step_digits
        )

    def recursive_add_children(
        self, session,
        parent, depth, n_children,
        path_chooser=None
    ):
        path_chooser = path_chooser or self.path_chooser_balanced
        if depth <= 0:
            return
        for i in range(n_children):
            node = self.Node(
                name=f'{parent.name}.{str(i)}',
                path=path_chooser(parent, i, n_children)
            )
            session.add(node)
            session.commit()
            self.recursive_add_children(session, node, depth - 1, n_children, path_chooser=path_chooser)

    def populate(self, depth, n_children, path_chooser=None):
        path_chooser = path_chooser or self.path_chooser_balanced
        with Session(self.engine, future=True) as s:
            root = self.Node(name='r', path=Ltree('r'))
            s.add(root)
            self.recursive_add_children(s, root, depth, n_children, path_chooser)
            s.commit()

    def all_nodes(self):
        with Session(self.engine, future=True) as s:
            return s.execute(select(self.Node).order_by(self.Node.path)).scalars().all()

    def print_tree(self):
        for o in self.all_nodes():
            print(o)

    def path_chooser_balanced(self, parent, i, n_children):
        step = round(((self.max_number + 1) / (n_children + 1)))
        return parent.path + Ltree(f'{(step * (i + 1)):0{self.max_digits}d}')

    def path_chooser_free_path(self, parent, i, n_children):
        return func.oltree_free_path(parent.path + '__LAST__')

    def path_chooser_sequential(self, parent, i, n_children):
        return parent.path + Ltree(str(i))
