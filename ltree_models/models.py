import sqlalchemy

from sqlalchemy_utils import LtreeType, Ltree
from sqlalchemy import (
    and_,
    BigInteger,
    bindparam,
    case,
    column,
    Column,
    Text,
    Index,
    UniqueConstraint,
    func,
    select,
    Sequence,
    text,
    type_coerce,
    update,
)
from sqlalchemy.orm import (
    aliased,
    column_property,
    declarative_mixin,
    declared_attr,
    foreign,
    object_session,
    relationship,
    remote,
    Session,
)
from sqlalchemy.ext.hybrid import (
    hybrid_property,
)

__all__ = (
    'LtreeMixin',
    'OLtreeMixin',
)


def subpath(path, offset, length=None):
    path = str(path)
    return Ltree('.'.join(path.split('.')[offset:length]))


@declarative_mixin
class Common:

    name_path_sep = '/'

    _path_id = Column(BigInteger, Sequence('path_id_seq'))

    @staticmethod
    def next_path_id(session):
        seq = Sequence('path_id_seq')
        return session.execute(seq.next_value()).scalar_one()

    @declared_attr
    def path(cls):  # pylint: disable=no-self-argument
        # seq = Sequence('path_id_seq')
        return Column(
            LtreeType, nullable=False,
            server_default=text(
                "'newborn'::ltree || nextval('path_id_seq')::text::ltree"
            )
        )

    @hybrid_property
    def parent_path(self):
        elements = str(self.path).split('.')[:-1]
        if elements:
            return Ltree('.'.join(elements))
        else:
            return None
        # Or, if we wanted to get the database to do this for absolute
        # consistency:
        #
        # s = object_session(self)
        # return s.execute(select(func.subpath(self.path, 0, -1))).scalar_one()

    @parent_path.expression
    def parent_path(cls):  # pylint: disable=no-self-argument
        return func.subpath(cls.path, 0, -1)

    @declared_attr
    def node_name(cls):  # pylint: disable=no-self-argument
        return Column(Text, nullable=False)

    @hybrid_property
    def name_path(self):
        name_list = [node.node_name for node in self.ancestors]
        name_list.append(self.node_name)
        return self.name_path_sep.join(name_list)

    @declared_attr
    def parent(cls):  # pylint: disable=no-self-argument
        return relationship(
            cls,
            primaryjoin=lambda: remote(cls.path) == func.subpath(foreign(cls.path), 0, -1),
            backref='children',
            viewonly=True,
        )

    @declared_attr
    def ancestors(cls):  # pylint: disable=no-self-argument
        return relationship(
            cls,
            primaryjoin=lambda: and_(
                remote(cls.path).op('@>', is_comparison=True)(foreign(cls.path)),
                func.nlevel(remote(cls.path)) < func.nlevel(foreign(cls.path))
            ),
            order_by=lambda: cls.path,
            uselist=True,
            viewonly=True
        )

    def set_new_path(self, new_path):
        cls = self.__class__
        s = object_session(self)
        # with Session(object_session(self).get_bind(), future=True) as s:
        s.execute(
            update(
                cls
            ).where(
                cls.path.op('<@', is_comparison=True)(self.path)
            ).values(
                path=case(
                    (
                        cls.path == self.path,
                        new_path
                    ),
                    (
                        cls.path != self.path,
                        # new_path + func.subpath(cls.path, func.nlevel(self.path))
                        text(":new_path || subpath(path, nlevel(:current_path))")
                    )
                )
            ).execution_options(
                synchronize_session='fetch'
            ),
            params={'new_path': new_path, 'current_path': self.path}
        )

    def __repr__(self):
        return f"{self.__class__.__name__}(id={self.id!r}, node_name={self.node_name!r}, path={self.path!r})"  # pylint: disable=no-member


@declarative_mixin
class LtreeMixin(Common):

    @declared_attr
    def __table_args__(cls):  # pylint: disable=no-self-argument
        return (
            UniqueConstraint('path', deferrable=True, initially='immediate'),
        )

    @Common.parent_path.setter  # pylint: disable=no-member
    def parent_path(self, value):
        self.set_new_path(Ltree(value) + subpath(self.path, -1))


@declarative_mixin
class OLtreeMixin(Common):

    @declared_attr
    def __table_args__(cls):  # pylint: disable=no-self-argument
        return (
            Index(f'{cls.__tablename__}_path_idx', cls.path, postgresql_using='gist'),
            UniqueConstraint('path', deferrable=True, initially='immediate'),
        )

    @hybrid_property
    def previous_sibling(self):
        cls = self.__class__
        cls2 = aliased(cls)
        s = object_session(self)
        pl = select(
            cls.path, func.lag(cls.path).over(order_by=cls.path).label('lag')
        ).where(
            func.subpath(cls.path, 0, -1) == func.subpath(self.path, 0, -1)
        ).subquery()
        return s.execute(
            select(cls2).join(pl, cls2.path == pl.c.lag).where(pl.c.path == self.path)
        ).scalar_one_or_none()

    # @previous_sibling.expression
    # def previous_sibling(cls):
    #     cls2 = aliased(cls)
    #     cls_tmp = aliased(cls)
    #     pl = select(
    #         cls_tmp.path, func.lag(cls_tmp.path).over(order_by=cls_tmp.path).label('lag')
    #     ).where(
    #         func.subpath(cls_tmp.path, 0, -1) == func.subpath(cls.path, 0, -1)
    #         # True
    #     ).subquery()
    #     return (
    #         select(cls2).join(pl, cls2.path == pl.c.lag).where(pl.c.path == cls.path)
    #     )

    @hybrid_property
    def previous_sibling_path(self):
        prev = self.previous_sibling
        return prev.path if prev else None  # pylint: disable=no-member

    @previous_sibling_path.setter
    def previous_sibling_path(self, value):
        cls = self.__class__
        previous_sibling_path = Ltree(value)
        s = object_session(self)
        new_path = s.execute(
            func.oltree_free_path(previous_sibling_path)
        ).scalar_one()
        self.set_new_path(new_path)

    @hybrid_property
    def next_sibling(self):
        cls = self.__class__
        cls2 = aliased(cls)
        s = object_session(self)
        pl = select(
            cls.path, func.lead(cls.path).over(order_by=cls.path).label('lead')
        ).where(
            func.subpath(cls.path, 0, -1) == func.subpath(self.path, 0, -1)
        ).subquery()
        return s.execute(
            select(cls2).join(pl, cls2.path == pl.c.lead).where(pl.c.path == self.path)
        ).scalar_one_or_none()
