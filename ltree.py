from sqlalchemy_utils import LtreeType, Ltree

from sqlalchemy import (
    Column,
    Text,
)


def add_ltree_extension(engine):
    engine.execute(text("CREATE EXTENSION IF NOT EXISTS ltree;"))


def add_after_function(engine, table_name, func_name='ltree_free_path'):
    engine.execute(text(
f'''
CREATE OR REPLACE FUNCTION public.{func_name}(parent ltree, after ltree DEFAULT NULL::ltree)
    RETURNS ltree
    LANGUAGE plpgsql
AS $function$
DECLARE
    parent_level int := nlevel(parent);
    next_occupied ltree := NULL::ltree;
    after_pos numeric := NULL;
    next_occupied_pos numeric := NULL;
    next_pos numeric := NULL;
    pos_big_step numeric := 1e3;
    pos_max numeric := 1e6 - 1;
BEGIN
IF parent_level != (nlevel(after)-1) OR NOT parent @> after
THEN
    RAISE EXCEPTION '% is not a child of %', after, parent;
END IF;
IF after IS NULL
THEN
    -- Passing NULL as after means after should be set to whatever the last node
    -- is, which might still be NULL if there aren't any nodes at this level yet.
    after := path from {table_name} where parent @> path and parent_level = (nlevel(path) - 1) order by path desc limit 1;
END IF;
IF after IS NULL
THEN
    -- There are no child nodes of parent. Choose half way between top of range and 0.
    RETURN parent || round(pos_max/2)::text::ltree;
END IF;
-- after should exist now.
IF after = '__START__'
THEN
    -- Passing __START__ as after means find a position before the first node.
    next_occupied := path from {table_name}
    WHERE
        parent @> path and parent_level = (nlevel(path) -1)
    ORDER BY path LIMIT 1;
    IF next_occupied IS NULL
        -- There are no child nodes of parent. Choose half way between top of range and 0.
        RETURN parent || round(pos_max/2)::text::ltree;        
    END IF;
END IF;
after_pos = subpath(after, -1)::text::numeric;
IF after_pos = pos_max
THEN
    RAISE EXCEPTION 'Out of space after %', after;
END IF;
next_occupied := path from {table_name}
WHERE
    parent @> path AND parent_level = (nlevel(path) -1) AND path > after
ORDER BY path LIMIT 1;
IF next_occupied IS NULL
THEN
    IF after_pos = pos_max - 1
    THEN
        -- special case when only one away from the top.
        next_pos = pos_max;
    ELSE
        IF after_pos + pos_big_step > pos_max
        THEN
            next_pos = round((after_pos + pos_max) / 2);
        ELSE
            next_pos = after_pos + pos_big_step;
        END IF;
    END IF;
ELSE
    next_occupied_pos := subpath(next_occupied, -1)::text::numeric;
    next_pos := round((after_pos + next_occupied_pos)/2);
    IF next_pos = after_pos OR next_pos = next_occupied_pos
    THEN
        RAISE EXCEPTION 'Out of space between % and %', after, next_occupied;
    END IF;
END IF;
RETURN parent || next_pos::text::ltree;
END;
$function$
'''
    ))



class NodeBase:
    def __repr__(self):
       return f"{self.__class__.__name__}(id={self.id!r}, name={self.name!r}, path={self._path!r})"


def class_factory(base, id_type, tablename = 'ltree_nodes'):
    if not isinstance(id_type, Column):
        id_type = Column(id_type, primary_key=True)
    class_attrs = {
        '__tablename__': tablename,
        'id': id_type,
        'name': Column(Text, nullable=False),
        '_path': Column(LtreeType, nullable=False, unique=True),
    }

    LtreeNode = type('LtreeNode', (base, NodeBase), class_attrs)

    return LtreeNode
