from sqlalchemy import (
    text,
)


__all__ = (
    'add_ltree_extension',
    'free_path_text',
    'add_free_path_function',
)


def add_ltree_extension(engine):
    with engine.begin() as con:
        con.execute(text("CREATE EXTENSION IF NOT EXISTS ltree;"))


def free_path_text(
    table_name='oltree_nodes', func_name='oltree_free_path',
    max_digits=64, step_digits=48,
    ):
    format_text = 'FM' + '0' * max_digits
    return text(f'''
CREATE OR REPLACE FUNCTION public.{func_name}(parent ltree, after ltree DEFAULT NULL::ltree)
    RETURNS ltree
    LANGUAGE plpgsql
AS $function$
DECLARE
    parent_level int := nlevel(parent);
    after_pos numeric := NULL;
    before ltree := NULL::ltree;
    before_pos numeric := NULL;
    next_pos numeric := NULL;
    big_step_pos numeric := 1e{step_digits};
    max_pos numeric := 1e{max_digits} - 1;
BEGIN
IF NOT (
    after IS NULL OR after = '__LAST__' OR after = '__FIRST__' OR
    ( parent_level = (nlevel(after)-1) AND parent @> after )
) THEN
    RAISE EXCEPTION '% is not a child of %', after, parent;
END IF;
IF after IS NULL OR after = '__LAST__' THEN
    -- Passing NULL as after means after should be set to whatever the last node
    -- is, which might still be NULL if there aren't any nodes at this level yet.
    IF after = '__LAST__' THEN
        after := NULL;
    END IF;
    after := path from {table_name}
    WHERE parent @> path AND parent_level = (nlevel(path) - 1)
    ORDER BY path DESC
    LIMIT 1;
ELSEIF after = '__FIRST__' THEN
    -- Passing __FIRST__ as after means find a position before the first node.
    before := path from {table_name}
    WHERE parent @> path AND parent_level = (nlevel(path) - 1)
    ORDER BY path
    LIMIT 1;
    after := NULL;
ELSE
    -- Make sure after has the correct number of digits so that lexical sorting
    -- works.
    -- after := parent || to_char(after_pos, '{format_text}')::ltree;
    before := path from {table_name}
    WHERE parent @> path AND parent_level = (nlevel(path) - 1) AND path > after
    ORDER BY path
    LIMIT 1;
END IF;
-- RAISE NOTICE 'after: %, before: %', after, before;
IF after IS NOT NULL THEN
    after_pos := subpath(after, -1)::text::numeric;
END IF;
IF after IS NULL AND before IS NULL THEN
    -- There are no child nodes of parent. Choose half way between top of range and 0.
    next_pos := round(max_pos/2);
ELSEIF before IS NULL THEN
    -- RAISE NOTICE 'looking for a spot after %', after;
    -- RAISE NOTICE 'after_pos: %', after_pos;
    -- Find a spot after after.
    IF after_pos = max_pos THEN
        RAISE EXCEPTION 'Out of space after %', after;
    ELSEIF after_pos = max_pos - 1 THEN
        -- special case when only one away from the top.
        next_pos = max_pos;
    ELSE
        IF after_pos + big_step_pos > round((after_pos + max_pos) / 2) THEN
            next_pos := round((after_pos + max_pos) / 2);
        ELSE
            next_pos := after_pos + big_step_pos;
        END IF;
    END IF;
ELSEIF after IS NULL THEN
    -- Find a spot before before.
    before_pos = subpath(before, -1)::text::numeric;
    IF before_pos = 0 THEN
        RAISE EXCEPTION 'Out of space before %', before;
    ELSEIF before_pos = 1 THEN
        -- special case when only one space left at the bottom.
        next_pos = 0;
    ELSE
        IF before_pos - big_step_pos < round(before_pos / 2) THEN
            next_pos := round(before_pos / 2);
        ELSE
            next_pos := before_pos - big_step_pos;
        END IF;
    END IF;
ELSE
    -- Find a spot between after and before.
    before_pos := subpath(before, -1)::text::numeric;
    next_pos := round((after_pos + before_pos)/2);
    IF next_pos = after_pos OR next_pos = before_pos THEN
        RAISE EXCEPTION 'Out of space between % and %', after, before;
    END IF;
END IF;
RETURN parent || to_char(next_pos, '{format_text}')::ltree;
END;
$function$
''')


def add_free_path_function(
    engine,
    table_name='oltree_nodes', func_name='oltree_free_path',
    max_digits=64, step_digits=48,
    ):
    with engine.begin() as con:
        con.execute(
            free_path_text(table_name, func_name, max_digits, step_digits)
        )
