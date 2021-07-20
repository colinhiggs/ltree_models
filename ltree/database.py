from sqlalchemy import (
    text,
)

DEFAULT_PREFIX = 'oltree_'
DEFAULT_POSTFIX = None
DEFAULT_TABLE_NAME = 'nodes'
DEFAULT_MAX_DIGITS = 16
DEFAULT_STEP_DIGITS = 8

__all__ = (
    'add_ltree_extension',
    'add_oltree_functions',
    'free_path_text',
    'rebalance_text',
    'DEFAULT_PREFIX',
    'DEFAULT_POSTFIX',
    'DEFAULT_TABLE_NAME',
    'DEFAULT_MAX_DIGITS',
    'DEFAULT_STEP_DIGITS',
)


def wrap_name(base_name, prefix=DEFAULT_PREFIX, postfix=DEFAULT_POSTFIX):
    '''
    Wraps a name with a prefix and a postfix.
    '''
    if prefix is None:
        prefix = ''
    elif not prefix.endswith('_'):
        prefix = prefix + '_'

    if postfix is None:
        postfix = ''
    elif not postfix.startswith('_'):
        postfix = '_' + postfix

    return f'{prefix}{base_name}{postfix}'


def add_ltree_extension(engine):
    '''
    Add the ltree extension to the database.
    '''
    with engine.begin() as con:
        con.execute(text("CREATE EXTENSION IF NOT EXISTS ltree;"))


def noretry_free_path_parent_sibling_text(
    table_name=DEFAULT_TABLE_NAME,
    prefix=DEFAULT_PREFIX, postfix=DEFAULT_POSTFIX,
    max_digits=DEFAULT_MAX_DIGITS, step_digits=DEFAULT_STEP_DIGITS,
):
    table_name = wrap_name(table_name, prefix=prefix, postfix=postfix)
    func_name = wrap_name('noretry_free_path_parent_sibling', prefix=prefix, postfix=postfix)
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
        RAISE EXCEPTION 'Out of space after %', after
        USING ERRCODE = 'indicator_overflow';
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
        RAISE EXCEPTION 'Out of space before %', before
        USING ERRCODE = 'indicator_overflow';
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
        RAISE EXCEPTION 'Out of space between % and %', after, before
        USING ERRCODE = 'indicator_overflow';
    END IF;
END IF;
RETURN parent || to_char(next_pos, '{format_text}')::ltree;
END;
$function$
''')


def noretry_free_path_text(
    table_name=DEFAULT_TABLE_NAME,
    prefix=DEFAULT_PREFIX, postfix=DEFAULT_POSTFIX,
    max_digits=DEFAULT_MAX_DIGITS, step_digits=DEFAULT_STEP_DIGITS,
):
    table_name = wrap_name(table_name, prefix=prefix, postfix=postfix)
    func_name = wrap_name('noretry_free_path', prefix=prefix, postfix=postfix)
    format_text = 'FM' + '0' * max_digits
    return text(f'''
CREATE OR REPLACE FUNCTION public.{func_name}(after ltree)
    RETURNS ltree
    LANGUAGE plpgsql
AS $function$
DECLARE
    parent ltree := subpath(after, 0, -1);
    after_leaf ltree := NULL;
    parent_level int := nlevel(parent);
    after_pos numeric := NULL;
    before ltree := NULL::ltree;
    before_pos numeric := NULL;
    next_pos numeric := NULL;
    big_step_pos numeric := 1e{step_digits};
    max_pos numeric := 1e{max_digits} - 1;
    found_parent ltree := NULL::ltree;
    -- To be used if it is decided to treat non existant after node as an error.
    -- found_after ltree := NULL::ltree;
BEGIN
IF parent_level < 1 THEN
    RAISE EXCEPTION
    '"%" is not a child node: can''t assign as sibling of root.', after;
END IF;
found_parent := path from {table_name} WHERE path = parent;
IF found_parent IS NULL THEN
    RAISE EXCEPTION 'parent "%" does not exist.', parent;
END IF;
after_leaf := subpath(after, -1);
IF after_leaf = '__LAST__' THEN
    -- Passing __LAST__ as after means after should be set to whatever the last node
    -- is, which might be NULL if there aren't any nodes at this level yet.
    -- after := NULL;
    after := path from {table_name}
    WHERE parent @> path AND parent_level = (nlevel(path) - 1)
    ORDER BY path DESC
    LIMIT 1;
ELSEIF after_leaf = '__FIRST__' THEN
    -- Passing __FIRST__ as after means find a position before the first node.
    before := path from {table_name}
    WHERE parent @> path AND parent_level = (nlevel(path) - 1)
    ORDER BY path
    LIMIT 1;
    after := NULL;
ELSE
    -- Make sure after has the correct number of digits so that lexical sorting
    -- works.
    after_pos := subpath(after, -1)::text::numeric;
    after := parent || to_char(after_pos, '{format_text}')::ltree;
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
        RAISE EXCEPTION 'Out of space after %', after
        USING ERRCODE = 'indicator_overflow';
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
        RAISE EXCEPTION 'Out of space before %', before
        USING ERRCODE = 'indicator_overflow';
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
        RAISE EXCEPTION 'Out of space between % and %', after, before
        USING ERRCODE = 'indicator_overflow';
    END IF;
END IF;
RETURN parent || to_char(next_pos, '{format_text}')::ltree;
END;
$function$
''')


def rebalance_text(
    table_name=DEFAULT_TABLE_NAME,
    prefix=DEFAULT_PREFIX, postfix=DEFAULT_POSTFIX,
    max_digits=DEFAULT_MAX_DIGITS, step_digits=DEFAULT_STEP_DIGITS,
):
    table_name = wrap_name(table_name, prefix=prefix, postfix=postfix)
    func_name = wrap_name('rebalance', prefix=prefix, postfix=postfix)
    format_text = 'FM' + '0' * max_digits
    return text(f'''
CREATE OR REPLACE PROCEDURE public.{func_name}(parent ltree)
    LANGUAGE plpgsql
AS $procedure$
DECLARE
    parent_level int := nlevel(parent);
    max_pos numeric := 1e{max_digits} - 1;
    step numeric;
    n_children numeric := 1;
BEGIN
n_children := COUNT(*) FROM {table_name}
    WHERE parent @> path and parent_level = nlevel(path) - 1;
step := ((max_pos + 1) / (n_children + 1));
RAISE NOTICE 'children % / %, step: %', n_children, (max_pos), step;
IF step <= 1.0::numeric THEN
    RAISE EXCEPTION 'out of space rebalancing %', parent
    USING ERRCODE = 'indicator_overflow';
END IF;
WITH ordinals AS (
    SELECT
        row_number() OVER (ORDER BY path) as row,
        path
    FROM {table_name}
    WHERE parent @> path and parent_level = nlevel(path) - 1
)
UPDATE {table_name}
SET
    path = parent || to_char(
        round(ordinals.row * step), '{format_text}'
    )::ltree
FROM ordinals
WHERE oltree_nodes.path = ordinals.path;
END;
$procedure$
''')


def free_path_parent_sibling_text(
    table_name=DEFAULT_TABLE_NAME,
    prefix=DEFAULT_PREFIX, postfix=DEFAULT_POSTFIX,
    max_digits=DEFAULT_MAX_DIGITS, step_digits=DEFAULT_STEP_DIGITS,
):
    table_name = wrap_name(table_name, prefix=prefix, postfix=postfix)
    func_name = wrap_name('free_path_parent_sibling', prefix=prefix, postfix=postfix)
    rebalance_name = wrap_name('rebalance', prefix=prefix, postfix=postfix)
    free_path_name = wrap_name('noretry_free_path_parent_sibling', prefix=prefix, postfix=postfix)
    format_text = 'FM' + '0' * max_digits
    return text(f'''
CREATE OR REPLACE FUNCTION public.{func_name}(parent ltree, after ltree DEFAULT NULL::ltree)
    RETURNS ltree
    LANGUAGE plpgsql
AS $function$
BEGIN
RETURN {free_path_name}(parent, after);
EXCEPTION
    WHEN indicator_overflow THEN
        RAISE NOTICE 'rebalancing %', parent;
        CALL {rebalance_name}(parent);
        RETURN {free_path_name}(parent, after);
END;
$function$
''')


def free_path_text(
    table_name=DEFAULT_TABLE_NAME,
    prefix=DEFAULT_PREFIX, postfix=DEFAULT_POSTFIX,
    max_digits=DEFAULT_MAX_DIGITS, step_digits=DEFAULT_STEP_DIGITS,
):
    table_name = wrap_name(table_name, prefix=prefix, postfix=postfix)
    func_name = wrap_name('free_path', prefix=prefix, postfix=postfix)
    rebalance_name = wrap_name('rebalance', prefix=prefix, postfix=postfix)
    free_path_name = wrap_name('noretry_free_path', prefix=prefix, postfix=postfix)
    format_text = 'FM' + '0' * max_digits
    return text(f'''
CREATE OR REPLACE FUNCTION public.{func_name}(after ltree)
    RETURNS ltree
    LANGUAGE plpgsql
AS $function$
BEGIN
RETURN {free_path_name}(after);
EXCEPTION
    WHEN indicator_overflow THEN
        RAISE NOTICE 'rebalancing %', subpath(after, 0, -1);
        CALL {rebalance_name}(subpath(after, 0, -1));
        RETURN {free_path_name}(after);
END;
$function$
''')


def add_oltree_functions(
    engine,
    table_name=DEFAULT_TABLE_NAME,
    prefix=DEFAULT_PREFIX, postfix=DEFAULT_POSTFIX,
    max_digits=DEFAULT_MAX_DIGITS, step_digits=DEFAULT_STEP_DIGITS,
):
    fnames = (
        'rebalance',
        'noretry_free_path',
        'free_path',
        'noretry_free_path_parent_sibling',
        'free_path_parent_sibling',
    )

    for fname in fnames:
        with engine.begin() as con:
            con.execute(
                globals()[f'{fname}_text'](
                    table_name=table_name,
                    prefix=prefix,
                    postfix=postfix,
                    max_digits=max_digits,
                    step_digits=step_digits
                )
            )
