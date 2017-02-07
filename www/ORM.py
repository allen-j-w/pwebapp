#!/usr/bin/env python3
# -*-coding:utf-8 -*-
import asyncio, logging, aiomysql


def log(sql, args=()):
    logging.info('SQL: %s' % sql)


@asyncio.coroutine
def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = yield from aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw.get('user', 'root'),
        password=kw.get('password', 'root'),
        db=kw.get('db', 'pyweb'),
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


@asyncio.coroutine
def select(sql, args, size=None):
    log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        cursor = yield from conn.cursor(aiomysql.DictCursor)
        yield from cursor.execute(sql.replace('?', '%s'), args or ())
        if size:
            result = yield from cursor.fetchmany(size)
        else:
            result = yield from cursor.fetchall()
        yield from cursor.close()
        logging.log('SELECT: %s rows returned.')
        return result


@asyncio.coroutine
def update(sql, args):
    log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        try:
            cursor = yield from conn.cursor()
            yield from cursor.execute(sql.replace('?', '%s'), args)
            affected_lines = cursor.rowcount
            yield from cursor.close()
        except BaseException as e:
            raise
        return affected_lines


class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):
    def __init__(self, name=None, ddl='varchar(100)', primary_key=False, default=None):
        super.__init__(name, ddl, primary_key, default)


class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super.__init__(name, 'boolean', False, default)


class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super.__init__(name, 'bigint', primary_key, default)


class FloatField(Field):
    def __init__(self, name=None, default=0.0):
        super.__init__(name, 'real', False, default)


class TextField(Field):
    def __init__(self, name=None, default=None):
        super.__init__(name, 'text', False, default)


class ModelMetaclass(type):
    def __new__(cls, name, base, attrs):
        if name == 'Model':
            return type.__new__(cls, name, base, attrs)
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, tableName))
        mappings = dict()
        fields = []
        primary_key = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                mappings[k] = v
                logging.info('mapping found: %s-->%s' % (k, v))
                if v.primary_key:
                    if primary_key:
                        raise Exception('Duplicate primary key.')
                    primary_key = k
                else:
                    fields.append(k)
        if not primary_key:
            raise Exception('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fileds = list(map(lambda f: '`%s`', fields))
        attrs['__mappings__'] = mappings
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primary_key
        attrs['__fields__'] = fields
        attrs['__select__'] = 'select * from `%s`' % tableName
        attrs['__insert__'] = 'insert into `%s` values (%s)' % ','.join(['?']*(len(fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName,  primary_key)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primary_key)