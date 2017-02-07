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


