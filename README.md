# sdb
基于torndb封装的简单orm库

## 基础使用

```python
from sdb import torndb

db = torndb.Connection(
    host='127.0.0.1',
    database='test',
    user='root',
    password='root'
)
print(db.get("select * from table1 limit 1"))
print(db.query("select * from table1 limit 10"))

for i in db.iter("select * from table1 limit 1"):
    print(i)
```

## 连接池

```python
from sdb import pool

pooldb = pool.PoolDB(
    pool_size = 10,
    host='127.0.0.1',
    database='moda_demo',
    user='root',
    password='root'
).init_pool()

db = pooldb.get_db()
print(db.get("select * from table1 limit 1"))
pooldb.put_db(db)

with pool.SafeDB(pooldb) as db:
    print(db.get("select * from table1 limit 1"))
```

## ORM
```python
from sdb import orm
class Table1(orm.BaseOrm):
    __tablename__= 'table1'
    __ddl__ = """
    create table table1(a int,b int)
    """

with pool.SafeDB(pooldb) as db:
    Table1(db).select(a='1',b=12).order('id').limit(2,3).query()
    Table1(db).update(a='1',b=12).where(a='1',b=12).query()
    Table1(db).insert(a='1',b=12).execute_lastrowid()
    Table1(db).delete(a='1',b=12).execute()
    Table1(db).select().order('id').limit(2,3).echo()
    Table1(db).select().where(b=1).where2('id>%s',1).limit(2,3).query()
    Table1(db).update({'a':'1','b':12}).where(a='1',b=12).execute_rowcount()
```

## 事务控制

```python
with pool.SafeDB(pooldb) as db:
    tx = pool.transDB(db)
    tx.add(test_123().insert(a=1,b=1),'生成订单',1)
    tx.add(test_123().update(a=2,b=1).where(a=1),'更新库存',1)
    tx.add(test_456().insert(a=3,b=0),'写日志',1)
    tx.save()
```