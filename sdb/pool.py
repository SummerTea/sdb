from queue import Queue, Empty, Full
from pymysql import IntegrityError
from .torndb import Connection

class Pool(Queue):

    def __init__(self, constructor, poolsize=5):
        Queue.__init__(self, poolsize)
        self.constructor = constructor

    def get(self, block=1):
        try:
            return self.empty() and self.constructor() or Queue.get(
                self, block)
        except Empty:
            return self.constructor()

    def put(self, obj, block=1):
        try:
            return self.full() and None or Queue.put(self, obj, block)
        except Full:
            pass


class Constructor:

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __call__(self):
        return Connection(*self.args, **self.kwargs)


class PoolDB():

    def __init__(self, pool_size=5, **kwargs):
        self.pool_size = pool_size
        self.db_config = kwargs
        self._pool = None

    def init_pool(self):
        self._pool = Pool(Constructor(**self.db_config), self.pool_size)
        return self

    def get_db(self):
        return self._pool.get()

    def put_db(self, db):
        self._pool.put(db)


class RowCountErr(Exception):

    def __init__(self, addorm, cnt):
        self.value = addorm
        self.cnt = cnt

    def __str__(self):
        return "数据事务执行异常,与预计值不相等,值:%s %s,备注:%s,sql:%s %s" % (
            self.value[0], self.cnt, self.value[3], self.value[0],
            self.value[1])


class SafeDB(object):

    def __init__(self, pool: PoolDB):
        self.pool = pool
        self._exec_lst_ = []  #执行列表参数
        self._is_conn = 0  #是否获取链接
        self._db = None

    def conn(self):
        self._db = self.pool.get_db()
        self._is_conn = 1
        return self._db

    def __enter__(self):
        return self.conn()

    def __exit__(self, exc_type, exc_val, exc_tb):
        #没有出现异常则还给连接池，出现异常则关闭句柄
        if exc_type==None and exc_val==None and exc_tb==None \
                and self._db.isTrans == False:
            self.pool.put_db(self._db)
            self._is_conn = 0
        else:
            if getattr(self._db, "close", None) is not None:
                self._db.close()

    def __del__(self):
        if self._is_conn == 0 and getattr(self._db, "close", None):
            self._db.close()


class transDB:
    """模仿 sqlalchemy.save 方法,该方法默认开启事务控制
       调用方式:
       tx = transDB()
       tx.add(BaseOrm().insert(user=1,sex=1),'生成订单',1)
       tx.add(BaseOrm().update(user=2,sex=1),'更新库存',1)
       tx.add(BaseOrm().insert(user=3,sex=0),'写日志',1)
       tx.save()
    """

    def __init__(self, db):
        self.db = db
        self._exec_lst_ = []

    def add(self, orm, remark='', cnt=1):
        _c = (orm._sql, orm._para, cnt, remark)
        self._exec_lst_.append(_c)

    def save(self):
        self.db.begin()
        try:
            for i in self._exec_lst_:
                _cnt = self.db.execute_rowcount(*tuple([i[0]] + i[1]))
                if i[2] == -1:  #如果定义值为-1,则不做判断
                    pass
                elif i[2] != 0:
                    if _cnt == i[2]: pass
                    else: raise RowCountErr(i, _cnt)
            self.db.commit()
            return {'status': '1'}
        except IntegrityError as e:
            if "'PRIMARY'" in e[1]:
                self.db.rollback()
                return {
                    'status': '-1',
                    'except': 'PRIMARY',
                    'msg': '%s' % str(e),
                    'remark': i[3]
                }
            else:
                self.db.rollback()
                return {
                    'status': '-2',
                    'except': 'INTEGRITY',
                    'msg': '%s' % str(e),
                    'remark': i[3]
                }
        except RowCountErr as e:
            self.db.rollback()
            return {
                'status': '-3',
                'except': 'ROWCOUNT',
                'msg': '%s' % e,
                'remark': i[3]
            }
        # except Exception,e:
        #     self.db.rollback()
        #     return {'status':'-5','except':'Other','msg':'%s' % str(e),'remark':str(e)}
        finally:
            self._exec_lst_ = []
