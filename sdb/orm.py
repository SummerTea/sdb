class BaseOrm:
    __tablename__ = None
    __primary__ = None
    __translKv__ = None  #接收值为 ('K','V')
    __ddl__ = None  #建表语句

    def __init__(self, db=None):
        self._sql = ''
        self._type = ''
        self._para = []
        self._sql_kv = {}
        self._db = db


    def transl(self, key, defaultval=None):
        """用于维表编码转换"""
        if self.__translKv__ == None: raise Exception("__translKv__ 没有定义")
        _v = self.select().where2('`%s`=%%s' % self.__translKv__[0], key).get()
        if _v:
            return _v[self.__translKv__[1]]
        elif defaultval:
            return defaultval
        else:
            return 'code:%s' % key

    def instsql(self, *args):
        """
        实例化SQL语法,用于自定义SQL

        eg.
        BaseOrm().instsql("select * from dual where 1=?",1).get()
        BaseOrm().instsql("select * from dual where 1=?",1).query()
        BaseOrm().instsql("select * from dual where 1=?",1).paginate()
        """
        self._sql = args[0]
        self._para = list(args[1:])
        return self

    def paginate(self, page=1, per_page=20):
        """
        分页函数
        in
        page     : 当前页数
        per_page : 每页显示记录条数

        out
        count   : 总记录条数
        data    : 当前页数据集
        page     : 当前页数
        per_page : 每页显示记录条数
        """
        _sql1 = self._sql
        _count = self.count()  #累计值
        self._sql = _sql1
        _data = self.limit((page - 1) * per_page, per_page).query()
        return {
            'total': _count,
            'data': _data,
            'page': page,
            'per_page': per_page
        }

    def select(self, params={}, **kwargs):
        self._type = 's'
        self._sql = ''
        self._para = []
        self._sql_kv = {}
        _dct = dict(params, **kwargs)
        self._sql += """select * from `%s` """ % self.__tablename__
        return self.where(params=_dct)

    def querybypk(self, pk_value):
        self._type = 's'
        self._sql = ''
        self._para = []
        if self.__primary__ == None: raise Exception("主键异常")
        self._sql += """select * from `%s` """ % self.__tablename__
        return self.where(params={self.__primary__: pk_value})

    def update(self, params={}, **kwargs):
        self._type = 'u'
        self._sql = ''
        self._para = []
        self._sql_kv = {}
        _dct = dict(params, **kwargs)
        self._sql_kv['update'] = _dct
        _lst = _dct.keys()
        self._sql += """update `%s` set """ % self.__tablename__ + ','.join(
            ["`%s`=%%s" % i for i in _lst]) + " "
        for i in _lst:
            self._para.append(_dct[i])
        return self

    def insert(self, params={}, **kwargs):
        self._type = 'i'
        self._sql = ''
        self._para = []
        self._sql_kv = {}
        _dct = dict(params, **kwargs)
        self._sql_kv['insert'] = _dct
        _lst = _dct.keys()
        self._sql += """insert into `%s`(""" % self.__tablename__ \
                   + ','.join(["`%s`" % i for i in _lst]) + ") values(" \
                   + ','.join(['%s' for i in _lst]) + ") "
        for i in _lst:
            self._para.append(_dct[i])
        return self

    def replace(self, params={}, **kwargs):
        self._type = 'r'
        self._sql = ''
        self._para = []
        self._sql_kv = {}
        if self.__primary__ == None: raise Exception("主键异常")
        _dct = dict(params, **kwargs)
        if self.__primary__ not in [i for i in _dct.keys()]:
            raise Exception("参数不存在主键")
        if self.select_by_pk(_dct[self.__primary__]).get():
            _update_dct = dict(
                [i for i in _dct.iteritems() if i[0] != self.__primary__])
            return self.update(_update_dct).where(
                params={self.__primary__: _dct[self.__primary__]})
        else:
            return self.insert(_dct)

    def delete(self, params={}, **kwargs):
        self._type = 'd'
        self._sql = ''
        self._para = []
        self._sql_kv = {}
        _dct = dict(params, **kwargs)
        self._sql += """delete from `%s` """ % self.__tablename__
        return self.where(params=_dct)

    def limit(self, *args):
        self._sql += """ limit %s """ % ','.join(['%s' for i in args])
        for i in args:
            self._para.append(i)
        return self

    def where(self, params={}, **kwargs):
        _dct = dict(params, **kwargs)
        self._sql_kv['where'] = _dct
        if len(_dct) == 0: pass
        else:
            _lst = _dct.keys()
            self._sql += " where " + ' and '.join(
                ["`%s` = %%s" % i for i in _lst]) + " "
            for i in _lst:
                self._para.append(_dct[i])
        return self

    def _chk_where(self):
        """确认条件里面是否有where条件"""
        if ' where ' in self._sql: self._sql += ' and '
        else: self._sql += ' where '

    def in_(self, col, args):
        """col   列名
           args  绑定变量值 类型为 list
        """
        self._chk_where()
        _in_k = ','.join(['%s' for i in args])
        self._sql += """ `%s` in (%s) """ % (col.lower(), _in_k)
        for i in args:
            self._para.append(i)
        return self

    def _comm_operator(self, col, args, typ=''):
        """通用表达式转换函数
           支持: ['<' ,'<=' ,'!=' ,'>=' ,'>' ,'<>']
        """
        _lst = ['<', '<=', '!=', '>=', '>', '<>']
        if typ not in _lst: raise Exception("表达式不支持")
        self._chk_where()
        _in_k = ','.join(['%s' for i in args])
        self._sql += """ `%s` %s %%s """ % (col.lower(), typ, args)
        self._para.append(i)
        return self

    def lt(self, col, args):
        """a < b """
        self._comm_operator(col, args, '<')

    def le(self, col, args):
        """a <= b """
        self._comm_operator(col, args, '<=')

    def ne(self, col, args):
        """a != b """
        self._comm_operator(col, args, '!=')

    def ge(self, col, args):
        """a >= b """
        self._comm_operator(col, args, '>=')

    def gt(self, col, args):
        """a > b """
        self._comm_operator(col, args, '>')

    def where2(self, wheresql, *args):
        self._chk_where()
        self._sql += """ %s """ % wheresql
        if len(args) == 0: pass
        else:
            for i in args:
                self._para.append(i)
        return self

    def order(self, col=''):
        _order = []
        _sql_order = ''
        for i in col.split(','):
            for j in i.split(' '):
                if j.strip().upper() in ['DESC', 'ASC']:
                    _order.append(j)
                elif j.strip() == '':
                    pass
                else:
                    _order.append(',`%s`' % j)
        _sql_order = ' '.join(_order)[1:]
        self._sql += """ order by %s """ % _sql_order
        return self

    def _before_hock(self):
        """注入消费事件
            自定义消费事件需要扩展外部方法
        """
        self.before_hock_insert()
        self.before_hock_update()
        self.before_hock_delete()

    def before_hock_insert(self):
        """给继承方法修改"""
        pass

    def before_hock_update(self):
        """给继承方法修改"""
        pass

    def before_hock_delete(self):
        """给继承方法修改"""
        pass

    def echo(self):
        _msg = "sql:%s | para:%s" % (self._sql, str(tuple(self._para)))
        print(_msg)
        return _msg

    def count(self, col=''):
        if col == '':
            _col = '*'
        else:
            _col = "`%s`" % col
        self._sql = 'select count(%s) cnt_1 from (%s) a' % (_col, self._sql)
        return self.get()['cnt_1']

    def sum(self, col):
        self._sql = 'select sum(`%s`) sum_1 from (%s) a' % (col, self._sql)
        return self.get()['sum_1']

    def max(self, col):
        self._sql = 'select max(`%s`) max_1 from (%s) a' % (col, self._sql)
        return self.get()['max_1']

    def query(self):
        return self._db.query(*tuple([self._sql] + self._para))

    def get(self):
        return self._db.get(*tuple([self._sql] + self._para))

    def execute(self):
        self._before_hock()
        return self._db.execute(*tuple([self._sql] + self._para))

    def execute_lastrowid(self):
        self._before_hock()
        return self._db.execute_lastrowid(*tuple([self._sql] + self._para))

    def execute_rowcount(self):
        self._before_hock()
        return self._db.execute_rowcount(*tuple([self._sql] + self._para))