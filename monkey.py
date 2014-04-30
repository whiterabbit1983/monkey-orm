'''
Program: Monkey ORM
Author: Dmitry A. Paramonov (c) 2014
License: GPLv2 or higher
'''

import re
import copy
import sqlite3
import collections
import datetime

class UnknownFieldProperty(Exception): pass
class NoTableDefined(Exception): pass
class NotUniquePrimaryKey(Exception): pass
class UnknownTableColumn(Exception):
    def __init__(self, fld_name):
        msg = "Unknown column name '{}'".format(fld_name)
        super(UnknownTableColumn, self).__init__(msg)

class Expr:
    def __init__(self, left, right):
        right, _ = self._escape_str([right, None])
        self.left = left
        self.right = right

    def _escape_str(self, params):
        return map(
            lambda x: "'{}'".format(x) if isinstance(x, str) else x,
            params)

class Eq(Expr):
    def __str__(self):
        return "{left} = {right}".format(
            left=self.left,
            right=self.right
            )

class Neq(Expr):
    def __str__(self):
        return "{left} != {right}".format(
            left=self.left,
            right=self.right
            )

class And(Expr):
    def __str__(self):
        return "({left}) and ({right})".format(
            left=self.left,
            right=self.right
            )

class Or(Expr):
    def __str__(self):
        return "({left}) or ({right})".format(
            left=self.left,
            right=self.right
            )

class Gt(Expr):
    def __str__(self):
        return "{left} > {right}".format(
            left=self.left,
            right=self.right
            )

class Lt(Expr):
    def __str__(self):
        return "{left} < {right}".format(
            left=self.left,
            right=self.right
            )

class Gte(Expr):
    def __str__(self):
        return "{left} >= {right}".format(
            left=self.left,
            right=self.right
            )

class Lte(Expr):
    def __str__(self):
        return "{left} <= {right}".format(
            left=self.left,
            right=self.right
            )

class In(Expr):
    def __init__(self, col, in_lst):
        self._col = col
        in_lst = self._escape_str(in_lst)
        self._in_lst = in_lst

    def __str__(self):
        return "{col} in ({lst})".format(
            col=self._col,
            lst=", ".join(self._in_lst)
            )

class Like(Expr):
    def __str__(self):
        return "{col} like {expr}".format(
            col=self.left,
            expr="%{}%".format(self.right)
            )

class Asc(Expr):
    def __init__(self, col):
        self._col = col

    def __str__(self):
        return "{} asc".format(self._col.self_name)

class Desc(Asc):
    def __str__(self):
        return "{} desc".format(self._col.self_name)

class ExprResult:
    def __init__(self, expr):
        self._expr = expr

    def __str__(self):
        return str(self._expr)

    def __and__(self, other):
        return And(self, other)
    __rand__ = __and__

    def __or__(self, other):
        return Or(self, other)
    __ror__ = __or__

class Field:
    affinity = ""

    def __init__(self, **kwargs):
        self.cond_expr = None
        self.allowed_props = {
        "unique": False,
        "autoincrement": False,
        "null": True,
        "primary_key": False,
        "default": None,
        "max_length": None,
        "date": None,
        "time": None,
        "affinity": None}
        if not set(kwargs.keys()).issubset(set(self.allowed_props.keys())):
            # TODO: make it clear what field property is wrong
            raise UnknownFieldProperty
        self.allowed_props.update(kwargs)
        if isinstance(self.allowed_props["default"], bytes):
            self.allowed_props["default"] = self.allowed_props["default"].decode("utf-8")
        if isinstance(
            self.allowed_props["default"],
            str) or isinstance(
            self.allowed_props["default"],
            datetime.datetime):
            self.allowed_props["default"] = "'{}'".format(self.allowed_props["default"])

    def __eq__(self, other):
        return ExprResult(Eq(self.self_name, other))

    def __ne__(self, other):
        return ExprResult(Neq(self.self_name, other))

    def __gt__(self, other):
        return ExprResult(Gt(self.self_name, other))

    def __lt__(self, other):
        return ExprResult(Lt(self.self_name, other))

    def __ge__(self, other):
        return ExprResult(Gte(self.self_name, other))

    def __le__(self, other):
        return ExprResult(Lte(self.self_name, other))

    def is_in(self, lst):
        return ExprResult(In(self.self_name, lst))

    def like(self, other):
        return ExprResult(Like(self.self_name, other))

    def __str__(self):
        def add_prop(s, prop):
            return "{}{} ".format(s, prop)
        allowed_props = self.allowed_props
        res = add_prop("", self.affinity)
        if allowed_props["primary_key"]:
            res = add_prop(res, "primary key")
        if allowed_props["autoincrement"]:
            res = add_prop(res, "autoincrement")
        if not allowed_props["null"]:
            res = add_prop(res, "not null")
        if allowed_props["unique"]:
            res = add_prop(res, "unique")
        if allowed_props["default"] is not None:
            res = add_prop(res, "default {}".format(allowed_props["default"]))
        return res.strip()

    def __get__(self, inst, owner):
        if inst is None:
            return self
        if not hasattr(inst, "updated"):
            inst.updated = False
        return inst.columns[self.self_name]

    def __set__(self, inst, val):
        setattr(inst, "updated", True)
        inst.columns[self.self_name] = val

class Auto(Field):
    affinity = "integer"

    def __init__(self, **kwargs):
        super(Auto, self).__init__(**kwargs)
        self.allowed_props["autoincrement"] = True
        self.allowed_props["null"] = False

class Integer(Field):
    affinity = "integer"

class Float(Field):
    affinity = "float"

class Double(Field):
    affinity = "double"

class Real(Field):
    affinity = "real"

class Text(Field):
    affinity = "text"

    def __init__(self, **kwargs):
        super(Text, self).__init__(**kwargs)
        if "max_length" in kwargs:
            self.affinity = "varchar({})".format(kwargs["max_length"])

class Date(Field):
    affinity = "datetime"

class Bool(Field):
    affinity = "bool"

class Blob(Field):
    affinity = "blob"

# raises NoTableDefined when table is not given
def type_err_to_no_table(m):
    def wrapper(self, *args, **kwargs):
        try:
            m(self, *args, **kwargs)
        except TypeError:
            raise NoTableDefined
    return wrapper

class ForeignKey(Field):
    affinity = "integer"

    @type_err_to_no_table
    def __init__(self, table_cls, **kwargs):
        super(ForeignKey, self).__init__(**kwargs)

class ManyToMany(Field):
    affinity = ""

    @type_err_to_no_table
    def __init__(self, table_cls, **kwargs):
        self._dependent_tab = table_cls
        super(ManyToMany, self).__init__(**kwargs)

    @property
    def dependent_tab_name(self):
        return self._dependent_tab.__table__

    @property
    def dependent_tab(self):
        return self._dependent_tab

class MetaTable(type):
    @classmethod
    def __prepare__(meta, name, bases):
        return collections.OrderedDict()

    def __new__(meta, newcls, bases, clsdict):
        # check for double primary keys exception
        pks = [0 for fld_inst in clsdict.values()
        if isinstance(fld_inst, Field) and fld_inst.allowed_props["primary_key"]]
        
        if len(pks) > 1:
            raise NotUniquePrimaryKey

        columns = collections.OrderedDict()
        defaults = collections.OrderedDict()
        # set __table__ attribute
        if not "__table__" in clsdict:
            clsdict["__table__"] = newcls.lower()
        for fld_name, fld_instance in clsdict.items():
            if isinstance(fld_instance, Field):
                fld_instance.self_name = fld_name
                if not isinstance(fld_instance, ManyToMany):
                    columns[fld_name] = str(fld_instance)
                    defaults[fld_name] = fld_instance.allowed_props["default"]
                else:
                    pass
                    #columns[fld_name] = Queryset(fld_instance.dependent_tab)
        clsdict["columns"] = columns
        clsdict["defaults"] = defaults
        clsdict["field_defs"] = ", ".join(["{} {}".format(fld_name, fld_def)
            for fld_name, fld_def in columns.items()
            if not isinstance(fld_def, Queryset)])

        return type.__new__(meta, newcls, bases, clsdict)

class Table(metaclass=MetaTable):
    @classmethod
    def fromtuple(cls, t):
        kwargs = dict(zip(cls.columns.keys(), t))
        return cls(**kwargs)

    def __init__(self, **kwargs):
        self.columns = collections.OrderedDict.fromkeys(
            self.__class__.columns.keys())
        # check if nonexistent col names provided
        for k in kwargs.keys():
            if not k in self.columns.keys():
                raise UnknownTableColumn(k)
        # TODO: deny to update any m2m column
        self.columns.update(self.__class__.defaults)
        self.columns.update(kwargs)
        self.updated = False

        for col_name, col_value in self.columns.items():
            setattr(self, "_{}".format(col_name), col_value)

    def values(self, with_id=False):
        # TODO: get rid of this monkey code :)
        if with_id:
            keys = self.columns.keys()
        else:
            keys =  filter(lambda x: x != "id", self.columns.keys())
        return [self.columns[k] for k in keys]

    def keys(self, with_id=False):
        if with_id:
            keys = self.columns.keys()
        else:
            keys = filter(lambda x: x != "id", self.columns.keys())
        return [k for k in keys]

class Queryset:
    def __init__(self, tab_cls, cursor=None, where=None):
        self._order_by = ""
        for attr, attr_val in locals().items():
            setattr(self, "_{}".format(attr), attr_val)

    def set_cursor(self, cursor):
        self._cursor = cursor

    def order_by(self, column):
        if isinstance(column, Field):
            self._order_by = column.self_name
        elif isinstance(column, Expr):
            self._order_by = str(column)

    def all(self):
        # TODO: implement this method as iterator
        if isinstance(self._where, ExprResult):
            self._where = "where {}".format(self._where)
        else:
            self._where = ""
        all_recs = self._cursor.execute(
            "select * from {table} {where} {order_by}".format(
                table=self._tab_cls.__table__,
                where=self._where,
                order_by=self._order_by
                ).strip()).fetchall()
        return [self._tab_cls.fromtuple(rec) for rec in all_recs]

class Store:
    def __init__(self, db_string):
        match = re.search("(.+)://(.+)", db_string)
        self.engine = match.group(1)
        self.db = db = match.group(2)
        # TODO: do abstraction to use arbitrary engine, not only sqlite
        self._conn = conn = sqlite3.connect(db)
        self._cursor = cur = conn.cursor()
        cur.execute("pragma foreign_keys = on")

    def raw(self, sql):
        return self._cursor.execute(sql).fetchall()

    __truediv__ = raw

    # TODO: finish m2m tables
    def _create_m2m_table(self, table_cls):
        for attr, attr_cls in table_cls.__dict__.items():
            if isinstance(attr_cls, ManyToMany):
                self._cursor.execute(
                    "create table if not exists {fst_tab}_{snd_tab} "
                    "({fst_tab}_id,{snd_tab}_id,"
                    "foreign key({fst_tab}_id) references {fst_tab}(id) on delete cascade,"
                    "foreign key({snd_tab}_id) references {snd_tab}(id) on delete cascade)".format(
                        fst_tab=table_cls.__table__,
                        snd_tab=attr_cls.dependent_tab_name,
                        )
                    )

    def create_table(self, table_cls):
        self._cursor.execute(
            "create table if not exists {table} ({fld_defs})".format(
                table=table_cls.__table__,
                fld_defs=table_cls.field_defs
                ))
        self._create_m2m_table(table_cls)

    # store << Table_class is the same as store.create_table(Table_class)
    __lshift__ = create_table

    def add(self, tab_inst):
        with_id = False
        if tab_inst.updated and isinstance(tab_inst.id, int):
            with_id = True
        values = tab_inst.values(with_id)
        self._cursor.execute(
            "insert or replace into {table} ({cols}) values ({values_phs})".format(
                table=tab_inst.__class__.__table__,
                cols=", ".join(tab_inst.keys(with_id)),
                values_phs=",".join(["?" for _ in values])
                )
            , tuple(values))
        if not with_id:
            tab_inst.id = tab_inst.columns["id"] = self._cursor.lastrowid
        tab_inst.updated = False

    # + operator
    __add__ = __radd__ = add

    def delete(self, tab_inst):
        self._cursor.execute("delete from {table} where id = ?".format(
            table=tab_inst.__class__.__table__
            ), (tab_inst.id,))

    # - operator
    __sub__ = delete

    def __call__(self, table_cls, where=None):
        return Queryset(table_cls, cursor=self._cursor, where=where)