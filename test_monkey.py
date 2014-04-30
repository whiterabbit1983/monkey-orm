import sqlite3
import unittest
from monkey import *
from datetime import datetime

class Table2(Table):
    id = Auto(primary_key=True)
    title = Text()

class Table3(Table):
    id = Auto(primary_key=True)

# sample table classes
class Table1(Table):
    id = Auto(primary_key=True)
    text_field = Text()
    varchar_field = Text(max_length=100)
    date_field = Date()
    m2m_field = ManyToMany(Table2)
    bool_field = Bool()
    foreign_key = ForeignKey(Table3)
    int_field = Integer()
    float_field = Float()
    double_field = Double()
    real_field = Real()
    blob_field = Blob()

class Table4(Table):
    id = Auto()

class Table5(Table):
    __table__ = "my_table"
    id = Auto(primary_key=True)
    text_field = Text()
    varchar_field = Text(max_length=100)
    date_field = Date()
    bool_field = Bool()
    int_field = Integer()
    float_field = Float()
    blob_field = Blob()

class OrmSqliteTest(unittest.TestCase):
    def setUp(self):
        self.store = store = Store("sqlite://:memory:")
        self.cur = store._cursor

    def test_store_db_and_engine_string(self):
        s = Store("sqlite://:memory:")
        self.assertEqual(s.engine, "sqlite")
        self.assertEqual(s.db, ":memory:")

    def test_record_attrs(self):
        class Tab(Table):
            id = Auto(primary_key=True)
            name = Text()

        t = Tab(id=1,name="name")
        self.assertTrue(hasattr(t, "id") and hasattr(t, "name"))
        self.assertEqual(t.id, 1)
        self.assertEqual(t.name, "name")

    def test_foreign_keys_on(self):
        fks = self.cur.execute("pragma foreign_keys").fetchall()
        self.assertTrue((1,) in fks)

    def test_table_has_field_defs_method(self):
        class Tab(Table):
            id = Auto(primary_key=True)

        self.assertTrue(hasattr(Tab, "field_defs"))

    def test_table_has_columns_attr(self):
        class Tab(Table):
            id = Auto(primary_key=True)

        self.assertTrue(hasattr(Tab, "columns"))

    def test_create_table(self):
        self.store.create_table(Table3)
        self.store.create_table(Table2)
        self.store.create_table(Table1)
        self.store.create_table(Table5)
        tables = self.cur.execute("select name from sqlite_master").fetchall()

        self.assertIn(("table3",), tables)
        self.assertIn(("table2",), tables)
        self.assertIn(("table1",), tables)
        self.assertIn(("table1_table2",), tables)
        self.assertIn(("my_table",), tables)

    def test_create_table_lshift(self):
        class Tab(Table):
            id = Auto(primary_key=True)

        self.store << Tab
        tables = self.cur.execute("select name from sqlite_master").fetchall()
        self.assertIn(("tab",), tables)

    def test_auto_create_m2m_table(self):
        self.store.create_table(Table1)
        tables = self.cur.execute("select name from sqlite_master").fetchall()
        self.assertIn(("table1",), tables)
        self.assertIn(("table1_table2",), tables)

    def test_no_m2m_column_names(self):
        class Tab2(Table):
            id = Auto(primary_key=True)

        class Tab(Table):
            id = Auto(primary_key=True)
            m2m = ManyToMany(Tab2)

        self.assertFalse("m2m" in Tab.field_defs)

    def test_table_field_defs(self):
        self.assertEqual(
            Table1.field_defs,
            "id integer primary key autoincrement not null, "
            "text_field text, varchar_field varchar(100), "
            "date_field datetime, bool_field bool, "
            "foreign_key integer, int_field integer, "
            "float_field float, double_field double, "
            "real_field real, blob_field blob")

    def test_table_field_types(self):
        self.store.create_table(Table1)
        tab1_info = self.cur.execute("pragma table_info(table1)").fetchall()
        # col info
        # (n, field_name, field_type, not_null, default, primary_key)
        self.assertEqual(
            tab1_info,
            [
            (0, "id", "integer", 1, None, 1),
            (1, "text_field", "text", 0, None, 0),
            (2, "varchar_field", "varchar(100)", 0, None, 0),
            (3, "date_field", "datetime", 0, None, 0),
            (4, "bool_field", "bool", 0, None, 0),
            (5, "foreign_key", "integer", 0, None, 0),
            (6, "int_field", "integer", 0, None, 0),
            (7, "float_field", "float", 0, None, 0),
            (8, "double_field", "double", 0, None, 0),
            (9, "real_field", "real", 0, None, 0),
            (10, "blob_field", "blob", 0, None, 0)
            ])

    def test_table_field_types_simple(self):
        class Tab(Table):
            id = Auto(primary_key=True)
            name = Text()
            num = Integer()

        self.store.create_table(Tab)
        tab1_info = self.cur.execute("pragma table_info(tab)").fetchall()
        # col info
        # (n, field_name, field_type, not_null, default, primary_key)
        self.assertEqual(
            tab1_info,
            [
            (0, "id", "integer", 1, None, 1),
            (1, "name", "text", 0, None, 0),
            (2, "num", "integer", 0, None, 0),
            ])

    @unittest.skip
    def test_table_foreign_key_list(self):
        self.store.create_table(Table1)
        fk_list = self.cur.execute("pragma foreign_key_list(table1)").fetchall()
        # fk list info
        # ()
        self.assertEqual(
            fk_list,
            [(0, 0, 'table3', 'foreign_key', 'id', 'NO ACTION', 'NO ACTION', 'NONE')])

    @unittest.skip
    def test_m2m_table_columns(self):
        self.store.create_table(Table2)
        self.store.create_table(Table1)
        m2m_cols = self.cur.execute("pragma table_info(table1_table2)").fetchall()
        self.assertEqual(
            m2m_cols,
            [
            (0, "table1_id", "integer", 1, None, 1),
            (1, "table2_id", "integer", 1, None, 1),
            ])

    def test_store_add_no_values_no_defaults(self):
        rec1 = Table1()
        self.store.create_table(Table1)
        self.store.add(rec1)
        t = self.cur.execute("select * from table1").fetchall()
        self.assertEqual(
            t,
            [(1,None,None,None,None,None,None,None,None,None,None,)])

    def test_table_column_value(self):
        class Tab(Table):
            id = Auto(primary_key=True)
            name = Text()

        t = Tab(name="name 1")
        self.assertEqual(t.columns["name"], "name 1")

    def test_two_records_have_different_attrs(self):
        class Tab(Table):
            id = Auto(primary_key=True)
            name = Text()
        rec1 = Tab(name="name 1")
        rec2 = Tab(name="name 2")
        self.assertNotEqual(rec1.name, rec2.name)

    def test_store_add_vals_no_def(self):
        '''
        Use this test to show
        '''
        class Tab(Table):
            id = Auto(primary_key=True)
            name = Text()
        self.store.create_table(Tab)
        rec1 = Tab(name="name 1")
        rec2 = Tab(name="name 2")
        self.store.add(rec1)
        self.store.add(rec2)
        t = self.cur.execute("select * from tab").fetchall()
        self.assertEqual(
            t,
            [(1,"name 1"),(2,"name 2")])

    def test_store_add_values_no_defaults(self):
        rec1 = Table1(
            text_field="text",
            varchar_field="vc",
            date_field=datetime(1999, 1, 22, 1, 12, 9),
            bool_field=True,
            foreign_key=1,
            int_field=0,
            float_field=0.1,
            double_field=0.2,
            real_field=0.3,
            blob_field=b"x")
        self.store.create_table(Table1)
        self.store.add(rec1)
        t = self.cur.execute("select * from table1").fetchall()
        self.assertEqual(
            t,
            [(1,"text","vc","1999-01-22 01:12:09",True,1,0,0.1,0.2,0.3,b"x")])

    @unittest.skip
    def test_store_add_no_values_defaults(self):
        class NewTab(Table):
            id = Auto(primary_key=True)
            text_field = Text(default="text")
            varchar_field = Text(max_length=100, default="vc")
            date_field = Date(default=datetime(2001,1,27,12,13,59))
            m2m_field = ManyToMany(Table2, default=1)
            bool_field = Bool(default=True)
            foreign_key = ForeignKey(Table3, default=1)
            int_field = Integer(default=0)
            float_field = Float(default=0.1)
            double_field = Double(default=0.2)
            real_field = Real(default=0.3)
            blob_field = Blob(default=b"x")

        rec1 = NewTab()
        self.store.create_table(NewTab)
        self.store.add(rec1)
        t = self.cur.execute("select * from newtab").fetchall()
        self.assertEqual(
            t,
            [(1,"text","vc","2001-01-27 12:13:59",True,1,0,0.1,0.2,0.3,b"x")])

    def test_store_add_values_defaults(self):
        class NewTab(Table):
            id = Auto(primary_key=True)
            text_field = Text(default="text")
            varchar_field = Text(max_length=100, default="vc")
            date_field = Date(default=datetime(2001,1,27,12,13,59))
            m2m_field = ManyToMany(Table2, default=1)
            bool_field = Bool(default=True)
            foreign_key = ForeignKey(Table3, default=1)
            int_field = Integer(default=0)
            float_field = Float(default=0.1)
            double_field = Double(default=0.2)
            real_field = Real(default=0.3)
            blob_field = Blob(default=b"x")

        rec1 = NewTab(
            text_field="txt",
            varchar_field="varchar",
            date_field=datetime(1999,1,13,12,12,12),
            bool_field=False,
            foreign_key=2,
            int_field=12,
            float_field=1.1,
            double_field=1.2,
            real_field=1.3,
            blob_field=b"blob")

        self.store.create_table(NewTab)
        self.store.add(rec1)
        t = self.cur.execute("select * from newtab").fetchall()
        self.assertEqual(
            t,
            [(1,"txt","varchar","1999-01-13 12:12:12",False,2,12,1.1,1.2,1.3,b"blob")])

    def test_store_fetch_all(self):
        self.store.create_table(Table2)
        self.store.add(Table2(title="rec1"))
        self.store.add(Table2(title="rec2"))
        all_recs = self.store(Table2).all()
        self.assertEqual(len(all_recs), 2)
        first = all_recs[0]
        second = all_recs[1]
        self.assertEqual(first.id, 1)
        self.assertEqual(first.title, "rec1")
        self.assertEqual(second.id, 2)
        self.assertEqual(second.title, "rec2")

    def test_store_fetch_all_filter(self):
        self.store.create_table(Table2)
        self.store.add(Table2(title="rec1"))
        self.store.add(Table2(title="rec2"))
        self.store.add(Table2(title="rec2"))
        
        all_recs = self.store(Table2, Table2.title == "rec2").all()
        self.assertEqual(len(all_recs), 2)
        first = all_recs[0]
        second = all_recs[1]
        self.assertEqual(first.id, 2)
        self.assertEqual(first.title, "rec2")
        self.assertEqual(second.id, 3)
        self.assertEqual(second.title, "rec2")

    def test_record_updated(self):
        class Tab(Table):
            id = Auto(primary_key=True)
            name = Text()

        t = Tab(name = "name 1")
        self.assertFalse(t.updated)
        t.name = "name 2"
        self.assertTrue(t.updated)

    def test_record_id_set_correctly(self):
        class Tab(Table):
            id = Auto(primary_key=True)
            name = Text()

        t = Tab(name = "name 1")
        self.store.create_table(Tab)
        self.store.add(t)
        self.assertEqual(t.id, 1)

    def test_store_update(self):
        class Tab(Table):
            id = Auto(primary_key=True)
            name = Text()

        self.store.create_table(Tab)
        rec = Tab(name="name 1")
        self.store.add(rec)
        self.assertEqual(self.store.raw("select name from tab where id = 1"), [("name 1",)])
        rec.name = "name 2"
        self.store.add(rec)
        self.assertEqual(self.store.raw("select name from tab where id = 1"), [("name 2",)])

    @unittest.skip
    def test_store_update_group(self):
        self.store.add(Table2(title="rec1"))
        self.store.add(Table2(title="rec2"))
        self.store.add(Table2(title="rec2"))
        rec = self.store(Table2, Table2.title == "rec2").all()[0]
        rec.title = "newrec2"
        self.store.add(rec)
        r = self.cur.execute("select * from table2 where id = 2").fetchall()
        self.assertEqual(r, [(2, "newrec2",)])

    @unittest.skip
    def test_store_delete_group(self):
        self.store.add(Table2(title="rec1"))
        self.store.add(Table2(title="rec2"))
        self.store.add(Table2(title="rec2"))
        recs = self.store(Table2, Table2.title == "rec2").all()
        recs.delete()
        recs = self.cur.execute("select * from table2").fetchall()
        self.assertEqual(recs, [(1, "rec1",)])

    def test_store_results_are_queryset(self):
        self.store.create_table(Table2)
        self.store.add(Table2(title="rec1"))
        self.store.add(Table2(title="rec2"))
        recs = self.store(Table2)
        self.assertTrue(isinstance(recs, Queryset))

    @unittest.skip
    def test_foreign_key_set(self):
        class NewTab(Table):
            id = Auto(primary_key=True)
            title = ForeignKey(AnotherTab)

        class AnotherTab(Table):
            id = Auto(primary_key=True)
            title = Text()

        t1 = AnotherTab(title="title 1")
        nt1 = NewTab(title=t1)
        nt2 = NewTab(title=t1)

        for r in [t1,nt1,nt2]:
            self.store.add(r)
        rec = self.store(NewTab).one()
        self.assertEqual(rec.id, 1)
        self.assertEqual(rec.title.id, 1)
        self.assertEqual(rec.title.title, "title 1")

        rec = self.store(AnotherTab).one()
        first, second = rec.newtab_set.all()
        self.assertEqual(first.id, 1)
        self.assertEqual(second.id, 2)

    @unittest.skip
    def test_m2m_set(self):
        class NewTab(Table):
            id = Auto(primary_key=True)
            text = Text()
            m2m = ManyToMany(AnotherTab)

        class AnotherTab(Table):
            id = Auto(primary_key=True)
            another_text = Text()

        nt1 = NewTab(text="text 1")
        nt2 = NewTab(text="text 2")
        at1 = AnotherTab(another_text="another_text 1")        
        at2 = AnotherTab(another_text="another_text 2")

        nt1.m2m.add(at1, at2)
        at1.newtab_set.add(nt1,nt2)

        for r in [nt1,at1]:
            self.store.add(r)

        first, second = self.store(NewTab).all()
        self.assertEqual(first.id, 1)
        self.assertEqual(second.id, 2)
        self.assertEqual(first.text, "text 1")
        self.assertEqual(second.text, "text 2")
        self.assertEqual(len(second.m2m.all()), 0)
        first_at, second_at = first.m2m.all()
        self.assertEqual(first_at.id, 1)
        self.assertEqual(second_at.id, 2)
        self.assertEqual(first_at.another_text, "another_text 1")
        self.assertEqual(second_at.another_text, "another_text 2")

        first, second = self.store(AnotherTab).all()
        self.assertEqual(first.id, 1)
        self.assertEqual(second.id, 2)
        self.assertEqual(first.another_text, "another_text 1")
        self.assertEqual(second.another_text, "another_text 2")
        self.assertEqual(len(second.newtab_set.all()), 0)
        first_nt, second_nt = first.newtab_set.all()
        self.assertEqual(first_nt.id, 1)
        self.assertEqual(second_nt.id, 2)
        self.assertEqual(first_nt.text, "text 1")
        self.assertEqual(second_nt.text, "text 2")

class TableInstancesTest(unittest.TestCase):
    def test_m2m_field_null_by_default(self):
        class AnotherTable(Table):
            id = Auto(primary_key=True)
            text = Text()

        class NewTable(Table):
            id = Auto(primary_key=True)
            m2m = ManyToMany(AnotherTable)

        raised = False
        try:
            rec = NewTable()
        except FieldValueNotSet:
            raised = True

        self.assertFalse(raised)

    def test_foreignkey_field_null_by_default(self):
        class AnotherTable(Table):
            id = Auto(primary_key=True)
            text = Text()

        class NewTable(Table):
            id = Auto(primary_key=True)
            fk = ForeignKey(AnotherTable)

        raised = False
        try:
            rec = NewTable()
        except FieldValueNotSet:
            raised = True
        
        self.assertFalse(raised)

    def test_text_null_field_not_set(self):
        class NewTable(Table):
            id = Auto(primary_key=True)
            text = Text(null=True)

        raised = False
        try:
            rec = NewTable()
        except FieldValueNotSet:
            raised = True

        self.assertFalse(raised)

    def test_text_not_null_field_not_set(self):
        class NewTable(Table):
            id = Auto(primary_key=True)
            text = Text()

        raised = False
        try:
            rec = NewTable()
        except FieldValueNotSet:
            raised = True

        self.assertTrue(raised)

    def test_varchar_null_field_not_set(self):
        class NewTable(Table):
            id = Auto(primary_key=True)
            text = Text(max_length=10, null=True)

        raised = False
        try:
            rec = NewTable()
        except FieldValueNotSet:
            raised = True

        self.assertFalse(raised)

    def test_varchar_not_null_field_not_set(self):
        class NewTable(Table):
            id = Auto(primary_key=True)
            text = Text(max_length=10)

        raised = False
        try:
            rec = NewTable()
        except FieldValueNotSet:
            raised = True

        self.assertTrue(raised)

    def test_int_null_field_not_set(self):
        class NewTable(Table):
            id = Auto(primary_key=True)
            text = Integer(null=True)

        raised = False
        try:
            rec = NewTable()
        except FieldValueNotSet:
            raised = True

        self.assertFalse(raised)

    def test_int_not_null_field_not_set(self):
        class NewTable(Table):
            id = Auto(primary_key=True)
            text = Integer()

        raised = False
        try:
            rec = NewTable()
        except FieldValueNotSet:
            raised = True

        self.assertTrue(raised)

    def test_bool_null_field_not_set(self):
        class NewTable(Table):
            id = Auto(primary_key=True)
            text = Bool(null=True)

        raised = False
        try:
            rec = NewTable()
        except FieldValueNotSet:
            raised = True

        self.assertFalse(raised)

    def test_bool_not_null_field_not_set(self):
        class NewTable(Table):
            id = Auto(primary_key=True)
            text = Bool()

        raised = False
        try:
            rec = NewTable()
        except FieldValueNotSet:
            raised = True

        self.assertTrue(raised)

    def test_date_null_field_not_set(self):
        class NewTable(Table):
            id = Auto(primary_key=True)
            text = Date(null=True)

        raised = False
        try:
            rec = NewTable()
        except FieldValueNotSet:
            raised = True

        self.assertFalse(raised)

    def test_date_not_null_field_not_set(self):
        class NewTable(Table):
            id = Auto(primary_key=True)
            text = Date()

        raised = False
        try:
            rec = NewTable()
        except FieldValueNotSet:
            raised = True

        self.assertTrue(raised)

    def test_float_null_field_not_set(self):
        class NewTable(Table):
            id = Auto(primary_key=True)
            text = Float(null=True)

        raised = False
        try:
            rec = NewTable()
        except FieldValueNotSet:
            raised = True

        self.assertFalse(raised)

    def test_float_not_null_field_not_set(self):
        class NewTable(Table):
            id = Auto(primary_key=True)
            text = Float()

        raised = False
        try:
            rec = NewTable()
        except FieldValueNotSet:
            raised = True

        self.assertTrue(raised)

    def test_blob_null_field_not_set(self):
        class NewTable(Table):
            id = Auto(primary_key=True)
            text = Blob(null=True)

        raised = False
        try:
            rec = NewTable()
        except FieldValueNotSet:
            raised = True

        self.assertFalse(raised)

    def test_blob_not_null_field_not_set(self):
        class NewTable(Table):
            id = Auto(primary_key=True)
            text = Blob()

        raised = False
        try:
            rec = NewTable()
        except FieldValueNotSet:
            raised = True

        self.assertTrue(raised)

    def test_auto_field_cannot_be_null(self):
        with self.assertRaises(WrongFieldProp):
            class NewTable(Table):
                id = Auto(null=True)

class TableClassTest(unittest.TestCase):
    def test_table_name(self):
        class NewTable(Table):
            id = Auto(primary_key=True)

        class AnotherTable(Table):
            __table__ = "my_table"
            id = Auto(primary_key=True)

        self.assertTrue(hasattr(NewTable, "__table__"))
        self.assertTrue(NewTable.__table__ == "newtable")
        self.assertTrue(AnotherTable.__table__ == "my_table")

    def test_table_two_pk_exception(self):
        raised = False
        try:
            class NewTable(Table):
                id = Auto(primary_key=True)
                another_id = Auto(primary_key=True)
        except NotUniquePrimaryKey:
            raised = True

        self.assertTrue(raised)

        raised = False
        try:
            class NewTable(Table):
                id = Auto(primary_key=True)
                another_id = Auto()
        except NotUniquePrimaryKey:
            raised = True

        self.assertFalse(raised)


class FieldSqliteTest(unittest.TestCase):
    def test_field_unique(self):
        f = Field(unique=True)
        self.assertEqual(str(f), "unique")
        f = Field(unique=False)
        self.assertEqual(str(f), "")

    def test_field_primary_key(self):
        f = Field(primary_key=True)
        self.assertEqual(str(f), "primary key")
        f = Field(primary_key=False)
        self.assertEqual(str(f), "")

    def test_field_auto_inc(self):
        f = Field(autoincrement=True)
        self.assertEqual(str(f), "autoincrement")
        f = Field(autoincrement=False)
        self.assertEqual(str(f), "")

    def test_field_not_null(self):
        f = Field(null=False)
        self.assertEqual(str(f), "not null")
        f = Field(null=True)
        self.assertEqual(str(f), "")

    def test_field_default_int(self):
        f = Field(default=1)
        self.assertEqual(str(f), "default 1")
        f = Field(default=None)
        self.assertEqual(str(f), "")

    def test_allowed_prop_default_is_string(self):
        f = Field(default="s")
        self.assertTrue(isinstance(f.allowed_props["default"], str))

    def test_allowed_prop_default_is_string(self):
        f = Field(default="s")
        self.assertEqual(f.allowed_props["default"], "'s'")

    def test_field_default_str(self):
        f = Field(default="s")
        self.assertEqual(str(f), "default 's'")

    def test_field_default_kwargs(self):
        f = Field()
        self.assertEqual(str(f), "")

    def test_field_exc_when_wrong_kwarg(self):
        with self.assertRaises(UnknownFieldProperty):
            f = Field(unknown_prop=1)

    def test_autofield_default_kwargs(self):
        f = Auto()
        self.assertEqual(str(f), "integer autoincrement not null")

    def test_autofield_primary_key(self):
        f = Auto(primary_key=True)
        self.assertEqual(str(f), "integer primary key autoincrement not null")

    def test_intfield_default_kwargs(self):
        f = Integer()
        self.assertEqual(str(f), "integer")

    def test_floatfield_default_kwargs(self):
        f = Float()
        self.assertEqual(str(f), "float")

    def test_doublefield_default_kwargs(self):
        f = Double()
        self.assertEqual(str(f), "double")

    def test_realfield_default_kwargs(self):
        f = Real()
        self.assertEqual(str(f), "real")

    def test_textfield_default_kwargs(self):
        f = Text()
        self.assertEqual(str(f), "text")

    def test_vcfield_default_kwargs(self):
        f = Text(max_length=10)
        self.assertEqual(str(f), "varchar(10)")

    def test_datefield_default_kwargs(self):
        f = Date()
        self.assertEqual(str(f), "datetime")

    def test_boolfield_default_kwargs(self):
        f = Bool()
        self.assertEqual(str(f), "bool")

    def test_blobfield_default_kwargs(self):
        f = Blob()
        self.assertEqual(str(f), "blob")

    def test_fkfield_default_kwargs(self):
        class NewTab(Table):
            id = Auto(primary_key=True)

        fk = ForeignKey(NewTab)
        self.assertEqual(str(fk), "integer")

    def test_fk_no_table_exception(self):
        with self.assertRaises(NoTableDefined):
            class Tab(Table):
                id = Auto(primary_key=True)
                fk = ForeignKey()

    def test_m2m_no_table_exception(self):
        with self.assertRaises(NoTableDefined):
            class Tab(Table):
                id = Auto(primary_key=True)
                m2m = ManyToMany()


class TestExpressions(unittest.TestCase):
    def test_expr_left_right(self):
        pass