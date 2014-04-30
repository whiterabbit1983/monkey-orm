This is a simple ORM written in Python. It's under heavy develpoment and lacks a lot of features now.

Sample session:

```python
>>> from monkey import *

>>> store = Store("sqlite://:memory:")
>>> class Tab(Table):
        id = Auto(primary_key=True)
        name = Text() 
>>> t = Tab(name="name 1")
>>> store.create_table(Tab)

you can also run to create a table:
>>> store << Tab

run
>>> store.add(t)
or
>>> store + t
to add a record

run
>>> store(Tab).all()
to get all records from table called 'Tab'

run
>>> store(Tab, Tab.id == 2).all()
to get all records with id = 2

run
>>> store.delete(t)
or
>>> store - t
to delete a record

```
