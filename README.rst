============================================
Asynchronous client library for Airtable API
============================================

.. image:: https://badge.fury.io/py/aioairtable.svg
   :target: https://pypi.org/project/aioairtable
   :alt: Latest PyPI package version

.. image:: https://img.shields.io/badge/license-MIT-blue.svg
   :target: https://github.com/gleb-chipiga/aioairtable/blob/master/LICENSE
   :alt: License

.. image:: https://img.shields.io/pypi/dm/aioairtable
   :target: https://pypistats.org/packages/aioairtable
   :alt: Downloads count

Key Features
============

* Asyncio and `aiohttp <https://github.com/aio-libs/aiohttp>`_ based
* All `airtable REST API <https://airtable.com/api>`_ methods supported
* API rate limit support
* Fully type annotated (`PEP 484 <https://www.python.org/dev/peps/pep-0484/>`_)
* Mapping of table fields into variable names

Installation
============
aioairtable is available on PyPI. Use pip to install it:

.. code-block:: bash

    pip install aioairtable

Requirements
============

* Python >= 3.11
* `aiohttp <https://github.com/aio-libs/aiohttp>`_
* `multidict <https://github.com/aio-libs/multidict>`_
* `backoff <https://github.com/litl/backoff>`_
* `aiofreqlimit <https://github.com/gleb-chipiga/aiofreqlimit>`_
* `yarl <https://github.com/aio-libs/yarl>`_
* `msgspec <https://github.com/jcrist/msgspec>`_

Using aioairtable
==================
Pass a value of any hashable type to `acquire` or do not specify any parameter:

.. code-block:: python

    import asyncio

    from msgspec import Struct, field

    from aioairtable import Airtable, SortDirection


    class TableFields(Struct):
        field_1: str | None = field(default=None, name="Field 1")
        field_2: str | None = field(default=None, name="Field 2")
        field_3: str | None = field(default=None, name="Field 3")


    async def main() -> None:
        airtable = Airtable(api_key="some_key")
        base = airtable.base("base_id")
        table = base.table("table_name", TableFields)
        records, offset = await table.list_records(
            fields=("field_1", "field_2"),
            filter_by_formula="{field_3}",
            max_records=100500,
            page_size=3,
            sort=(
                ("field_1", SortDirection.ASC),
                ("field_2", SortDirection.DESC),
            ),
            view="table3",
            offset="record033",
        )
        for record in records:
            print(record)

        record = await table.create_record(
            TableFields(
                field_1="value_1_new_001",
                field_2="value_2_new_001",
                field_3="value_3_new_001",
            )
        )
        await record.delete()


    asyncio.run(main())