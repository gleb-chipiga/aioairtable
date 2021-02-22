============================================
Asynchronous client library for Airtable API
============================================

Key Features
============

* Asyncio and `aiohttp <https://github.com/aio-libs/aiohttp>`_ based
* All `airtable REST API <https://airtable.com/api>`_ methods supported
* API rate limit support
* Fully type annotated (`PEP 484 <https://www.python.org/dev/peps/pep-0484/>`_)

Installation
============
aioairtable is available on PyPI. Use pip to install it:

.. code-block:: bash

    pip install aioairtable

Requirements
============

* Python >= 3.8
* `aiohttp <https://github.com/aio-libs/aiohttp>`_
* `multidict <https://github.com/aio-libs/multidict>`_
* `backoff <https://github.com/litl/backoff>`_
* `aiofreqlimit <https://github.com/gleb-chipiga/aiofreqlimit>`_
* `yarl <https://github.com/aio-libs/yarl>`_

Using aioairtable
==================
Pass a value of any hashable type to `acquire` or do not specify any parameter:

.. code-block:: python

    import asyncio

    from aioairtable import Airtable, SortDirection


    async def main():
        airtable = Airtable(api_key='some_key')
        base = airtable.base('base_id')
        table = base.table('table_name')
        records, offset = await table.list_records(
            fields=('field_1', 'field_2'),
            filter_by_formula='{field_3}',
            max_records=100500,
            page_size=3,
            sort=(('field_1', SortDirection.ASC),
                  ('field_2', SortDirection.DESC)),
            view='table3',
            offset='record033'
        )
        for record in records:
            print(record)

        record = await table.create_record({'field_1': 'value_1_new_001',
                                            'field_2': 'value_2_new_001',
                                            'field_3': 'value_3_new_001'})
        await record.delete()


    asyncio.run(main())