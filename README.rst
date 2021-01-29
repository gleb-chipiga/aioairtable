About
=====
Asynchronous client library for Airtable API.

Installation
============
aioairtable requires Python 3.8 or greater and is available on PyPI. Use pip to install it:

.. code-block:: bash

    pip install aioairtable

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