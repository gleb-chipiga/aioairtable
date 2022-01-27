import asyncio
from datetime import datetime
from typing import AsyncGenerator
from unittest.mock import call

import pytest
from pytest_mock import MockerFixture
from yarl import URL

from aioairtable import Airtable
from aioairtable import aioairtable as aat
from aioairtable.aioairtable import (AirtableRecord, CellFormat, SortDirection,
                                     parse_dt)
from aioairtable.helpers import json_dumps


@pytest.fixture
def dt_str() -> str:
    return datetime.now().strftime(aat.DT_FORMAT)


@pytest.fixture
def url() -> URL:
    return URL('https://example.com')


@pytest.fixture
async def airtable() -> AsyncGenerator[Airtable, None]:
    airtable = Airtable('secret_key')
    yield airtable
    await airtable.close()


@pytest.mark.asyncio
async def test_airtable_underscore_request(airtable: Airtable, url: URL,
                                           mocker: MockerFixture) -> None:
    response_data = {'some_key': 55}
    request = mocker.patch.object(airtable._client, 'request')
    response = request.return_value.__aenter__.return_value
    response.read.return_value = json_dumps(response_data)
    assert await airtable._request('GET', url) == response_data
    request.assert_called_once_with(
        'GET', url, headers={'Authorization': 'Bearer secret_key'}, json=None)
    response.read.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_airtable_request(airtable: Airtable, url: URL,
                                mocker: MockerFixture) -> None:
    loop = asyncio.get_running_loop()
    response_data = {'some_key': 55}
    request = mocker.patch.object(airtable, '_request')
    request.return_value = response_data
    assert await airtable.request('some_base_id', 'GET', url) == response_data
    request.assert_awaited_once_with('GET', url, json=None)
    time1 = loop.time()
    assert await airtable.request('some_base_id', 'GET', url) == response_data
    time2 = loop.time()
    assert time2 - time1 >= aat.AT_INTERVAL


@pytest.mark.asyncio
async def test_airtable_base_request(airtable: Airtable, url: URL,
                                     mocker: MockerFixture) -> None:
    response_data = {'some_key': 55}
    base = airtable.base('some_base_id')
    request = mocker.patch.object(base._airtable, 'request')
    request.return_value = response_data
    assert await base.request('GET', url) == response_data
    request.assert_awaited_once_with('some_base_id', 'GET', url, json=None)


@pytest.mark.asyncio
async def test_airtable_table_request(airtable: Airtable, url: URL,
                                      mocker: MockerFixture) -> None:
    response_data = {'some_key': 55}
    base = airtable.base('some_base_id')
    table = base.table('some_table')
    request = mocker.patch.object(table._base, 'request')
    request.return_value = response_data
    assert await table._request('GET', url) == response_data
    request.assert_awaited_once_with('GET', url, json=None)


@pytest.mark.asyncio
async def test_airtable_table_list_records(airtable: Airtable,
                                           mocker: MockerFixture) -> None:
    base = airtable.base('some_base_id')
    table = base.table('some_table')
    request = mocker.patch.object(table, '_request')
    request.return_value = {'records': []}
    records, offset = await table.list_records(
        fields=('field1', 'field2', 'field3'),
        filter_by_formula='{field4}',
        max_records=100500,
        page_size=10,
        sort=(('field5', SortDirection.ASC), ('field6', SortDirection.DESC)),
        view='table3',
        cell_format=CellFormat.JSON,
        time_zone='Europe/Moscow',
        user_locale='ru',
        offset='offset22'
    )
    request.assert_awaited_once_with(
        'GET',
        table.url.with_query((
            ('fields[]', 'field1'),
            ('fields[]', 'field2'),
            ('fields[]', 'field3'),
            ('filterByFormula', '{field4}'),
            ('maxRecords', '100500'),
            ('pageSize', '10'),
            ('sort[0][field]', 'field5'),
            ('sort[0][direction]', 'asc'),
            ('sort[1][field]', 'field6'),
            ('sort[1][direction]', 'desc'),
            ('view', 'table3'),
            ('cellFormat', 'json'),
            ('timeZone', 'Europe/Moscow'),
            ('userLocale', 'ru'),
            ('offset', 'offset22')
        ))
    )
    assert records == ()
    assert offset is None


@pytest.mark.asyncio
async def test_airtable_table_iter_records(airtable: Airtable, dt_str: str,
                                           mocker: MockerFixture) -> None:
    base = airtable.base('some_base_id')
    table = base.table('some_table')
    request = mocker.patch.object(table, '_request')
    request.side_effect = (
        {'records': [
            {'id': 'record1', 'fields': {}, 'createdTime': dt_str},
            {'id': 'record2', 'fields': {}, 'createdTime': dt_str},
            {'id': 'record3', 'fields': {}, 'createdTime': dt_str}
        ], 'offset': 'offset1'},
        {'records': [
            {'id': 'record4', 'fields': {}, 'createdTime': dt_str},
            {'id': 'record5', 'fields': {}, 'createdTime': dt_str},
            {'id': 'record6', 'fields': {}, 'createdTime': dt_str}
        ]}
    )
    records = tuple([record async for record in table.iter_records()])
    assert request.await_args_list == [
        call('GET', table.url.with_query(pageSize=25)),
        call('GET', table.url.with_query(pageSize=25, offset='offset1'))
    ]
    assert all(isinstance(record, AirtableRecord) for record in records)
    record_ids = ('record1', 'record2', 'record3', 'record4', 'record5',
                  'record6')
    assert tuple(record.id for record in records) == record_ids


@pytest.mark.asyncio
async def test_airtable_table_retrieve_record(airtable: Airtable, dt_str: str,
                                              mocker: MockerFixture) -> None:
    base = airtable.base('some_base_id')
    table = base.table('some_table')
    request = mocker.patch.object(table, '_request')
    request.return_value = {'id': 'record1', 'fields': {},
                            'createdTime': dt_str}
    record = await table.retrieve_record('record1')
    request.assert_awaited_once_with('GET', table.url / 'record1')
    assert isinstance(record, AirtableRecord)
    assert record.id == 'record1'
    assert record.fields == {}
    assert record.created_time == parse_dt(dt_str)
    assert record.table == table


@pytest.mark.asyncio
async def test_airtable_table_create_record(airtable: Airtable, dt_str: str,
                                            mocker: MockerFixture) -> None:
    base = airtable.base('some_base_id')
    table = base.table('some_table')
    request = mocker.patch.object(table, '_request')
    request.return_value = {'id': 'record1', 'fields': {},
                            'createdTime': dt_str}
    record = await table.create_record({})
    request.assert_awaited_once_with('POST', table.url, json={'fields': {}})
    assert isinstance(record, AirtableRecord)
    assert record.id == 'record1'
    assert record.fields == {}
    assert record.created_time == parse_dt(dt_str)
    assert record.table == table


@pytest.mark.asyncio
async def test_airtable_record_request(
    airtable: Airtable, dt_str: str, url: URL, mocker: MockerFixture
) -> None:
    response_data = {'some_key': 55}
    base = airtable.base('some_base_id')
    table = base.table('some_table')
    record = AirtableRecord('record1', {}, dt_str, table)
    request = mocker.patch.object(record.table._base, 'request')
    request.return_value = response_data
    assert await table._request('GET', url) == response_data
    request.assert_awaited_once_with('GET', url, json=None)


@pytest.mark.asyncio
async def test_airtable_record_update(airtable: Airtable, dt_str: str,
                                      mocker: MockerFixture) -> None:
    base = airtable.base('some_base_id')
    table = base.table('some_table')
    record = AirtableRecord('record1', {}, dt_str, table)
    request = mocker.patch.object(record, '_request')
    request.return_value = {'fields': {}}
    assert await record.update({}) == {}
    request.assert_awaited_once_with('PATCH', record.url, json={'fields': {}})


@pytest.mark.asyncio
async def test_airtable_record_delete(airtable: Airtable, dt_str: str,
                                      mocker: MockerFixture) -> None:
    base = airtable.base('some_base_id')
    table = base.table('some_table')
    record = AirtableRecord('record1', {}, dt_str, table)
    request = mocker.patch.object(record, '_request')
    request.return_value = {'deleted': True}
    assert await record.delete()
    request.assert_awaited_once_with('DELETE', record.url)
