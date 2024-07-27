import asyncio
from datetime import UTC, datetime
from typing import AsyncGenerator, Final
from unittest.mock import call

import msgspec.json
import pytest
import pytest_asyncio
from msgspec import Struct
from multidict import CIMultiDict
from pytest_mock import MockerFixture
from yarl import URL

from aioairtable import Airtable
from aioairtable import aioairtable as aat
from aioairtable.aioairtable import (
    AirtableRecord,
    CellFormat,
    Fields,
    RecordRequest,
    SortDirection,
)

DT_FORMAT: Final = "%Y-%m-%dT%H:%M:%S.000Z"


def parse_dt(string: str) -> datetime:
    return datetime.strptime(string, DT_FORMAT).replace(tzinfo=UTC)


@pytest.fixture
def dt_str() -> str:
    return datetime.now().strftime(DT_FORMAT)


@pytest.fixture
def url() -> URL:
    return URL("https://example.com")


@pytest_asyncio.fixture
async def _airtable() -> AsyncGenerator[Airtable, None]:
    airtable = Airtable("secret_key")
    yield airtable
    await airtable.close()


class SomeResponseData(Struct, frozen=True):
    some_key: int


@pytest.fixture
def some_response_data() -> SomeResponseData:
    return SomeResponseData(55)


@pytest.mark.asyncio
async def test_airtable_underscore_request(
    _airtable: Airtable,
    url: URL,
    some_response_data: SomeResponseData,
    mocker: MockerFixture,
) -> None:
    request = mocker.patch.object(_airtable._client, "request")
    response = request.return_value.__aenter__.return_value
    response.read.return_value = msgspec.json.encode(some_response_data)
    assert (
        await _airtable._request(
            "GET",
            url,
            SomeResponseData,
        )
        == some_response_data
    )
    request.assert_called_once_with(
        "GET",
        url,
        headers=CIMultiDict(
            (
                ("User-Agent", aat.SOFTWARE),
                ("Authorization", "Bearer secret_key"),
            )
        ),
        data=None,
    )
    response.read.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_airtable_underscore_request_payload(
    _airtable: Airtable,
    url: URL,
    some_response_data: SomeResponseData,
    mocker: MockerFixture,
) -> None:
    request = mocker.patch.object(_airtable._client, "request")
    response = request.return_value.__aenter__.return_value
    response.read.return_value = msgspec.json.encode(some_response_data)
    assert (
        await _airtable._request(
            "GET",
            url,
            SomeResponseData,
            Struct(),
        )
        == some_response_data
    )
    request.assert_called_once_with(
        "GET",
        url,
        headers=CIMultiDict(
            (
                ("User-Agent", aat.SOFTWARE),
                ("Authorization", "Bearer secret_key"),
                ("Content-Type", "application/json"),
            )
        ),
        data=b"{}",
    )
    response.read.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_airtable_request(
    _airtable: Airtable,
    url: URL,
    some_response_data: SomeResponseData,
    mocker: MockerFixture,
) -> None:
    loop = asyncio.get_running_loop()
    request = mocker.patch.object(_airtable, "_request")
    request.return_value = some_response_data
    assert (
        await _airtable.request(
            "some_base_id",
            "GET",
            url,
            SomeResponseData,
        )
        == some_response_data
    )
    request.assert_awaited_once_with("GET", url, SomeResponseData, None)
    time1 = loop.time()
    assert (
        await _airtable.request(
            "some_base_id",
            "GET",
            url,
            SomeResponseData,
        )
        == some_response_data
    )
    time2 = loop.time()
    assert time2 - time1 >= aat.AT_INTERVAL


@pytest.mark.asyncio
async def test_airtable_base_request(
    _airtable: Airtable,
    url: URL,
    some_response_data: SomeResponseData,
    mocker: MockerFixture,
) -> None:
    base = _airtable.base("some_base_id")
    request = mocker.patch.object(base._airtable, "request")
    request.return_value = some_response_data
    assert (
        await base.request(
            "GET",
            url,
            SomeResponseData,
        )
        == some_response_data
    )
    request.assert_awaited_once_with(
        "some_base_id",
        "GET",
        url,
        SomeResponseData,
        None,
    )


@pytest.mark.asyncio
async def test_airtable_table_request(
    _airtable: Airtable,
    url: URL,
    some_response_data: SomeResponseData,
    mocker: MockerFixture,
) -> None:
    base = _airtable.base("some_base_id")
    table = base.table("some_table", Fields)
    request = mocker.patch.object(table._base, "request")
    request.return_value = some_response_data
    assert (
        await table._request(
            "GET",
            url,
            SomeResponseData,
        )
        == some_response_data
    )
    request.assert_awaited_once_with(
        "GET",
        url,
        SomeResponseData,
        None,
    )


@pytest.mark.asyncio
async def test_airtable_table_list_records(
    _airtable: Airtable,
    mocker: MockerFixture,
) -> None:
    base = _airtable.base("some_base_id")
    table = base.table("some_table", Fields)
    request = mocker.patch.object(table, "_request")
    request.return_value = aat.RecordList(records=())
    records, offset = await table.list_records(
        fields=("field1", "field2", "field3"),
        filter_by_formula="{field4}",
        max_records=100500,
        page_size=10,
        sort=(("field5", SortDirection.ASC), ("field6", SortDirection.DESC)),
        view="table3",
        cell_format=CellFormat.JSON,
        time_zone="Europe/Moscow",
        user_locale="ru",
        offset="offset22",
    )
    request.assert_awaited_once_with(
        "GET",
        table.url.with_query(
            (
                ("fields[]", "field1"),
                ("fields[]", "field2"),
                ("fields[]", "field3"),
                ("filterByFormula", "{field4}"),
                ("maxRecords", "100500"),
                ("pageSize", "10"),
                ("sort[0][field]", "field5"),
                ("sort[0][direction]", "asc"),
                ("sort[1][field]", "field6"),
                ("sort[1][direction]", "desc"),
                ("view", "table3"),
                ("cellFormat", "json"),
                ("timeZone", "Europe/Moscow"),
                ("userLocale", "ru"),
                ("offset", "offset22"),
            )
        ),
        type_=aat.RecordList[Fields],
    )
    assert records == ()
    assert offset is None


@pytest.mark.asyncio
async def test_airtable_table_iter_records(
    _airtable: Airtable,
    dt_str: str,
    mocker: MockerFixture,
) -> None:
    base = _airtable.base("some_base_id")
    table = base.table("some_table", Fields)
    request = mocker.patch.object(table, "_request")
    now = datetime.now(UTC)
    request.side_effect = (
        aat.RecordList(
            records=(
                aat.Record(id="record1", fields=Fields(), created_time=now),
                aat.Record(id="record2", fields=Fields(), created_time=now),
                aat.Record(id="record3", fields=Fields(), created_time=now),
            ),
            offset="offset1",
        ),
        aat.RecordList(
            records=(
                aat.Record(id="record4", fields=Fields(), created_time=now),
                aat.Record(id="record5", fields=Fields(), created_time=now),
                aat.Record(id="record6", fields=Fields(), created_time=now),
            ),
        ),
    )
    records = tuple([record async for record in table.iter_records()])
    assert request.await_args_list == [
        call(
            "GET",
            table.url.with_query(pageSize=25),
            type_=aat.RecordList[Fields],
        ),
        call(
            "GET",
            table.url.with_query(pageSize=25, offset="offset1"),
            type_=aat.RecordList[Fields],
        ),
    ]
    assert all(isinstance(record, AirtableRecord) for record in records)
    record_ids = (
        "record1",
        "record2",
        "record3",
        "record4",
        "record5",
        "record6",
    )
    assert tuple(record.id for record in records) == record_ids


@pytest.mark.asyncio
async def test_airtable_table_retrieve_record(
    _airtable: Airtable,
    dt_str: str,
    mocker: MockerFixture,
) -> None:
    base = _airtable.base("some_base_id")
    table = base.table("some_table", Fields)
    request = mocker.patch.object(table, "_request")
    now = datetime.now(UTC)
    record = aat.Record(
        id="record1",
        fields=Fields(),
        created_time=now,
    )
    request.return_value = record
    at_record = await table.retrieve_record("record1")
    request.assert_awaited_once_with(
        "GET",
        table.url / "record1",
        type_=aat.Record[Fields],
    )
    assert isinstance(at_record, AirtableRecord)
    assert at_record.id == "record1"
    assert at_record.fields == Fields()
    assert at_record.created_time == now
    assert at_record.table == table


@pytest.mark.asyncio
async def test_airtable_table_create_record(
    _airtable: Airtable,
    dt_str: str,
    mocker: MockerFixture,
) -> None:
    base = _airtable.base("some_base_id")
    table = base.table("some_table", Fields)
    request = mocker.patch.object(table, "_request")
    now = datetime.now(UTC)
    request.return_value = aat.Record(
        id="record1",
        fields=Fields(),
        created_time=now,
    )
    record = await table.create_record(Fields())
    request.assert_awaited_once_with(
        "POST",
        table.url,
        payload=RecordRequest(Fields()),
        type_=aat.Record[Fields],
    )
    assert isinstance(record, AirtableRecord)
    assert record.id == "record1"
    assert record.fields == Fields()
    assert record.created_time == now
    assert record.table == table


@pytest.mark.asyncio
async def test_airtable_record_request(
    _airtable: Airtable,
    dt_str: str,
    url: URL,
    some_response_data: SomeResponseData,
    mocker: MockerFixture,
) -> None:
    base = _airtable.base("some_base_id")
    table = base.table("some_table", Fields)
    record = AirtableRecord(
        "record1",
        Fields(),
        datetime.now(UTC),
        table,
    )
    request = mocker.patch.object(record.table._base, "request")
    request.return_value = some_response_data
    assert (
        await table._request(
            "GET",
            url,
            SomeResponseData,
        )
        == some_response_data
    )
    request.assert_awaited_once_with(
        "GET",
        url,
        SomeResponseData,
        None,
    )


@pytest.mark.asyncio
async def test_airtable_record_update(
    _airtable: Airtable,
    dt_str: str,
    mocker: MockerFixture,
) -> None:
    base = _airtable.base("some_base_id")
    table = base.table("some_table", Fields)
    record = AirtableRecord(
        "record1",
        Fields(),
        datetime.now(UTC),
        table,
    )
    request = mocker.patch.object(record, "_request")
    request.return_value = aat.Record(
        "record1",
        Fields(),
        datetime.now(UTC),
    )
    await record.update(
        Fields(),
    )
    assert record.fields == Fields()
    request.assert_awaited_once_with(
        "PATCH",
        record.url,
        type_=aat.Record[Fields],
        payload=RecordRequest(Fields()),
    )


@pytest.mark.asyncio
async def test_airtable_record_delete(
    _airtable: Airtable,
    dt_str: str,
    mocker: MockerFixture,
) -> None:
    base = _airtable.base("some_base_id")
    table = base.table("some_table", Fields)
    record = AirtableRecord(
        "record1",
        Fields(),
        datetime.now(UTC),
        table,
    )
    request = mocker.patch.object(record, "_request")
    request.return_value = aat.DeletedRecord("record1", True)
    await record.delete()
    assert record.deleted
    request.assert_awaited_once_with(
        "DELETE",
        record.url,
        type_=aat.DeletedRecord,
    )
