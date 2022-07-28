import asyncio
import os
import re
from datetime import datetime, timezone
from tempfile import mkdtemp
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Dict,
    Final,
    List,
    Optional,
    Protocol,
    Tuple,
    cast,
    runtime_checkable,
)

import attr
import pytest
import pytest_asyncio
from aiohttp import (
    ClientResponseError,
    ClientSession,
    RequestInfo,
    UnixConnector,
)
from aiohttp.web import (
    Application,
    AppRunner,
    HTTPBadRequest,
    HTTPNotFound,
    Request,
    Response,
    StreamResponse,
    UnixSite,
    delete,
    get,
    json_response,
    middleware,
    patch,
    post,
)
from hypothesis import given
from hypothesis.strategies import integers
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from aioairtable import aioairtable as aat
from aioairtable.aioairtable import (
    Airtable,
    AirtableBase,
    AirtableRecord,
    AirtableTable,
    CellFormat,
    SortDirection,
    parse_dt,
)


@attr.s(auto_attribs=True, frozen=True)
class RequestData:
    method: str
    url: URL
    data: Any


Handler = Callable[[Request], Awaitable[StreamResponse]]


@runtime_checkable
class SupportsLessThan(Protocol):
    def __lt__(self, other: Any) -> bool:
        ...


def fields_sort_key(field: str) -> Callable[[aat.Record], SupportsLessThan]:
    def _fields_sort_key(record: aat.Record) -> SupportsLessThan:
        if not isinstance(record["fields"][field], SupportsLessThan):
            raise HTTPBadRequest(reason=f'"{field}" type not sortable')
        return cast(SupportsLessThan, record["fields"][field])

    return _fields_sort_key


class AirtableServer:
    def __init__(self, api_key: str) -> None:
        self._started: bool = False
        self._tmp_dir: Optional[str] = None
        self._connector: Optional[UnixConnector] = None
        self._loop = asyncio.get_running_loop()
        self._tables: Dict[Tuple[str, str], List[aat.Record]] = {}
        self._requests: List[RequestData] = []
        self._api_key: Final[str] = api_key
        application = Application(middlewares=(self._auth, self._log))
        application.router.add_routes(
            (
                get("/v0/{base_id}/{table_name}", self.list_records),
                get(
                    "/v0/{base_id}/{table_name}/{record_id}",
                    self.retrieve_record,
                ),
                post("/v0/{base_id}/{table_name}", self.create_record),
                patch(
                    "/v0/{base_id}/{table_name}/{record_id}",
                    self.update_record,
                ),
                delete(
                    "/v0/{base_id}/{table_name}/{record_id}",
                    self.delete_record,
                ),
            )
        )
        self._runner = AppRunner(application)

    @middleware
    async def _auth(
        self, request: Request, handler: Handler
    ) -> StreamResponse:
        if "Authorization" not in request.headers:
            raise HTTPBadRequest(reason="Authorization header absent")
        if request.headers["Authorization"] != f"Bearer {self._api_key}":
            raise HTTPBadRequest(reason="Wrong authorization header")
        if request.headers.get("User-Agent") != aat.SOFTWARE:
            raise HTTPBadRequest(reason="Wrong user-agent header")
        return await handler(request)

    @middleware
    async def _log(self, request: Request, handler: Handler) -> StreamResponse:
        url = request.url.with_scheme("https")
        has_data = request.method in ("POST", "PATCH")
        data = await request.json() if has_data else None
        self._requests.append(RequestData(request.method, url, data))
        return await handler(request)

    def requests(self) -> List[RequestData]:
        return self._requests.copy()

    def add_records(
        self, base_id: str, table_name: str, records: List[aat.Record]
    ) -> None:
        if (base_id, table_name) in self._tables:
            fields_keys = self._tables[base_id, table_name][0]["fields"].keys()
            assert all(
                fields_keys == record["fields"].keys() for record in records
            )
            self._tables[base_id, table_name].extend(records)
        else:
            self._tables[base_id, table_name] = records

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("Server already started")
        self._tmp_dir = await self._loop.run_in_executor(None, mkdtemp)
        await self._runner.setup()
        await UnixSite(self._runner, self._socket_path).start()
        self._started = True

    async def stop(self) -> None:
        if not self._started:
            raise RuntimeError("Server not started")
        await self._runner.cleanup()
        await self._loop.run_in_executor(None, os.remove, self._socket_path)
        await self._loop.run_in_executor(None, os.rmdir, self._tmp_dir)
        self._started = False

    @property
    def _socket_path(self) -> str:
        return f"{self._tmp_dir}/site.socket"

    @property
    def connector(self) -> UnixConnector:
        if self._connector is None:
            self._connector = UnixConnector(self._socket_path)
        return self._connector

    async def list_records(self, request: Request) -> Response:
        base_id = request.match_info["base_id"]
        table_name = request.match_info["table_name"]
        if (base_id, table_name) not in self._tables:
            raise HTTPNotFound(reason="Table not found")
        sort: Dict[int, Dict[str, str]] = {}
        for key in request.query:
            if (
                not key.startswith("fields")
                and len(request.query.getall(key)) > 1
            ):
                raise HTTPBadRequest(reason="Wrong sort parameter1")
            match = re.match(r"sort\[([^]]+)]\[([^]]+)]", key)
            if match is not None:
                index, fd = match.groups()
                if not index.isdigit() or fd not in ("field", "direction"):
                    raise HTTPBadRequest(reason="Wrong sort parameter2")
                if int(index) in sort:
                    sort[int(index)][fd] = request.query[key]
                else:
                    sort[int(index)] = {fd: request.query[key]}
        if not all("field" in item for item in sort.values()):
            raise HTTPBadRequest(reason="Wrong sort parameter3")
        records = self._tables[base_id, table_name].copy()
        for sort_index in sorted(sort, reverse=True):
            field = sort[sort_index]["field"]
            if field not in records[0]["fields"]:
                raise HTTPBadRequest(reason="Wrong sort parameter4")
            direction = sort[sort_index].get("direction", "asc")
            records.sort(
                key=fields_sort_key(field), reverse=direction == "desc"
            )
        if "maxRecords" in request.query:
            max_records = int(request.query["maxRecords"])
            records = records[:max_records]
        offset: Optional[str] = None
        if "offset" in request.query:
            for rec_idx, record in enumerate(records, start=1):
                if record["id"] == request.query["offset"]:
                    records = records[rec_idx:]
                    break
            else:
                raise HTTPBadRequest(reason="Wrong sort parameter5")
        page_size = int(request.query.get("pageSize", "100"))
        if len(records) > page_size:
            records = records[:page_size]
            offset = records[-1]["id"]
        if "fields[]" in request.query:
            fields = request.query.getall("fields[]")
            for record in records:
                record["fields"] = {
                    key: value
                    for key, value in record["fields"].items()
                    if key in fields
                }
        if offset is not None:
            response = {"records": records, "offset": offset}
        else:
            response = {"records": records}
        return json_response(response)

    async def retrieve_record(self, request: Request) -> Response:
        base_id = request.match_info["base_id"]
        table_name = request.match_info["table_name"]
        record_id = request.match_info["record_id"]
        if (base_id, table_name) not in self._tables:
            raise HTTPNotFound(reason="Table not found")
        for record in self._tables[base_id, table_name]:
            if record["id"] == record_id:
                return json_response(record)
        else:
            raise HTTPNotFound()

    async def create_record(self, request: Request) -> Response:
        base_id = request.match_info["base_id"]
        table_name = request.match_info["table_name"]
        fields = (await request.json())["fields"]
        created_time = datetime.now().strftime(aat.DT_FORMAT)
        record = aat.Record(
            id=f"record{self._loop.time()}",
            fields=fields,
            createdTime=created_time,
        )
        if (base_id, table_name) in self._tables:
            self._tables[base_id, table_name].append(record)
        else:
            self._tables[base_id, table_name] = [record]
        return json_response(record)

    async def update_record(self, request: Request) -> Response:
        base_id = request.match_info["base_id"]
        table_name = request.match_info["table_name"]
        record_id = request.match_info["record_id"]
        if (base_id, table_name) not in self._tables:
            raise HTTPNotFound(reason="Table not found")
        for record in self._tables[base_id, table_name]:
            if record["id"] == record_id:
                record["fields"] = (await request.json())["fields"]
                return json_response(record)
        else:
            raise HTTPNotFound()

    async def delete_record(self, request: Request) -> Response:
        base_id = request.match_info["base_id"]
        table_name = request.match_info["table_name"]
        record_id = request.match_info["record_id"]
        if (base_id, table_name) not in self._tables:
            raise HTTPNotFound(reason="Table not found")
        table = self._tables[base_id, table_name]
        for index, record in enumerate(table.copy()):
            if record["id"] == record_id:
                table.pop(index)
                deleted_record = aat.DeletedRecord(id=record_id, deleted=True)
                return json_response(deleted_record)
        else:
            raise HTTPNotFound()


@pytest_asyncio.fixture
def url() -> URL:
    url = URL("https://api.airtable.com/v0/base_id/table_name")
    return url.with_query(maxRecords=0)


@pytest_asyncio.fixture
def dt_str() -> str:
    return datetime.now().strftime(aat.DT_FORMAT)


@pytest_asyncio.fixture
async def server(dt_str: str) -> AsyncGenerator[AirtableServer, None]:
    server = AirtableServer("some_key")
    records = [
        aat.Record(
            id=f"record{index:03d}",
            fields={
                "field_1": f"value_1_{index:03d}",
                "field_2": f"value_2_{index:03d}",
                "field_3": f"value_3_{index:03d}",
            },
            createdTime=dt_str,
        )
        for index in range(200)
    ]
    server.add_records("base_id", "table_name", records)
    await server.start()
    yield server
    await server.stop()


@pytest_asyncio.fixture
async def airtable(server: AirtableServer) -> AsyncGenerator[Airtable, None]:
    airtable = Airtable("some_key", server.connector)
    yield airtable
    await airtable.close()


def test_parse_dt() -> None:
    string = "2020-12-06T13:45:55.000Z"
    dt = datetime(2020, 12, 6, 13, 45, 55, tzinfo=timezone.utc)
    assert parse_dt(string) == dt


@pytest.mark.parametrize(
    "string",
    (
        "2020-12-06T13:45:55.000",
        "2020-12-06T13:45:55",
        "2020-12-06 13:45:55",
        "2020-12-06",
        "some string",
    ),
)
def test_parse_dt_error(string: str) -> None:
    with pytest.raises(ValueError, match="time data"):
        parse_dt(string)


def test_backoff_wait_gen() -> None:
    wait_gen = aat.backoff_wait_gen()
    wait_gen.send(None)
    for value, i in zip(wait_gen, range(16)):
        assert value == aat.AT_WAIT + 2**i


def client_response_error(status: int) -> ClientResponseError:
    url = URL("example.com")
    info = RequestInfo(url, "GET", CIMultiDictProxy(CIMultiDict()), url)
    return ClientResponseError(info, tuple(), status=status)


@given(integers(min_value=100, max_value=526))
def test_backoff_giveup(status: int) -> None:
    backoff_flag = status not in (429, 502, 503, 504)
    assert aat.backoff_giveup(client_response_error(status)) == backoff_flag


def test_backoff_giveup_wrong_exception() -> None:
    with pytest.raises(AssertionError):
        aat.backoff_giveup(ValueError())


def test_build_repr() -> None:
    repr_str = aat.build_repr("A", b=1, c=2.34, d="efg")
    assert repr_str == "A(b=1, c=2.34, d='efg')"


@pytest.mark.asyncio
async def test_airtable_repr(airtable: Airtable) -> None:
    assert repr(airtable) == "Airtable(api_key='...')"


@pytest.mark.asyncio
async def test_airtable_client(
    server: AirtableServer, airtable: Airtable, url: URL
) -> None:
    assert isinstance(airtable.client, ClientSession)
    with pytest.raises(
        ClientResponseError, match="Authorization header absent"
    ):
        await airtable.client.get(url)
    assert server.requests() == []


@pytest.mark.asyncio
async def test_airtable_underscore_request(
    server: AirtableServer, airtable: Airtable, url: URL
) -> None:
    assert await airtable._request("GET", url) == {"records": []}
    assert server.requests() == [RequestData("GET", url, None)]


@pytest.mark.asyncio
async def test_airtable_request(
    server: AirtableServer, airtable: Airtable, url: URL
) -> None:
    loop = asyncio.get_running_loop()
    time1 = loop.time()
    assert await airtable.request("base_id", "GET", url) == {"records": []}
    assert server.requests() == [RequestData("GET", url, None)]
    assert await airtable.request("base_id", "GET", url) == {"records": []}
    time2 = loop.time()
    assert time2 - time1 >= aat.AT_INTERVAL
    assert server.requests() == [
        RequestData("GET", url, None),
        RequestData("GET", url, None),
    ]


@pytest.mark.asyncio
async def test_airtable_close() -> None:
    airtable = Airtable("secret_key")
    await airtable.close()
    assert airtable._client.closed


@pytest.mark.asyncio
async def test_airtable_context(airtable: Airtable) -> None:
    async with airtable:
        pass
    assert airtable._client.closed


@pytest.mark.asyncio
async def test_airtable_base(airtable: Airtable) -> None:
    base = airtable.base("some_base_id")
    assert isinstance(base, AirtableBase)
    assert base.id == "some_base_id"


@pytest.mark.asyncio
async def test_airtable_base_repr(airtable: Airtable) -> None:
    base = airtable.base("some_base_id")
    assert repr(base) == (
        "AirtableBase(base_id='some_base_id', "
        "airtable=Airtable(api_key='...'))"
    )


@pytest.mark.asyncio
async def test_airtable_base_id(airtable: Airtable) -> None:
    base = airtable.base("some_base_id")
    assert base.id == "some_base_id"


@pytest.mark.asyncio
async def test_airtable_base_url(airtable: Airtable) -> None:
    base = airtable.base("some_base_id")
    assert base.url == aat.API_URL / "some_base_id"


@pytest.mark.asyncio
async def test_airtable_base_request(
    server: AirtableServer, airtable: Airtable, url: URL
) -> None:
    base = airtable.base("base_id")
    assert await base.request("GET", url) == {"records": []}
    assert server.requests() == [RequestData("GET", url, None)]


@pytest.mark.asyncio
async def test_airtable_base_table(airtable: Airtable) -> None:
    base = airtable.base("some_base_id")
    table = base.table("some_table")
    assert isinstance(table, AirtableTable)
    assert table.name == "some_table"
    assert table.url == base.url / "some_table"
    assert table.base == base


@pytest.mark.asyncio
async def test_airtable_table_request(
    server: AirtableServer, airtable: Airtable, url: URL
) -> None:
    base = airtable.base("base_id")
    table = base.table("table_name")
    assert await table._request("GET", url) == {"records": []}
    assert server.requests() == [RequestData("GET", url, None)]


@pytest.mark.asyncio
async def test_airtable_table_list_records(
    server: AirtableServer, airtable: Airtable, dt_str: str
) -> None:
    base = airtable.base("base_id")
    table = base.table("table_name")
    records, offset = await table.list_records(
        fields=("field_1", "field_2"),
        filter_by_formula="{field_3}",
        max_records=100500,
        page_size=3,
        sort=(("field_1", SortDirection.ASC), ("field_2", SortDirection.DESC)),
        view="table3",
        cell_format=CellFormat.JSON,
        time_zone="Europe/Moscow",
        user_locale="ru",
        offset="record033",
    )
    assert server.requests() == [
        RequestData(
            "GET",
            table.url.with_query(
                (
                    ("fields[]", "field_1"),
                    ("fields[]", "field_2"),
                    ("filterByFormula", "{field_3}"),
                    ("maxRecords", "100500"),
                    ("pageSize", "3"),
                    ("sort[0][field]", "field_1"),
                    ("sort[0][direction]", "asc"),
                    ("sort[1][field]", "field_2"),
                    ("sort[1][direction]", "desc"),
                    ("view", "table3"),
                    ("cellFormat", "json"),
                    ("timeZone", "Europe/Moscow"),
                    ("userLocale", "ru"),
                    ("offset", "record033"),
                )
            ),
            None,
        )
    ]
    assert len(records) == 3
    for index, record in enumerate(records, start=34):
        assert isinstance(record, AirtableRecord)
        assert record.id == f"record{index:03d}"
        assert record.fields["field_1"] == f"value_1_{index:03d}"
        assert record.fields["field_2"] == f"value_2_{index:03d}"
        assert "field_3" not in record.fields
        assert record.created_time == parse_dt(dt_str)
        assert record.table == table
    assert offset == "record036"


@pytest.mark.asyncio
async def test_airtable_table_iter_records(
    server: AirtableServer, airtable: Airtable, dt_str: str
) -> None:
    base = airtable.base("base_id")
    table = base.table("table_name")
    records = [
        record
        async for record in table.iter_records(
            fields=["field_1"], max_records=6, page_size=3
        )
    ]
    assert len(records) == 6
    for index, record in enumerate(records):
        isinstance(record, AirtableRecord)
        assert record.id == f"record{index:03d}"
        assert record.fields["field_1"] == f"value_1_{index:03d}"
        assert "field_2" not in record.fields
        assert "field_3" not in record.fields
        assert record.created_time == parse_dt(dt_str)
        assert record.table == table
    assert server.requests() == [
        RequestData(
            "GET",
            table.url.with_query(
                (
                    ("fields[]", "field_1"),
                    ("maxRecords", "6"),
                    ("pageSize", "3"),
                )
            ),
            None,
        ),
        RequestData(
            "GET",
            table.url.with_query(
                (
                    ("fields[]", "field_1"),
                    ("maxRecords", "6"),
                    ("pageSize", "3"),
                    ("offset", "record002"),
                )
            ),
            None,
        ),
    ]


@pytest.mark.asyncio
async def test_airtable_table_retrieve_record(
    server: AirtableServer, airtable: Airtable, dt_str: str
) -> None:
    base = airtable.base("base_id")
    table = base.table("table_name")
    record = await table.retrieve_record("record003")
    assert isinstance(record, AirtableRecord)
    assert record.id == "record003"
    assert record.fields == {
        "field_1": "value_1_003",
        "field_2": "value_2_003",
        "field_3": "value_3_003",
    }
    assert record.created_time == parse_dt(dt_str)
    assert record.table == table
    assert server.requests() == [
        RequestData("GET", table.url / "record003", None)
    ]


@pytest.mark.asyncio
async def test_airtable_table_create_record(
    server: AirtableServer, airtable: Airtable, dt_str: str
) -> None:
    base = airtable.base("base_id")
    table = base.table("table_name")
    record = await table.create_record(
        {
            "field_1": "value_1_new_001",
            "field_2": "value_2_new_001",
            "field_3": "value_3_new_001",
        }
    )
    assert isinstance(record, AirtableRecord)
    assert isinstance(record.id, str)
    assert record.fields == {
        "field_1": "value_1_new_001",
        "field_2": "value_2_new_001",
        "field_3": "value_3_new_001",
    }
    assert record.created_time == parse_dt(dt_str)
    assert record.table == table
    assert server.requests() == [
        RequestData(
            "POST",
            table.url,
            {
                "fields": {
                    "field_1": "value_1_new_001",
                    "field_2": "value_2_new_001",
                    "field_3": "value_3_new_001",
                }
            },
        )
    ]


@pytest.mark.asyncio
async def test_airtable_record_init(airtable: Airtable, dt_str: str) -> None:
    base = airtable.base("some_base_id")
    table = base.table("some_table")
    record = AirtableRecord("record1", {}, dt_str, table)
    assert isinstance(record, AirtableRecord)


@pytest.mark.parametrize(
    "string",
    (
        "2020-12-06T13:45:55.000",
        "2020-12-06T13:45:55",
        "2020-12-06 13:45:55",
        "2020-12-06",
        "some string",
    ),
)
@pytest.mark.asyncio
async def test_airtable_record_init_error(
    airtable: Airtable, string: str
) -> None:
    base = airtable.base("some_base_id")
    table = base.table("some_table")
    with pytest.raises(ValueError, match="time data"):
        AirtableRecord("record1", {}, string, table)


@pytest.mark.asyncio
async def test_airtable_record_repr(airtable: Airtable) -> None:
    base = airtable.base("some_base_id")
    table = base.table("some_table")
    time_string = "2021-01-25T17:28:21.000Z"
    record = AirtableRecord("record1", {}, time_string, table)
    assert repr(record) == (
        "AirtableRecord(record_id='record1', fields={}, "
        "created_time=datetime.datetime(2021, 1, 25, 17, "
        "28, 21, tzinfo=datetime.timezone.utc), "
        "table=AirtableTable(table_name='some_table', "
        "base=AirtableBase(base_id='some_base_id', "
        "airtable=Airtable(api_key='...'))))"
    )


@pytest.mark.asyncio
async def test_airtable_record_table(airtable: Airtable, dt_str: str) -> None:
    base = airtable.base("some_base_id")
    table = base.table("some_table")
    record = AirtableRecord("record1", {}, dt_str, table)
    assert record.table == table


@pytest.mark.asyncio
async def test_airtable_record_request(
    server: AirtableServer, airtable: Airtable, url: URL, dt_str: str
) -> None:
    base = airtable.base("base_id")
    table = base.table("table_name")
    record = AirtableRecord("record000", {}, dt_str, table)
    assert await record._request("GET", record.url) == aat.Record(
        id="record000",
        fields={
            "field_1": "value_1_000",
            "field_2": "value_2_000",
            "field_3": "value_3_000",
        },
        createdTime=dt_str,
    )
    assert server.requests() == [RequestData("GET", record.url, None)]


@pytest.mark.asyncio
async def test_airtable_record_update(
    server: AirtableServer, airtable: Airtable, dt_str: str
) -> None:
    base = airtable.base("base_id")
    table = base.table("table_name")
    record = AirtableRecord("record000", {}, dt_str, table)
    fields = {
        "field_1": "value_1_new_000",
        "field_2": "value_2_new_000",
        "field_3": "value_3_new_000",
    }
    assert await record.update(fields.copy()) == fields
    assert server.requests() == [
        RequestData(
            "PATCH",
            record.url,
            {
                "fields": {
                    "field_1": "value_1_new_000",
                    "field_2": "value_2_new_000",
                    "field_3": "value_3_new_000",
                }
            },
        )
    ]


@pytest.mark.asyncio
async def test_airtable_record_delete(
    server: AirtableServer, airtable: Airtable, dt_str: str
) -> None:
    base = airtable.base("base_id")
    table = base.table("table_name")
    record = AirtableRecord("record000", {}, dt_str, table)
    assert await record.delete()
    assert server.requests() == [RequestData("DELETE", record.url, None)]
    with pytest.raises(RuntimeError, match="Record is already deleted"):
        assert await record.delete()
    assert server.requests() == [RequestData("DELETE", record.url, None)]
    with pytest.raises(RuntimeError, match="Record is deleted"):
        assert await record.update(
            {
                "field_1": "value_1_new_000",
                "field_2": "value_2_new_000",
                "field_3": "value_3_new_000",
            }
        )
    assert server.requests() == [RequestData("DELETE", record.url, None)]
