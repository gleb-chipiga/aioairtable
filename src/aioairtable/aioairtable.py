import logging
from collections.abc import AsyncIterator, Generator, Iterable
from datetime import datetime
from enum import IntEnum, StrEnum, unique
from types import TracebackType
from typing import Final, Generic, Literal, Self, TypeVar

import msgspec.json
from aiofreqlimit import FreqLimit, FreqLimitParams
from aiofreqlimit.backends.memory import InMemoryBackend
from aiohttp import BaseConnector, ClientResponseError, ClientSession
from msgspec import Struct, field
from multidict import CIMultiDict, MultiDict
from tenacity import RetryCallState, retry, retry_if_exception
from typing_extensions import override
from yarl import URL

from .helpers import get_software

__all__ = (
    "Airtable",
    "AirtableBase",
    "AirtableRecord",
    "AirtableTable",
    "Attachment",
    "CellFormat",
    "Collaborator",
    "Fields",
    "Method",
    "NewAttachment",
    "SortDirection",
    "Thumbnail",
)

SOFTWARE: Final = get_software()
API_URL: Final = URL("https://api.airtable.com/v0")
AT_INTERVAL: Final = 1 / 5
AT_WAIT: Final = 30.0


@unique
class BackoffCodes(IntEnum):
    TOO_MANY_REQUESTS = 429
    BAD_GATEWAY = 502
    SERVICE_UNAVAILABLE = 503
    GATEWAY_TIMEOUT = 504


Method = Literal["GET", "POST", "PATCH", "DELETE"]

logger = logging.getLogger("airtable")


class Fields(Struct, omit_defaults=True):
    pass


_T = TypeVar("_T", bound=Fields)
_F = TypeVar("_F", bound=Fields)


class Record(Struct, Generic[_T], frozen=True):
    id: str
    fields: _T
    created_time: datetime = field(name="createdTime")


class RecordRequest(Struct, Generic[_T], frozen=True):
    fields: _T


class DeletedRecord(Struct, frozen=True):
    id: str
    deleted: bool


class RecordList(Struct, Generic[_T], frozen=True):
    records: tuple[Record[_T], ...]
    offset: str | None = None


def _make_record_list_type_helper(
    fields_type: type[_F],
) -> type[RecordList[_F]]:
    """Helper function to create RecordList type with dynamic type."""
    # Here mypy sees a variable in type and complains with [valid-type]
    return RecordList[fields_type]  # type: ignore[valid-type]


def _make_record_type_helper(
    fields_type: type[_F],
) -> type[Record[_F]]:
    """Helper function to create Record type with dynamic type."""
    # Here mypy sees a variable in type and complains with [valid-type]
    return Record[fields_type]  # type: ignore[valid-type]


class Thumbnail(Struct, frozen=True):
    url: str
    width: int
    height: int


class NewAttachment(Struct, frozen=True, omit_defaults=True):
    url: str
    id: str | None = None
    filename: str | None = None


class Attachment(Struct, frozen=True, omit_defaults=True):
    id: str
    url: str
    filename: str
    size: int
    type: str
    width: int | None = None
    height: int | None = None
    thumbnails: tuple[Thumbnail, ...] | None = None


class Collaborator(Struct, frozen=True):
    id: str
    email: str
    name: str


@unique
class SortDirection(StrEnum):
    ASC = "asc"
    DESC = "desc"


@unique
class CellFormat(StrEnum):
    JSON = "json"
    STRING = "string"


def backoff_wait_gen(
    at_wait: float,
) -> Generator[float, None, None]:
    expo: float = 1.0
    yield expo
    while True:
        yield at_wait + expo
        expo *= 2


def backoff_giveup(exception: Exception) -> bool:
    assert isinstance(exception, ClientResponseError)
    try:
        _ = BackoffCodes(exception.status)
    except ValueError:
        return True
    else:
        return False


def backoff_should_retry(exception: BaseException) -> bool:
    if not isinstance(exception, ClientResponseError):
        return False
    return not backoff_giveup(exception)


def backoff_wait(at_wait: float, retry_state: RetryCallState) -> float:
    attempt = int(retry_state.attempt_number)
    expo_int = 1 << (attempt - 1)
    expo = float(expo_int)
    if attempt == 1:
        return expo
    return float(at_wait + expo)


def build_repr(
    class_name: str,
    **kwargs: object,
) -> str:
    args = ", ".join(f"{key}={value!r}" for key, value in kwargs.items())
    return f"{class_name}({args})"


_R = TypeVar("_R", bound=Struct)


class Airtable:
    def __init__(
        self,
        api_key: str,
        connector: BaseConnector | None = None,
    ) -> None:
        self._headers: Final = CIMultiDict({
            "User-Agent": SOFTWARE,
            "Authorization": f"Bearer {api_key}",
        })
        self._json_headers: Final = CIMultiDict({
            **self._headers,
            **{"Content-Type": "application/json"},
        })
        self._client: ClientSession = ClientSession(
            connector=connector,
            raise_for_status=True,
        )
        params = FreqLimitParams(limit=1, period=AT_INTERVAL)
        self._freq_limit: FreqLimit = FreqLimit(params, backend=InMemoryBackend())

    @override
    def __repr__(self) -> str:
        return build_repr("Airtable", api_key="...")

    @property
    def client(self) -> ClientSession:
        return self._client

    async def _request(
        self,
        method: Method,
        url: URL,
        type_: type[_R],
        payload: Struct | None = None,
    ) -> _R:
        async with self._client.request(
            method,
            url,
            headers=self._headers if payload is None else self._json_headers,
            data=msgspec.json.encode(payload) if payload is not None else None,
        ) as client_response:
            logger.debug(
                "Request %s %s %r",
                method,
                url.human_repr(),
                payload,
            )
            response_data = await client_response.read()
            response = msgspec.json.decode(response_data, type=type_)
            logger.debug(
                "Response %r",
                response,
            )
            return response

    @retry(
        retry=retry_if_exception(lambda exc: backoff_should_retry(exc)),
        wait=lambda state: backoff_wait(AT_WAIT, state),
        reraise=True,
    )
    async def request(
        self,
        base_id: str,
        method: Method,
        url: URL,
        type_: type[_R],
        payload: Struct | None = None,
    ) -> _R:
        async with self._freq_limit.resource(base_id):
            return await self._request(
                method,
                url,
                type_,
                payload,
            )

    async def close(self) -> None:
        await self._client.close()
        await self._freq_limit.clear()

    def base(self, base_id: str) -> "AirtableBase":
        return AirtableBase(base_id, self)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()


class AirtableBase:
    def __init__(
        self,
        base_id: str,
        airtable: Airtable,
    ) -> None:
        self._airtable: Final[Airtable] = airtable
        self._id: Final[str] = base_id
        self._url: Final[URL] = API_URL / base_id

    @override
    def __repr__(self) -> str:
        return build_repr(
            "AirtableBase",
            base_id=self._id,
            airtable=self._airtable,
        )

    @property
    def id(self) -> str:
        return self._id

    @property
    def url(self) -> URL:
        return self._url

    async def request(
        self,
        method: Method,
        url: URL,
        type_: type[_R],
        payload: Struct | None = None,
    ) -> _R:
        return await self._airtable.request(
            self._id,
            method,
            url,
            type_,
            payload,
        )

    def table(self, table_name: str, fields_type: type[_T]) -> "AirtableTable[_T]":
        return AirtableTable(table_name, self, fields_type)


class AirtableTable(Generic[_T]):
    def __init__(
        self,
        table_name: str,
        base: AirtableBase,
        fields_type: type[_T],
    ) -> None:
        self._name: Final = table_name
        self._base: Final = base
        self._url: Final[URL] = base.url / table_name
        self._fields_type: Final[type[_T]] = fields_type

    @override
    def __repr__(self) -> str:
        return build_repr("AirtableTable", table_name=self._name, base=self._base)

    @property
    def name(self) -> str:
        return self._name

    @property
    def url(self) -> URL:
        return self._url

    @property
    def base(self) -> AirtableBase:
        return self._base

    @property
    def fields_type(self) -> type[_T]:
        return self._fields_type

    def _make_record_list_type(
        self,
        fields_type: type[_T],
    ) -> type[RecordList[_T]]:
        # Use modular helper function
        return _make_record_list_type_helper(fields_type)

    def _make_record_type(
        self,
        fields_type: type[_T],
    ) -> type[Record[_T]]:
        # Use modular helper function and cast to preserve _T
        return _make_record_type_helper(fields_type)

    async def _request(
        self,
        method: Method,
        url: URL,
        type_: type[_R],
        payload: Struct | None = None,
    ) -> _R:
        return await self._base.request(
            method,
            url,
            type_,
            payload,
        )

    async def list_records(
        self,
        *,
        fields: Iterable[str] | None = None,
        filter_by_formula: str | None = None,
        max_records: int | None = None,
        page_size: int | None = None,
        sort: Iterable[tuple[str, SortDirection]] | None = None,
        view: str | None = None,
        cell_format: CellFormat | None = None,
        time_zone: str | None = None,
        user_locale: str | None = None,
        offset: str | None = None,
    ) -> tuple[tuple["AirtableRecord[_T]", ...], str | None]:
        params = MultiDict[int | str]()
        if fields is not None:
            params.extend(("fields[]", fields) for fields in fields)
        if filter_by_formula is not None:
            params.add("filterByFormula", filter_by_formula)
        if max_records is not None:
            params.add("maxRecords", max_records)
        if page_size is not None:
            params.add("pageSize", page_size)
        if sort is not None:
            for index, (field_name, direction) in enumerate(sort):
                params.add(f"sort[{index}][field]", field_name)
                params.add(f"sort[{index}][direction]", direction)
        if view is not None:
            params.add("view", view)
        if cell_format is not None:
            params.add("cellFormat", cell_format)
        if time_zone is not None:
            params.add("timeZone", time_zone)
        if user_locale is not None:
            params.add("userLocale", user_locale)
        if offset is not None:
            params.add("offset", offset)
        url = self._url.with_query(params)
        # Get runtime type RecordList[...] via helper
        record_list_type = self._make_record_list_type(self.fields_type)

        record_list = await self._request(
            "GET",
            url,
            type_=record_list_type,
        )
        records = tuple(
            AirtableRecord(
                record.id,
                record.fields,
                record.created_time,
                self,
            )
            for record in record_list.records
        )
        return records, record_list.offset

    async def iter_records(
        self,
        *,
        fields: Iterable[str] | None = None,
        filter_by_formula: str | None = None,
        max_records: int | None = None,
        page_size: int = 25,
        sort: Iterable[tuple[str, SortDirection]] | None = None,
        view: str | None = None,
        cell_format: CellFormat | None = None,
        time_zone: str | None = None,
        user_locale: str | None = None,
    ) -> AsyncIterator["AirtableRecord[_T]"]:
        offset: str | None = None
        while True:
            records, offset = await self.list_records(
                fields=fields,
                filter_by_formula=filter_by_formula,
                max_records=max_records,
                page_size=page_size,
                sort=sort,
                view=view,
                cell_format=cell_format,
                time_zone=time_zone,
                user_locale=user_locale,
                offset=offset,
            )
            for record in records:
                yield record
            if offset is None:
                break

    async def retrieve_record(
        self,
        record_id: str,
    ) -> "AirtableRecord[_T]":
        # Create runtime type Record[...] via helper
        record_type = self._make_record_type(self.fields_type)

        record = await self._request(
            "GET",
            self._url / record_id,
            type_=record_type,
        )
        return AirtableRecord(
            record.id,
            record.fields,
            record.created_time,
            self,
        )

    async def create_record(
        self,
        fields: _T,
    ) -> "AirtableRecord[_T]":
        # Create runtime type Record[...]
        record_type = self._make_record_type(self.fields_type)

        record = await self._request(
            "POST",
            self._url,
            type_=record_type,
            payload=RecordRequest(fields),
        )
        return AirtableRecord(
            record.id,
            record.fields,
            record.created_time,
            table=self,
        )


class AirtableRecord(Generic[_T]):
    def __init__(
        self,
        record_id: str,
        fields: _T,
        created_time: datetime,
        table: AirtableTable[_T],
    ) -> None:
        self._table: Final = table
        self._id: Final = record_id
        self._url: Final = table.url / record_id
        self._fields: _T = fields
        self._created_time: Final = created_time
        self._deleted: bool = False

    @override
    def __repr__(self) -> str:
        return build_repr(
            "AirtableRecord",
            record_id=self._id,
            fields=self._fields,
            created_time=self._created_time,
            table=self._table,
        )

    @property
    def id(self) -> str:
        return self._id

    @property
    def url(self) -> URL:
        return self._url

    @property
    def fields(self) -> _T:
        return self._fields

    @property
    def created_time(self) -> datetime:
        return self._created_time

    @property
    def table(self) -> AirtableTable[_T]:
        return self._table

    @property
    def deleted(self) -> bool:
        return self._deleted

    async def _request(
        self,
        method: Method,
        url: URL,
        type_: type[_R],
        payload: Struct | None = None,
    ) -> _R:
        return await self._table.base.request(
            method,
            url,
            type_,
            payload,
        )

    async def update(
        self,
        fields: _T,
    ) -> None:
        if self._deleted:
            raise RuntimeError("Record is deleted")
        # Create runtime type Record[...] via helper function
        record_type = _make_record_type_helper(self._table.fields_type)

        record = await self._request(
            "PATCH",
            self._url,
            payload=RecordRequest(fields),
            type_=record_type,
        )
        self._fields = record.fields

    async def delete(self) -> None:
        if self._deleted:
            raise RuntimeError("Record is already deleted")
        record = await self._request(
            "DELETE",
            self._url,
            type_=DeletedRecord,
        )
        assert record.deleted
        self._deleted = record.deleted
