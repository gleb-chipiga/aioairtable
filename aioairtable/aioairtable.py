import logging
from datetime import datetime
from enum import IntEnum, StrEnum, unique
from types import TracebackType
from typing import (
    Any,
    AsyncIterator,
    Final,
    Generator,
    Generic,
    Iterable,
    Literal,
    Optional,
    Self,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import backoff
import msgspec.json
from aiofreqlimit import FreqLimit
from aiohttp import BaseConnector, ClientResponseError, ClientSession
from msgspec import Struct, field
from multidict import MultiDict
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
    "Method",
    "NewAttachment",
    "RecordList",
    "SortDirection",
    "Thumbnail",
)

SOFTWARE: Final[str] = get_software()
API_URL: Final[URL] = URL("https://api.airtable.com/v0")
AT_INTERVAL: Final[float] = 1 / 5
AT_WAIT: Final[float] = 30


@unique
class BackoffCodes(IntEnum):
    TOO_MANY_REQUESTS = 429
    BAD_GATEWAY = 502
    SERVICE_UNAVAILABLE = 503
    GATEWAY_TIMEOUT = 504


Method = Literal["GET", "POST", "PATCH", "DELETE"]

logger = logging.getLogger("airtable")


_T = TypeVar("_T", bound=Struct)


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


class Thumbnail(Struct, frozen=True):
    url: str
    width: int
    height: int


class NewAttachment(Struct, frozen=True, omit_defaults=True):
    url: str
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


def backoff_wait_gen(at_wait: float) -> Generator[float, Any, None]:
    expo_gen = backoff.expo()
    yield expo_gen.send(None)
    for value in expo_gen:
        yield at_wait + value


def backoff_giveup(exception: Exception) -> bool:
    assert isinstance(exception, ClientResponseError)
    try:
        BackoffCodes(exception.status)
    except ValueError:
        return True
    else:
        return False


def build_repr(class_name: str, **kwargs: Any) -> str:
    args = ", ".join(f"{key}={value!r}" for key, value in kwargs.items())
    return f"{class_name}({args})"


_R = TypeVar("_R", bound=Struct)


class Airtable:
    def __init__(
        self,
        api_key: str,
        connector: BaseConnector | None = None,
    ) -> None:
        self._auth_headers = {"Authorization": f"Bearer {api_key}"}
        self._client = ClientSession(
            connector=connector,
            headers={"User-Agent": SOFTWARE},
            raise_for_status=True,
        )
        self._freq_limit = FreqLimit(AT_INTERVAL)

    def __repr__(self) -> str:
        return build_repr("Airtable", api_key="...")

    @property
    def client(self) -> ClientSession:
        return self._client

    async def _request(
        self,
        method: Method,
        url: URL,
        type_: Type[_R],
        payload: Struct | None = None,
    ) -> _R:
        async with self._client.request(
            method,
            url,
            headers=self._auth_headers,
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

    @backoff.on_exception(
        backoff_wait_gen,
        ClientResponseError,
        giveup=backoff_giveup,
        at_wait=AT_WAIT,
    )
    async def request(
        self,
        base_id: str,
        method: Method,
        url: URL,
        type_: Type[_R],
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
        exc_type: Type[BaseException] | None,
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
        type_: Type[_R],
        payload: Any = None,
    ) -> _R:
        return await self._airtable.request(
            self._id,
            method,
            url,
            type_,
            payload,
        )

    def table(
        self, table_name: str, fields_type: Type[_T]
    ) -> "AirtableTable[_T]":
        return AirtableTable(table_name, self, fields_type)


class AirtableTable(Generic[_T]):
    def __init__(
        self,
        table_name: str,
        base: AirtableBase,
        fields_type: Type[_T],
    ) -> None:
        self._name: Final = table_name
        self._base: Final = base
        self._url: Final[URL] = base.url / table_name
        self._fields_type: Final = fields_type

    def __repr__(self) -> str:
        return build_repr(
            "AirtableTable", table_name=self._name, base=self._base
        )

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
    def fields_type(self) -> Type[_T]:
        return self._fields_type

    async def _request(
        self,
        method: Method,
        url: URL,
        type_: Type[_R],
        payload: Any = None,
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
        params: MultiDict[Union[int, str]] = MultiDict()
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
        fields_type = self._fields_type
        record_list = await self._request(
            "GET",
            url,
            type_=RecordList[fields_type],  # type: ignore[valid-type]
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
        fields: Optional[Iterable[str]] = None,
        filter_by_formula: Optional[str] = None,
        max_records: Optional[int] = None,
        page_size: int = 25,
        sort: Optional[Iterable[Tuple[str, SortDirection]]] = None,
        view: Optional[str] = None,
        cell_format: Optional[CellFormat] = None,
        time_zone: Optional[str] = None,
        user_locale: Optional[str] = None,
    ) -> AsyncIterator["AirtableRecord[_T]"]:
        offset: Optional[str] = None
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
        fields_type = self._fields_type
        record = await self._request(
            "GET",
            self._url / record_id,
            type_=Record[fields_type],  # type: ignore[valid-type]
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
        fields_type = self._fields_type
        record = await self._request(
            "POST",
            self._url,
            type_=Record[fields_type],  # type: ignore[valid-type]
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
        self._fields = fields
        self._created_time: Final = created_time
        self._deleted: bool = False

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
        type_: Type[_R],
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
        fields_type = self._table.fields_type
        record = await self._request(
            "PATCH",
            self._url,
            payload=RecordRequest(fields),
            type_=Record[fields_type],  # type: ignore[valid-type]
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
