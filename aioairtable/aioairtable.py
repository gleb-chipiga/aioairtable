from datetime import datetime, timezone
from enum import Enum
from typing import (Any, AsyncIterator, Final, Generator, Iterable, List,
                    Literal, Mapping, Optional, Tuple, TypedDict, Union)

import aiohttp
import backoff
from aiofreqlimit import FreqLimit
from aiohttp import BaseConnector
from multidict import MultiDict
from yarl import URL

from .helpers import get_software, json_dumps

__all__ = ('Method', 'Fields', 'Thumbnail', 'Attachment', 'Collaborator',
           'SortDirection', 'CellFormat', 'parse_dt', 'Airtable',
           'AirtableBase', 'AirtableTable', 'AirtableRecord')

SOFTWARE: Final[str] = get_software()
API_URL: Final[URL] = URL('https://api.airtable.com/v0')
AT_INTERVAL: Final[float] = 1 / 5
AT_WAIT: Final[float] = 30
TOO_MANY_REQUESTS: Final[int] = 429
DT_FORMAT: Final[str] = '%Y-%m-%dT%H:%M:%S.000Z'


Method = Literal['GET', 'POST', 'PATCH', 'DELETE']
Fields = Mapping[str, Any]


class Record(TypedDict):
    id: str
    fields: Fields
    createdTime: str


class DeletedRecord(TypedDict):
    id: str
    deleted: bool


class RecordList(TypedDict):
    records: List[Record]
    offset: str


class Thumbnail(TypedDict):
    url: str
    width: int
    height: int


class TotalAttachment(TypedDict):
    id: str
    url: str
    filename: str
    size: int
    type: str


class Attachment(TotalAttachment, total=False):
    width: int
    height: int
    thumbnails: List[Thumbnail]


class Collaborator(TypedDict):
    id: str
    email: str
    name: str


class SortDirection(str, Enum):
    ASC = 'asc'
    DESC = 'desc'


class CellFormat(str, Enum):
    JSON = 'json'
    STRING = 'string'


def parse_dt(string: str) -> datetime:
    return datetime.strptime(string, DT_FORMAT).replace(tzinfo=timezone.utc)


def backoff_wait_gen() -> Generator[float, None, None]:
    for value in backoff.expo():
        yield AT_WAIT + value


def backoff_giveup(exception: Exception) -> bool:
    assert isinstance(exception, aiohttp.ClientResponseError)
    return exception.status != TOO_MANY_REQUESTS


def build_repr(class_name: str, **kwargs: Any) -> str:
    args = ', '.join(f'{key}={value!r}' for key, value in kwargs.items())
    return f'{class_name}({args})'


class Airtable:

    def __init__(
        self, api_key: str, connector: Optional[BaseConnector] = None
    ) -> None:
        headers = {
            'Authorization': f'Bearer {api_key}',
            'User-Agent': SOFTWARE
        }
        self._session = aiohttp.ClientSession(
            connector=connector, headers=headers, json_serialize=json_dumps,
            raise_for_status=True)
        self._freq_limit = FreqLimit(AT_INTERVAL)

    def __repr__(self) -> str:
        return build_repr('Airtable', api_key='...')

    async def _request(self, method: Method, url: URL, **kwargs: Any) -> Any:
        async with self._session.request(method, url, **kwargs) as response:
            return await response.json()

    @backoff.on_exception(backoff_wait_gen, aiohttp.ClientResponseError,
                          giveup=backoff_giveup)
    async def request(self, base_id: str, method: Method, url: URL,
                      **kwargs: Any) -> Any:
        async with self._freq_limit.acquire(base_id):
            return await self._request(method, url, **kwargs)

    async def close(self) -> None:
        await self._session.close()
        await self._freq_limit.clear()

    def base(self, base_id: str) -> 'AirtableBase':
        return AirtableBase(base_id, self)


class AirtableBase:

    def __init__(self, base_id: str, airtable: Airtable) -> None:
        self._airtable: Final[Airtable] = airtable
        self._id: Final[str] = base_id
        self._url: Final[URL] = API_URL / base_id

    def __repr__(self) -> str:
        return build_repr('AirtableBase', base_id=self._id,
                          airtable=self._airtable)

    @property
    def id(self) -> str:
        return self._id

    @property
    def url(self) -> URL:
        return self._url

    async def request(self, method: Method, url: URL, **kwargs: Any) -> Any:
        return await self._airtable.request(self._id, method, url, **kwargs)

    def table(self, table_name: str) -> 'AirtableTable':
        return AirtableTable(table_name, self)


class AirtableTable:

    def __init__(self, table_name: str, base: AirtableBase) -> None:
        self._name: Final[str] = table_name
        self._base: Final[AirtableBase] = base
        self._url: Final[URL] = base.url / table_name

    def __repr__(self) -> str:
        return build_repr('AirtableTable', table_name=self._name,
                          base=self._base)

    @property
    def name(self) -> str:
        return self._name

    @property
    def url(self) -> URL:
        return self._url

    @property
    def base(self) -> AirtableBase:
        return self._base

    async def _request(self, method: Method, url: URL, **kwargs: Any) -> Any:
        return await self._base.request(method, url, **kwargs)

    async def list_records(
        self,
        *,
        fields: Optional[Iterable[str]] = None,
        filter_by_formula: Optional[str] = None,
        max_records: Optional[int] = None,
        page_size: Optional[int] = None,
        sort: Optional[Iterable[Tuple[str, SortDirection]]] = None,
        view: Optional[str] = None,
        cell_format: Optional[CellFormat] = None,
        time_zone: Optional[str] = None,
        user_locale: Optional[str] = None,
        offset: Optional[str] = None
    ) -> Tuple[Tuple['AirtableRecord', ...], Optional[str]]:
        params: MultiDict[Union[int, str]] = MultiDict()
        if fields is not None:
            params.extend(('fields[]', fields) for fields in fields)
        if filter_by_formula is not None:
            params.add('filterByFormula', filter_by_formula)
        if max_records is not None:
            params.add('maxRecords', max_records)
        if page_size is not None:
            params.add('pageSize', page_size)
        if sort is not None:
            for index, (field, direction) in enumerate(sort):
                params.add(f'sort[{index}][field]', field)
                params.add(f'sort[{index}][direction]', direction.value)
        if view is not None:
            params.add('view', view)
        if cell_format is not None:
            params.add('cellFormat', cell_format.value)
        if time_zone is not None:
            params.add('timeZone', time_zone)
        if user_locale is not None:
            params.add('userLocale', user_locale)
        if offset is not None:
            params.add('offset', offset)
        url = self._url.with_query(params)
        record_list: RecordList = await self._request('GET', url)
        records = tuple(AirtableRecord(record['id'], record['fields'],
                                       record['createdTime'], self)
                        for record in record_list['records'])
        return records, record_list.get('offset')

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
        user_locale: Optional[str] = None
    ) -> AsyncIterator['AirtableRecord']:
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
                offset=offset
            )
            for record in records:
                yield record
            if offset is None:
                break

    async def retrieve_record(self, record_id: str) -> 'AirtableRecord':
        record: Record = await self._request('GET', self._url / record_id)
        return AirtableRecord(record['id'], record['fields'],
                              record['createdTime'], self)

    async def create_record(self, fields: Fields) -> 'AirtableRecord':
        record: Record = await self._request('POST', self._url,
                                             json={'fields': fields})
        return AirtableRecord(record['id'], record['fields'],
                              record['createdTime'], self)


class AirtableRecord:

    def __init__(self, record_id: str, fields: Fields, created_time: str,
                 table: AirtableTable) -> None:
        self._table: Final[AirtableTable] = table
        self._id: Final[str] = record_id
        self._url: Final[URL] = table.url / record_id
        self._fields: Fields = fields
        self._created_time: Final[datetime] = parse_dt(created_time)
        self._deleted: bool = False

    def __repr__(self) -> str:
        return build_repr('AirtableRecord', record_id=self._id,
                          fields=self._fields, created_time=self._created_time,
                          table=self._table)

    @property
    def id(self) -> str:
        return self._id

    @property
    def url(self) -> URL:
        return self._url

    @property
    def fields(self) -> Fields:
        return self._fields

    @property
    def created_time(self) -> datetime:
        return self._created_time

    @property
    def table(self) -> AirtableTable:
        return self._table

    async def _request(self, method: Method, url: URL, **kwargs: Any) -> Any:
        return await self._table.base.request(method, url, **kwargs)

    async def update(self, fields: Fields) -> Fields:
        if self._deleted:
            raise RuntimeError('Record is deleted')
        record: Record = await self._request('PATCH', self._url,
                                             json={'fields': fields})
        self._fields = record['fields']
        return record['fields']

    async def delete(self) -> bool:
        if self._deleted:
            raise RuntimeError('Record is already deleted')
        record: DeletedRecord = await self._request('DELETE', self._url)
        self._deleted = record['deleted']
        return record['deleted']
