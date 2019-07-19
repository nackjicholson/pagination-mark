from base64 import b64decode, b64encode
from json import dumps, loads
from typing import Dict, Tuple, Optional, Sequence, Iterable, Union, Any

MarkFields = Iterable[Union[Tuple, str]]


def encode_mark(x: Any) -> str:
    """Encodes python data as base64 encoded json.
    """
    json_bytes = dumps(x).encode("utf-8")
    x_encoded = b64encode(json_bytes)
    return x_encoded.decode("utf-8")


def decode_mark(mark: str) -> Any:
    """Decodes a base64 encoded json string back into python.
    """
    return loads(b64decode(mark.encode("utf-8")))


class MarkManager:
    """Encapsulates the encoding and decoding of pagination markers.

    A pagination marker is a piece of relevant data which informs the application how
    to return a subsequent page of data when given the marker in a request. It can be
    comprised of multiple pieces of information which is why we encode the marker data
    as a JSON string, and then base64 encode it as a token for the client. Clients
    don't need a full data object, just something to hold on to and pass back as necessary.
    """

    def __init__(self, page_size, mark_fields: Optional[MarkFields] = None):
        if mark_fields is None:
            mark_fields = []

        self.page_size = page_size

        # Map of names to be in the mark data, to keys to pull from last record.
        self._mark_fields_map = {}
        for mark_field in mark_fields:
            if isinstance(mark_field, tuple):
                mark_key, record_key = mark_field
                self._mark_fields_map[mark_key] = record_key
            else:
                self._mark_fields_map[mark_field] = mark_field

    def _get_mark_data(self, last_record):
        if not self._mark_fields_map:
            return last_record

        mark_data = {}
        for mark_key, record_key in self._mark_fields_map.items():
            mark_data[mark_key] = last_record[record_key]
        return mark_data

    def next(self, contents: Sequence) -> Optional[str]:
        """Takes the list of results and produces the next_mark token for getting
        the next set of data.
        """
        contents_count = len(contents)

        if contents_count == self.page_size:
            marker_data = self._get_mark_data(contents[-1])
            return encode_mark(marker_data)
        elif contents_count < self.page_size:
            return None
        else:
            raise ValueError(
                f"Contents unexpectedly have higher count {contents_count} "
                f"than page_size {self.page_size}."
            )

    def load(
        self, mark: Optional[str], default: Any = None
    ) -> Optional[Union[Dict, str]]:
        """Given an encoded mark, returns it's decoded value.

        Does some minor validation that the data contained in the encoded mark is relevant
        for this mark manager to decode based on the marks declared at instantiation.
        """
        if mark is None:
            return default

        loaded_mark = decode_mark(mark)

        if not self._mark_fields_map:
            return loaded_mark

        loaded_mark_fields = set(loaded_mark.keys())
        expected_mark_fields = set(self._mark_fields_map.keys())

        if loaded_mark_fields != expected_mark_fields:
            raise RuntimeError(
                "Decoded mark contained unspecified data. "
                f"Expected {sorted(expected_mark_fields)}, found {sorted(loaded_mark_fields)}."
            )

        return loaded_mark
