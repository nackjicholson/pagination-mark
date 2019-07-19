import pytest

from pagination_mark import MarkManager


class TestMarkEncoder:
    """
    py.test tests/unit/source_data/test_pagination.py -k TestMarkEncoder
    """

    def test_mark_string_simple_mark(self):
        contents = ["foo", "bar", "baz"]

        mark_mgr = MarkManager(3)
        mark = mark_mgr.next(contents)

        actual = mark_mgr.load(mark)
        expected = "baz"

        assert actual == expected

    def test_mark_encoded_with_full_record(self):
        contents = [
            ("foo", "bar", "beep", "boop"),
            ("foo", "baz", "beep", "bang"),
            ("foo", "qux", "beep", "bing"),
        ]

        mark_mgr = MarkManager(3)
        mark = mark_mgr.next(contents)

        actual = mark_mgr.load(mark)
        # The JSON process turns it to a list.
        expected = ["foo", "qux", "beep", "bing"]

        assert actual == expected

    def test_mark_encoded_with_multiple_fields(self):
        contents = [
            {"foo": "bar", "beep": "boop"},
            {"foo": "baz", "beep": "bang"},
            {"foo": "qux", "beep": "bing"},
        ]

        mark_mgr = MarkManager(3, ["foo", "beep"])
        mark = mark_mgr.next(contents)

        actual = mark_mgr.load(mark)
        expected = {"foo": "qux", "beep": "bing"}

        assert actual == expected

    def test_mark_encoded_with_custom_renamed_fields(self):
        contents = [
            {"foo": "bar", "beep": "boop"},
            {"foo": "baz", "beep": "bang"},
            {"foo": "qux", "beep": "bing"},
        ]

        mark_mgr = MarkManager(3, [("ZAP", "foo"), ("ZIP", "beep")])
        mark = mark_mgr.next(contents)

        actual = mark_mgr.load(mark)
        expected = {"ZAP": "qux", "ZIP": "bing"}

        assert actual == expected

    def test_mark_is_none_for_last_page(self):
        contents = ["foo", "bar"]

        mark_mgr = MarkManager(3)
        mark = mark_mgr.next(contents)

        assert mark is None

    def test_raises_when_encoding_more_content_than_page_size(self):
        contents = ["foo", "bar", "baz", "qux"]

        mark_mgr = MarkManager(3)

        expected_message = (
            r"Contents unexpectedly have higher count 4 than page_size 3\."
        )
        with pytest.raises(ValueError, match=expected_message):
            mark_mgr.next(contents)

    def test_raises_when_encoded_mark_contains_unspecified_keys(self):
        """The mark being decoded cannot have more data than we tell the mark encoder about.
        """
        contents = [
            {"foo": "a", "bar": 1},
            {"foo": "b", "bar": 2},
            {"foo": "c", "bar": 3},
        ]
        mark_mgr = MarkManager(3, ["foo", "bar"])

        # This mark is encoded with fields foo and bar
        mark = mark_mgr.next(contents)

        # Building a new encoder which only handles 'bar'.
        mark_encoder2 = MarkManager(3, ["bar"])

        # Raises you have tried to decode a mark  with data your code didn't specify..
        expected_message = (
            r"Decoded mark contained unspecified data\. "
            r"Expected \['bar'\], found \['bar', 'foo'\]\."
        )
        with pytest.raises(RuntimeError, match=expected_message):
            mark_encoder2.load(mark)
