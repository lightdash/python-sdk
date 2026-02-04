"""Tests for the unified result types."""
import json
import warnings
import pytest

from lightdash.results import ResultSet, BaseResult
from lightdash.sql_runner import SqlResult


class TestResultSetProtocol:
    """Tests for the ResultSet protocol."""

    def test_sql_result_implements_protocol(self):
        """SqlResult should implement the ResultSet protocol."""
        result = SqlResult(
            rows=[{"a": 1, "b": 2}],
            columns=["a", "b"]
        )
        assert isinstance(result, ResultSet)

    def test_protocol_requires_to_df(self):
        """Protocol should require to_df method."""
        assert hasattr(ResultSet, "to_df")

    def test_protocol_requires_to_records(self):
        """Protocol should require to_records method."""
        assert hasattr(ResultSet, "to_records")

    def test_protocol_requires_iter(self):
        """Protocol should require __iter__ method."""
        assert hasattr(ResultSet, "__iter__")

    def test_protocol_requires_len(self):
        """Protocol should require __len__ method."""
        assert hasattr(ResultSet, "__len__")


class TestSqlResult:
    """Tests for SqlResult with unified interface."""

    def test_len(self):
        """SqlResult should support len()."""
        result = SqlResult(
            rows=[{"a": 1}, {"a": 2}, {"a": 3}],
            columns=["a"]
        )
        assert len(result) == 3

    def test_iter(self):
        """SqlResult should support iteration."""
        rows = [{"a": 1}, {"a": 2}, {"a": 3}]
        result = SqlResult(rows=rows, columns=["a"])

        iterated = list(result)
        assert iterated == rows

    def test_for_loop(self):
        """SqlResult should support for loop."""
        result = SqlResult(
            rows=[{"a": 1}, {"a": 2}],
            columns=["a"]
        )

        collected = []
        for row in result:
            collected.append(row["a"])

        assert collected == [1, 2]

    def test_to_records(self):
        """SqlResult.to_records() should return rows."""
        rows = [{"a": 1}, {"a": 2}]
        result = SqlResult(rows=rows, columns=["a"])

        assert result.to_records() == rows

    def test_to_json_str(self):
        """SqlResult.to_json_str() should return JSON string."""
        rows = [{"a": 1}, {"a": 2}]
        result = SqlResult(rows=rows, columns=["a"])

        json_str = result.to_json_str()
        assert json_str == '[{"a": 1}, {"a": 2}]'
        assert json.loads(json_str) == rows

    def test_to_json_deprecated(self):
        """SqlResult.to_json() should emit deprecation warning."""
        result = SqlResult(rows=[{"a": 1}], columns=["a"])

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            data = result.to_json()

            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "to_records()" in str(w[0].message)
            assert data == [{"a": 1}]


class TestBaseResultInheritance:
    """Tests for BaseResult inheritance."""

    def test_sql_result_is_base_result(self):
        """SqlResult should be a BaseResult."""
        result = SqlResult(rows=[], columns=[])
        assert isinstance(result, BaseResult)

    def test_inherited_to_json_str(self):
        """BaseResult subclasses should inherit to_json_str."""
        result = SqlResult(rows=[{"x": 10}], columns=["x"])
        assert result.to_json_str() == '[{"x": 10}]'

    def test_inherited_to_json_deprecation(self):
        """BaseResult subclasses should inherit to_json deprecation."""
        result = SqlResult(rows=[{"x": 10}], columns=["x"])

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result.to_json()
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
