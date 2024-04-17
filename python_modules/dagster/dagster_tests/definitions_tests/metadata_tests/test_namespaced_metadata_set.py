from typing import Optional

import pytest
from dagster import FloatMetadataValue, IntMetadataValue, UrlMetadataValue
from dagster._core.definitions.metadata import NamespacedMetadataSet
from pydantic import ValidationError


def test_extract_primitive_coercion():
    class MyMetadataSet(NamespacedMetadataSet):
        primitive_int: Optional[int] = None
        primitive_float: Optional[float] = None
        int_metadata_value: Optional[IntMetadataValue] = None
        url_metadata_value: Optional[UrlMetadataValue] = None

        @classmethod
        def namespace(cls) -> str:
            return "foo"

    assert MyMetadataSet.extract({"foo/primitive_int": 5}).primitive_int == 5
    assert MyMetadataSet.extract({"foo/primitive_float": 5}).primitive_float == 5
    assert MyMetadataSet.extract({"foo/primitive_int": IntMetadataValue(5)}).primitive_int == 5
    assert (
        MyMetadataSet.extract({"foo/primitive_float": FloatMetadataValue(5.0)}).primitive_float == 5
    )
    assert MyMetadataSet.extract(
        {"foo/int_metadata_value": IntMetadataValue(5)}
    ).int_metadata_value == IntMetadataValue(5)

    with pytest.raises(
        ValidationError, match="Input should be a valid dictionary or instance of IntMetadataValue"
    ):
        MyMetadataSet.extract({"foo/int_metadata_value": 5})
