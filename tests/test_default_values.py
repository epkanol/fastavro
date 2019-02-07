from fastavro import writer as fastavro_writer
from fastavro.read import SchemaResolutionError
import fastavro
import unittest

import pytest

from io import BytesIO

schema_dict_a = {
    "namespace": "example.avro2",
    "type": "record",
    "name": "evtest",
    "fields": [
        {"name": "a", "type": ["null", "string"], "default": "abc"}
    ]
}

schema_dict_a_def = {
    "namespace": "example.avro2",
    "type": "record",
    "name": "evtest",
    "fields": [
        {"name": "a", "type": ["null", "string"], "default": "def"}
    ]
}

empty_dict = { }

def avro_to_bytes_with_schema(avro_schema, avro_dict):
    with BytesIO() as bytes_io:
        fastavro_writer(bytes_io, avro_schema, [avro_dict])
        return bytes_io.getvalue()


def bytes_with_schema_to_avro(avro_read_schema, binary):
    with BytesIO(binary) as bytes_io:
        reader = fastavro.reader(bytes_io, avro_read_schema)
        return next(reader)


class TestDefaultValues(unittest.TestCase):

    def test_empty_dict_has_abc(self):
        empty_bytes = avro_to_bytes_with_schema(schema_dict_a, empty_dict)
        record_a = bytes_with_schema_to_avro(schema_dict_a, empty_bytes)
        assert record_a["a"] == "abc"

    def test_empty_dict_serialized_back_to_bytes_and_read_with_other_schema_has_def(self):
        empty_bytes = avro_to_bytes_with_schema(schema_dict_a, empty_dict)
        record_a = bytes_with_schema_to_avro(schema_dict_a, empty_bytes)
        # one could argue that, because we did not operate on record_a,
        # the byte representation should not automatically "fill in" the
        # default values
        bytes_again = avro_to_bytes_with_schema(schema_dict_a, record_a)
        #
        # reading the bytes again, with a schema with a different default value
        record_again = bytes_with_schema_to_avro(schema_dict_a_def, bytes_again)
        # should really have been def here
        #assert record_again["a"] == "def"
        assert record_again["a"] == "abc"



if __name__ == '__main__':
    unittest.main()
