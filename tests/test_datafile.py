import os
import ast
import unittest

from fastavro import writer, reader, parse_schema

def get_unions_schema():
  test_dir = os.path.dirname(os.path.abspath(__file__))
  schema_json_path = os.path.join(test_dir, 'unions.avsc')
  with open(schema_json_path, 'r') as f:
    schema_json = f.read()
  schema_dict = ast.literal_eval(schema_json)
  return parse_schema(schema_dict)


UNIONS_SCHEMA = get_unions_schema()

class TestDataFileUnions(unittest.TestCase):
  def __default_java_datum(self):
      datum = {
          'intOrLong': 0,
          'longOrInt': 0,
          'intOrLongNullable': None,
          'longOrIntNullable': None,
          'longOrFloatOrDouble': 0,
          'floatOrDouble': 0.0,
          'boolOrIntOrLongOrFloatOrDouble': False,
          'boolOrIntOrLongOrFloatOrDoubleNullable': None,
          'booleanNullable': None,
          'nullableBoolean': None,

      }
      return datum

  parameters = [
      ('intOrLong', ('int', 0), 'intOrLongZero'),
      ('intOrLong', ('int', 1), 'intOrLongOne'),
      ('longOrInt', ('long', 0), 'longOrIntZeroLong'),
      ('intOrLong', ('long', 0), 'intOrLongZeroLong'),
      ('intOrLong', ('long', 1), 'intOrLongOneLong'),
      ('longOrInt', ('int', 0), 'longOrIntZero'),
      ('longOrInt', ('int', 1), 'longOrIntOne'),
      ('longOrInt', ('long', 1), 'longOrIntOneLong'),
      ('intOrLongNullable', None, 'intOrLongNullableNull'),
      ('intOrLongNullable', ('int', 0), 'intOrLongNullableZero'),
      ('intOrLongNullable', ('long', 0), 'intOrLongNullableZeroLong'),
      ('longOrFloatOrDouble', ('long', 0), 'longOrFloatOrDoubleZero'),
      ('longOrFloatOrDouble', ('float', 0.0), 'longOrFloatOrDoubleZeroFloat'),
      ('longOrFloatOrDouble', ('double', 0.0), 'longOrFloatOrDoubleZeroDouble'),
      ('floatOrDouble', ('float', 0.0), 'floatOrDoubleZero'),
      ('floatOrDouble', ('float', 1.0), 'floatOrDoubleOne'),
      ('floatOrDouble', ('double', 0.0), 'floatOrDoubleZeroDouble'),
      ('floatOrDouble', ('double', 1.0), 'floatOrDoubleOneDouble'),
      ('booleanNullable', None, 'booleanNullableNull'),
      ('booleanNullable', False, 'booleanNullableFalse'),
      ('booleanNullable', True, 'booleanNullableTrue'),
      ('nullableBoolean', None, 'nullableBooleanNull'),
      ('nullableBoolean', False, 'nullableBooleanFalse'),
      ('nullableBoolean', True, 'nullableBooleanTrue'),

  ]

  def test_read_from_java(self):
      for p in self.parameters:
          with self.subTest(p, params=p):
              self.__validate_file(p)

  def test_write_to_python(self):
      for p in self.parameters:
          with self.subTest(p, params=p):
              self.__generate_file(p)


  def __validate_file(self, p):
      param = p[0]
      if callable(p[1]):
          value = p[1]()
      else:
          value = p[1]
      if isinstance(value, tuple):
          value = value[1]
      file = p[2]
      java_datum = self.__default_java_datum()
      java_datum[param] = value
      with open("/tmp/java/unions/" + file + ".avro", 'rb') as fo:
        for datum in reader(fo):
          self.assertEqual(java_datum, datum)

  def __generate_file(self, p):
      param = p[0]
      if callable(p[1]):
          value = p[1]()
      else:
          value = p[1]
      file = p[2]
      java_datum = self.__default_java_datum()
      java_datum[param] = value
      with open("/tmp/fastavro/unions/" + file + ".avro", 'wb') as out:
          writer(out, UNIONS_SCHEMA, [java_datum])


if __name__ == '__main__':
    unittest.main()
