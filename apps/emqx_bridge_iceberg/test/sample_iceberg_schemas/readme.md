Some examples here are taken from `python-iceberg`:
https://github.com/apache/iceberg-python/blob/7a6a7c8da4ac3695903e850b3b180fb539aac0a3/tests/conftest.py#L142

Examples taken from above:
- `simple`
- `nested`
- `nested_with_struct_key_map`
- `full_nested_fields`
- `schema_with_all_types`

Expected outputs were generated with
`schema_conversion.ConvertSchemaToAvro().iceberg_to_avro`.

For example:

```python
from pyiceberg import schema
from pyiceberg.schema import Schema
from pyiceberg.utils.schema_conversion import AvroSchemaConversion

pr = lambda s: print(s.model_dump_json(indent=2))
pra = lambda s: print(json.dumps(AvroSchemaConversion().iceberg_to_avro(s), indent=2))

simple = schema.Schema(
    NestedField(field_id=1, name="foo", field_type=StringType(), required=False),
    NestedField(field_id=2, name="bar", field_type=IntegerType(), required=True),
    NestedField(field_id=3, name="baz", field_type=BooleanType(), required=False),
    schema_id=1,
    identifier_field_ids=[2],
)

# Input
pr(simple)

# Output
pra(simple)
```

##### Notes

In the original implementation, when we have a nested map type, it seems that the inner
key field id ends up being used for both the inner and outer key field ids, which sounds
wrong.

From https://iceberg.apache.org/docs/1.8.1/schemas/ :

> Iceberg tracks each field in a table schema using an ID that is never reused in a table.

So, the generated expected output for the `nested` and `nested_with_struct_key_map` cases
(root field `quux`) was adjusted to accomodate this difference in implementation: the
field ids for the nested keys and values were changed to match their original fields,
rather than repeating them.
