# OpenAI-Compatible Tool Schema Subset

Status: draft
Last reviewed: 2026-05-06

## 1. Purpose

This document defines the limited schema format supported by the agent for externally configured tools.

The goal is not to implement full JSON Schema, OpenAPI, or Swagger. The goal is to support a small, deterministic subset of the OpenAI `strict: true` function-tool schema format so that:

1. Tool definitions supplied by users can be checked before they are sent to an OpenAI-compatible API.
2. Tool call arguments produced by the model can be validated before the tool handler is executed.
3. Tool responses can be validated locally before they are sent back to the model.

The schema is supplied externally as JSON. The agent must not require tool authors to write Erlang code or use a custom schema DSL.

## 2. References

Primary references:

- OpenAI Structured Outputs guide: https://developers.openai.com/api/docs/guides/structured-outputs
- OpenAI Function Calling guide: https://developers.openai.com/api/docs/guides/function-calling

The supported subset in this document is intentionally smaller than OpenAI's full documented Structured Outputs subset.

## 3. Non-goals

The agent does not support:

- Full JSON Schema.
- OpenAPI or Swagger schema semantics.
- Schema recursion.
- `$ref` or `$defs`.
- Top-level `anyOf`.
- `oneOf` or `allOf` at any level.
- Discriminated unions as a JSON Schema feature. Model variants with nested `anyOf` and single-value `enum` discriminator fields instead.
- Open objects.
- Optional missing object properties.
- Provider-specific tool response schema extensions.

## 4. Accepted external tool definition shapes

The agent should target the OpenAI Responses API function-tool shape and normalize accepted tool definitions to that flat representation internally.

Chat-Completions-style nested function tools may be accepted as migration input, but they should not be the internal representation. After normalization, every function tool should have this shape:

```json
{
  "type": "function",
  "name": "get_weather",
  "description": "Get current weather for a location.",
  "strict": true,
  "parameters": {
    "type": "object",
    "properties": {},
    "required": [],
    "additionalProperties": false
  }
}
```

### 4.1 Responses-style flat function tool

```json
{
  "type": "function",
  "name": "get_weather",
  "description": "Get current weather for a location.",
  "strict": true,
  "parameters": {
    "type": "object",
    "properties": {
      "location": {
        "type": "string",
        "description": "City and country, for example: Paris, France."
      },
      "units": {
        "type": ["string", "null"],
        "enum": ["celsius", "fahrenheit"],
        "description": "Temperature units, or null for the default."
      }
    },
    "required": ["location", "units"],
    "additionalProperties": false
  }
}
```

### 4.2 Chat-Completions-style nested function tool

```json
{
  "type": "function",
  "function": {
    "name": "get_weather",
    "description": "Get current weather for a location.",
    "strict": true,
    "parameters": {
      "type": "object",
      "properties": {
        "location": {
          "type": "string",
          "description": "City and country, for example: Paris, France."
        },
        "units": {
          "type": ["string", "null"],
          "enum": ["celsius", "fahrenheit"],
          "description": "Temperature units, or null for the default."
        }
      },
      "required": ["location", "units"],
      "additionalProperties": false
    }
  }
}
```

## 5. Tool definition validation rules

At registration time, validate the tool definition before adding it to the registry.

### 5.1 Required tool fields

A normalized function tool must contain:

| Field | Required | Type | Notes |
|---|---:|---|---|
| `type` | yes | string | Must be `"function"`. |
| `name` | yes | string | Tool name used by model calls. |
| `description` | no | string | Strongly recommended. |
| `strict` | yes | boolean | Must be `true`. |
| `parameters` | yes | object | Must be a valid root object schema from this document. |

### 5.2 Tool name rules

Recommended rule:

```text
^[A-Za-z0-9_-]{1,64}$
```

Reject names outside this pattern.

### 5.3 Strict mode

Require:

```json
"strict": true
```

Do not silently assume strict mode. Reject omitted or false `strict` in externally supplied tools.

Reason: different OpenAI-compatible providers and OpenAI API surfaces may differ in default behavior. Requiring explicit strict mode avoids ambiguity.

## 6. Supported schema data type

The agent supports a small schema type called `SimpleOpenAISchema`.

Conceptually:

```text
SimpleOpenAISchema = RootObjectSchema

RootObjectSchema = ObjectSchema where nullable is not allowed

FieldSchema =
    ScalarSchema
  | NullableScalarSchema
  | EnumSchema
  | ArraySchema
  | NullableArraySchema
  | NestedObjectSchema
  | NullableNestedObjectSchema
  | AnyOfSchema
```

The root schema must always be an object. Nested fields may be scalar values, arrays, or non-recursive objects.

## 7. Root object schema

Every function `parameters` schema must be an object schema.

### 7.1 Required shape

```json
{
  "type": "object",
  "properties": {
    "field_name": {
      "type": "string"
    }
  },
  "required": ["field_name"],
  "additionalProperties": false
}
```

### 7.2 Root object rules

The root schema must satisfy all of these rules:

1. `type` must be exactly `"object"`.
2. `properties` must exist and must be a JSON object.
3. `required` must exist and must be an array of strings.
4. `required` must contain exactly every key from `properties`.
5. `additionalProperties` must exist and must be exactly `false`.
6. The root schema must not be nullable.
7. The root schema must not use `anyOf`, `$ref`, or `$defs`.

### 7.3 Empty argument object

For a tool with no arguments, use an explicit empty object schema:

```json
{
  "type": "object",
  "properties": {},
  "required": [],
  "additionalProperties": false
}
```

## 8. Object schemas

Nested objects use the same object rules as the root, except they may be nullable.

### 8.1 Non-null object field

```json
{
  "type": "object",
  "properties": {
    "id": {
      "type": "string"
    },
    "email": {
      "type": ["string", "null"],
      "format": "email"
    }
  },
  "required": ["id", "email"],
  "additionalProperties": false
}
```

### 8.2 Nullable object field

```json
{
  "type": ["object", "null"],
  "properties": {
    "id": {
      "type": "string"
    }
  },
  "required": ["id"],
  "additionalProperties": false
}
```

### 8.3 Runtime object validation

Given a schema object with properties `P` and required list `R`, validate a JSON object value as follows:

1. The value must be a JSON object.
2. Every key in `R` must be present.
3. No key outside `P` may be present.
4. Every property value must validate against its property schema.
5. The order of keys is irrelevant for validation.

## 9. Scalar schemas

Supported scalar types:

```json
{ "type": "string" }
{ "type": "integer" }
{ "type": "number" }
{ "type": "boolean" }
```

### 9.1 Runtime scalar mapping

| Schema type | JSON value | Erlang decoded value, typical |
|---|---|---|
| `string` | JSON string | binary |
| `integer` | JSON integer | integer |
| `number` | JSON number | integer or float |
| `boolean` | JSON boolean | `true` or `false` |
| `null` | JSON null | decoder-specific, often `null` |

The implementation should not hard-code the null representation if the JSON decoder can be configured. Prefer passing the expected null value as an option.

### 9.2 Integer vs number

For `integer`, accept only whole JSON numbers decoded as Erlang integers.

For `number`, accept integers and floats.

Do not accept strings containing numbers.

## 10. Nullable schemas

Strict function schemas require all fields to be present. Optionality is represented by allowing `null` as a value, not by omitting the key.

### 10.1 Supported nullable form

Only this two-element form is supported:

```json
{ "type": ["string", "null"] }
```

Equivalent order is also accepted:

```json
{ "type": ["null", "string"] }
```

### 10.2 Allowed nullable types

Nullable type arrays may contain exactly one real type plus `"null"`:

```json
{ "type": ["string", "null"] }
{ "type": ["integer", "null"] }
{ "type": ["number", "null"] }
{ "type": ["boolean", "null"] }
{ "type": ["array", "null"], "items": { "type": "string" } }
{ "type": ["object", "null"], "properties": {}, "required": [], "additionalProperties": false }
```

Reject type arrays with more than two entries.

Reject type arrays that do not include `"null"`.

Reject type arrays with more than one non-null type.

## 11. Enum schemas

Enums constrain a scalar value to a fixed set of values.

### 11.1 Non-null enum

```json
{
  "type": "string",
  "enum": ["celsius", "fahrenheit"]
}
```

Runtime validation:

1. Validate the value type first.
2. Check exact membership in `enum`.

### 11.2 Nullable enum

```json
{
  "type": ["string", "null"],
  "enum": ["celsius", "fahrenheit"]
}
```

Runtime validation policy for this project:

1. If the value is `null` and the schema type allows `null`, accept it.
2. If the value is not `null`, validate the non-null type.
3. If the value is not `null`, require exact membership in `enum`.

This is an OpenAI-compatibility policy for the agent's limited schema subset. It is not intended to be a complete implementation of general JSON Schema enum semantics.

### 11.3 Enum value types

For the first implementation, support enum values of these JSON types only:

- string
- integer
- number
- boolean
- null, only if the schema type allows null

Reject object or array enum values.

## 12. Array schemas

Arrays must be homogeneous. Tuple-style arrays are not supported.

### 12.1 Required shape

```json
{
  "type": "array",
  "items": {
    "type": "string"
  }
}
```

### 12.2 Nullable array

```json
{
  "type": ["array", "null"],
  "items": {
    "type": "integer"
  }
}
```

### 12.3 Array item rules

The `items` field is required for every array schema.

The `items` value must be a valid field schema.

The `items` schema must not use recursion, `$ref`, or `$defs`.

### 12.4 Optional array constraints

The agent may support:

```json
{
  "type": "array",
  "items": { "type": "string" },
  "minItems": 1,
  "maxItems": 20
}
```

Validation rules:

1. `minItems`, if present, must be a non-negative integer.
2. `maxItems`, if present, must be a non-negative integer.
3. If both are present, `minItems <= maxItems`.
4. Runtime array length must satisfy both constraints.

## 13. Combinator schemas

The agent supports `anyOf` as a field schema, but **not** at the root of `parameters`.

OpenAI strict function schemas do not support `oneOf` or `allOf`. Reject them in all schemas.

```json
{
  "anyOf": [
    { "type": "string" },
    { "type": "integer" }
  ]
}
```

### 13.1 Rules

1. `anyOf` must be an array of valid field schemas.
2. The array must be non-empty.
3. Each subschema is validated as a field schema, not as a root schema.
4. Subschemas must not use recursion, `$ref`, or `$defs`.
5. A field schema must not have both `type` and `anyOf`.

### 13.2 Runtime validation

The value is valid if at least one `anyOf` subschema validates.

### 13.3 Modeling variants

To model variants, use `anyOf` with object branches that have a single-value `enum` discriminator. This makes the branches mutually exclusive in practice without using unsupported `oneOf` or `const`.

For example, a pipeline step array can be modeled like this:

```json
{
  "type": "array",
  "items": {
    "anyOf": [
      {
        "type": "object",
        "properties": {
          "id": { "type": "string" },
          "type": { "type": "string", "enum": ["call_tool"] },
          "tool": { "type": "string" },
          "args": {
            "type": ["object", "null"],
            "properties": {},
            "required": [],
            "additionalProperties": false
          },
          "result_path": { "type": ["string", "null"] }
        },
        "required": ["id", "type", "tool", "args", "result_path"],
        "additionalProperties": false
      },
      {
        "type": "object",
        "properties": {
          "id": { "type": "string" },
          "type": { "type": "string", "enum": ["llm_loop"] },
          "provider_name": { "type": "string" },
          "model": { "type": "string" },
          "instructions": { "type": "string" },
          "tools": {
            "type": ["array", "null"],
            "items": { "type": "string" }
          },
          "set_result_schema": {
            "type": "object",
            "properties": {
              "status": { "type": "string", "enum": ["ok", "error"] }
            },
            "required": ["status"],
            "additionalProperties": false
          },
          "result_path": { "type": "string" }
        },
        "required": [
          "id",
          "type",
          "provider_name",
          "model",
          "instructions",
          "tools",
          "set_result_schema",
          "result_path"
        ],
        "additionalProperties": false
      }
    ]
  }
}
```

## 14. Optional type-specific constraints

The first implementation should support Level 1 only. Level 2 can be added if needed.

### 14.1 Level 1: safest core subset

Supported everywhere relevant:

- `type`
- `description`
- `properties`
- `required`
- `additionalProperties`
- `items`
- `enum`

### 14.2 Level 2: stricter local validation

Supported string constraints:

- `pattern`
- `format`

Supported string formats:

- `date-time`
- `time`
- `date`
- `duration`
- `email`
- `hostname`
- `ipv4`
- `ipv6`
- `uuid`

Supported number and integer constraints:

- `minimum`
- `maximum`
- `exclusiveMinimum`
- `exclusiveMaximum`
- `multipleOf`

Supported array constraints:

- `minItems`
- `maxItems`

Rule: if the agent accepts a keyword at schema registration time, it must enforce that keyword at runtime. If the agent does not enforce a keyword, reject schemas containing that keyword.

## 15. Rejected schema keywords

Reject these keywords in all schemas:

```text
$schema
$id
$ref
$defs
definitions
not
oneOf
allOf
if
then
else
dependentRequired
dependentSchemas
patternProperties
additionalItems
unevaluatedProperties
unevaluatedItems
contains
propertyNames
minProperties
maxProperties
default
examples
readOnly
writeOnly
nullable
const
```

Notes:

- `nullable: true` is an OpenAPI-style idiom. Reject it. Use `type: ["T", "null"]` instead.
- `default` is rejected because the agent should validate exactly what the model supplied, not silently mutate arguments during validation.
- `const` is rejected. Use a single-value `enum` instead.
- `oneOf` and `allOf` are rejected because they are not supported by OpenAI strict function schemas. Use nested `anyOf` with discriminator `enum` fields for variants.
- `$ref` and `$defs` are rejected to avoid recursion and reference resolution.

## 16. Accepted schema keywords by schema kind

### 16.1 Object schema

Required:

- `type`
- `properties`
- `required`
- `additionalProperties`

Optional:

- `description`

Allowed nullable form for nested objects only:

```json
"type": ["object", "null"]
```

### 16.2 String schema

Required:

- `type`

Optional Level 1:

- `description`
- `enum`

Optional Level 2:

- `pattern`
- `format`

Allowed nullable form:

```json
"type": ["string", "null"]
```

### 16.3 Integer schema

Required:

- `type`

Optional Level 1:

- `description`
- `enum`

Optional Level 2:

- `minimum`
- `maximum`
- `exclusiveMinimum`
- `exclusiveMaximum`
- `multipleOf`

Allowed nullable form:

```json
"type": ["integer", "null"]
```

### 16.4 Number schema

Required:

- `type`

Optional Level 1:

- `description`
- `enum`

Optional Level 2:

- `minimum`
- `maximum`
- `exclusiveMinimum`
- `exclusiveMaximum`
- `multipleOf`

Allowed nullable form:

```json
"type": ["number", "null"]
```

### 16.5 Boolean schema

Required:

- `type`

Optional Level 1:

- `description`
- `enum`

Allowed nullable form:

```json
"type": ["boolean", "null"]
```

### 16.6 Array schema

Required:

- `type`
- `items`

Optional Level 1:

- `description`

Optional Level 2:

- `minItems`
- `maxItems`

Allowed nullable form:

```json
"type": ["array", "null"]
```

### 16.7 Combinator schema

A field schema may use `anyOf` instead of a direct `type`.

Required:

- `anyOf`

Rules:

- Must be a non-empty array of valid field schemas.
- Subschemas may not be root object schemas (they are validated as fields).
- Not allowed at the root of `parameters`.

## 17. Registration-time validation

When a tool definition is loaded from external configuration, perform these checks:

1. Parse the tool configuration JSON.
2. Detect whether the tool uses the flat or nested OpenAI function-tool shape.
3. Normalize to a common internal representation.
4. Validate `type == "function"`.
5. Validate the function name.
6. Validate `strict == true`.
7. Validate `parameters` as a root object schema.
8. Reject all unsupported schema keywords.
9. Validate each schema node recursively, without allowing `$ref` or recursion.
10. Store the validated tool definition in the registry.

A tool with an invalid schema must not be registered.

## 18. Runtime tool-call argument validation

The model's tool call arguments are untrusted input.

The agent implementation should use the OpenAI Responses API. Chat Completions examples are kept only to support migration from existing code and configurations.

### 18.1 Responses API function call shape

Typical relevant fields:

```json
{
  "type": "function_call",
  "call_id": "call_abc123",
  "name": "get_weather",
  "arguments": "{\"location\":\"Paris, France\",\"units\":null}"
}
```

### 18.2 Chat Completions tool call shape

Typical relevant fields:

```json
{
  "id": "call_abc123",
  "type": "function",
  "function": {
    "name": "get_weather",
    "arguments": "{\"location\":\"Paris, France\",\"units\":null}"
  }
}
```

### 18.3 Runtime validation flow

For each requested tool call:

1. Extract the tool name.
2. Check that the tool name exists in the registry.
3. Extract the `arguments` string.
4. Decode `arguments` as JSON.
5. Require the decoded value to be a JSON object.
6. Validate the decoded object against the registered tool's `parameters` schema.
7. Reject the call if validation fails.
8. Execute the tool only after successful validation.

### 18.4 Runtime argument validation errors

Return structured internal errors for at least these cases:

| Error code | Meaning |
|---|---|
| `unknown_tool` | The model requested a tool not present in the registry. |
| `invalid_arguments_json` | The `arguments` field was not valid JSON. |
| `arguments_not_object` | The decoded arguments value was not a JSON object. |
| `missing_required_property` | A required property was absent. |
| `unexpected_property` | An object contained a key not declared in `properties`. |
| `wrong_type` | A value did not match the schema type. |
| `enum_mismatch` | A non-null value was not present in `enum`. |
| `array_item_invalid` | An array item failed validation. |
| `any_of_no_match` | None of the `anyOf` subschemas validated. |
| `constraint_failed` | A Level 2 constraint failed. |

Recommended error structure:

```json
{
  "error": "wrong_type",
  "path": ["items", 0, "id"],
  "expected": "string",
  "actual": "integer"
}
```

## 19. Tool response schema validation

OpenAI function tool definitions describe the input arguments through `parameters`. They do not define a standard function result schema field in the normal function tool definition.

Therefore:

1. Tool response schemas are local to the agent.
2. Do not include local response schemas in the OpenAI function-tool payload unless a specific OpenAI-compatible provider explicitly documents such an extension.
3. Use the same `SimpleOpenAISchema` subset for response schemas.
4. Validate the tool result before sending it back to the model.

### 19.1 Recommended response schema shape

A local response schema should be just a schema object, for example:

```json
{
  "type": "object",
  "properties": {
    "temperature": {
      "type": "number"
    },
    "units": {
      "type": "string",
      "enum": ["celsius", "fahrenheit"]
    },
    "summary": {
      "type": "string"
    }
  },
  "required": ["temperature", "units", "summary"],
  "additionalProperties": false
}
```

The response schema is not a new DSL. It is the same restricted schema object used for `parameters`, supplied separately in the agent's local configuration.

## 20. Examples

### 20.1 Valid no-argument tool

```json
{
  "type": "function",
  "name": "get_server_time",
  "description": "Return the server time.",
  "strict": true,
  "parameters": {
    "type": "object",
    "properties": {},
    "required": [],
    "additionalProperties": false
  }
}
```

### 20.2 Valid tool with scalars and nullable enum

```json
{
  "type": "function",
  "name": "search_docs",
  "description": "Search project documents.",
  "strict": true,
  "parameters": {
    "type": "object",
    "properties": {
      "query": {
        "type": "string",
        "description": "Search query."
      },
      "limit": {
        "type": ["integer", "null"],
        "minimum": 1,
        "maximum": 100,
        "description": "Maximum number of results, or null for default."
      },
      "scope": {
        "type": ["string", "null"],
        "enum": ["all", "current_project"],
        "description": "Search scope, or null for default."
      }
    },
    "required": ["query", "limit", "scope"],
    "additionalProperties": false
  }
}
```

### 20.3 Valid runtime arguments

```json
{
  "query": "schema validator",
  "limit": null,
  "scope": "current_project"
}
```

### 20.4 Invalid runtime arguments: missing required nullable field

Invalid:

```json
{
  "query": "schema validator",
  "scope": "current_project"
}
```

Reason:

```text
limit is missing. Nullable means the value may be null; it does not mean the key may be omitted.
```

### 20.5 Invalid runtime arguments: unexpected property

Invalid:

```json
{
  "query": "schema validator",
  "limit": 10,
  "scope": "all",
  "debug": true
}
```

Reason:

```text
additionalProperties is false, and debug is not declared in properties.
```

### 20.6 Invalid schema: OpenAPI nullable idiom

Invalid:

```json
{
  "type": "string",
  "nullable": true
}
```

Use instead:

```json
{
  "type": ["string", "null"]
}
```

### 20.7 Invalid schema: missing additionalProperties

Invalid:

```json
{
  "type": "object",
  "properties": {
    "query": { "type": "string" }
  },
  "required": ["query"]
}
```

Reason:

```text
Every object schema must explicitly set additionalProperties to false.
```

### 20.8 Invalid schema: required does not match properties

Invalid:

```json
{
  "type": "object",
  "properties": {
    "query": { "type": "string" },
    "limit": { "type": ["integer", "null"] }
  },
  "required": ["query"],
  "additionalProperties": false
}
```

Reason:

```text
All properties must be required. The required list must be exactly ["query", "limit"].
```

## 21. Implementation checklist

Registration path:

- [ ] Parse external JSON config.
- [ ] Accept Responses-style flat function tools.
- [ ] Accept nested Chat-Completions function tools for migration.
- [ ] Normalize tool definitions internally to the Responses-style flat shape.
- [ ] Require `strict: true`.
- [ ] Validate tool name.
- [ ] Validate root `parameters` schema.
- [ ] Reject unsupported keywords.
- [ ] Reject recursion and references.
- [ ] Register only validated tools.

Runtime call path:

- [ ] Extract model-requested tool name.
- [ ] Lookup registered tool.
- [ ] Decode arguments JSON string.
- [ ] Require decoded arguments object.
- [ ] Validate arguments against schema.
- [ ] Execute tool only after validation.
- [ ] Validate tool result against local response schema, if configured.
- [ ] Return tool output to model only after response validation succeeds.

Validator behavior:

- [ ] Produce path-aware validation errors.
- [ ] Treat missing nullable fields as errors.
- [ ] Treat null as valid only when `type` explicitly includes `"null"`.
- [ ] Enforce `additionalProperties: false`.
- [ ] Enforce `required == properties.keys()` for every object schema.
- [ ] Keep schema validation and value validation separate.
- [ ] Support nested `anyOf` in field schemas.
- [ ] Reject root-level `anyOf`.
- [ ] Reject `oneOf` and `allOf` everywhere.

## 22. Recommended Erlang module boundaries

Actual modules in the agent plugin:

```text
emqx_agent_oai_tool_schema
  - validate schema documents at registration time

emqx_agent_oai_tool_value
  - validate decoded JSON values against validated schemas
```

These may be integrated into the existing tool registry and pipeline execution
paths rather than requiring separate `config`, `registry`, or `runner` modules.

Suggested public functions:

```erlang
validate_tool_definition(JsonTerm) -> {ok, Tool} | {error, Errors}.

validate_schema(Schema, root) -> ok | {error, Errors}.
validate_schema(Schema, field) -> ok | {error, Errors}.

validate_value(Schema, Value, Options) -> ok | {error, Errors}.

validate_tool_call(ToolRegistry, ToolCall, Options) ->
    {ok, ToolName, Args} | {error, Errors}.

validate_tool_result(Tool, Result, Options) ->
    ok | {error, Errors}.
```

Recommended options:

```erlang
#{
    null_value => null,
    constraint_level => 1,
    max_depth => 10,
    max_properties => 5000
}
```

## 23. Design principles

1. Accept only schemas the agent can enforce locally.
2. Prefer rejecting ambiguous schema features over partially implementing them.
3. Treat model tool-call arguments as untrusted input.
4. Keep OpenAI payload compatibility separate from local tool registry design.
5. Do not mutate arguments during validation.
6. Do not execute a tool until arguments validate successfully.
7. Do not send a tool result back to the model until the result validates successfully.
