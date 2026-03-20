# Subscription Message Filter BNF

This document describes the filter-expression syntax accepted by
`emqx_subscription_filter` for subscriptions of the form:

```text
<topic-filter> "?" <filter-expression>
```

The `?` suffix is interpreted as a subscription message filter only when
`mqtt.subscription_message_filter = enable`. Otherwise `?` remains part of the
plain MQTT topic filter.

## Grammar

```bnf
subscription      ::= topic-filter [ "?" filter-expression ]

filter-expression ::= clause *( ws? "&" ws? clause )

clause            ::= equality-clause | numeric-clause

equality-clause   ::= key ws? "=" ws? string-value

numeric-clause    ::= key ws? numeric-op ws? number

numeric-op        ::= ">" | "<" | ">=" | "<="

key               ::= token

string-value      ::= token

number            ::= token

token             ::= 1*( non-empty-octet )
                    ; lexical validity is further constrained by the
                    ; operator-splitting rules below

ws                ::= 1*( SP | HTAB | CR | LF )
```

## Parsing Rules

1. The topic filter is split at the first `?`.
2. The filter expression must be non-empty after trimming surrounding whitespace.
3. Clauses are separated only by `&`. This is logical AND. There is no OR, NOT,
   grouping, or precedence.
4. Each clause must contain exactly one operator.
5. Leading and trailing whitespace is trimmed:
   around the whole expression, around each clause, and around each key/value.
6. `=` keeps the right-hand side as a string.
7. `>`, `<`, `>=`, and `<=` require the right-hand side to parse as a number.
8. An empty key or empty value makes the whole expression invalid.
9. Because each clause must split into exactly two parts, unescaped operator
   characters inside a key or value are not supported.

## Matching Semantics

The filter is evaluated only against MQTT 5 `User-Property` entries.

```text
key=value
```

matches when any user-property with the same key has exactly that value.

```text
key>25
key<=25
```

matches when any user-property with the same key can be parsed as a number and
satisfies the comparison.

All clauses in the expression must match for the subscription to match.

## Invalid Examples

```text
topic?
topic?location
topic?=roomA
topic?location=
topic?location=roomA&
topic?location=roomA&&value>25
topic?value>abc
topic?value=1=2
```

## Valid Examples

```text
sensor/+/temperature?location=roomA
sensor/+/temperature?location=roomA&value>25
$share/g/sensor/+/temperature?location = roomA & value >= 25
```

## Additional Constraints

- Filtered subscriptions are rejected for `$queue/...`, `$q/...`,
  `$stream/...`, and `$s/...` topic filters.
- Unsubscribe requests strip the `?filter-expression` suffix before topic-filter
  validation when the feature is enabled.
