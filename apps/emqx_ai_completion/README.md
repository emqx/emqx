# EMQX AI Completion

## Create Providers

```bash
curl -s -u key:secret -X POST -H "Content-Type: application/json" http://localhost:18083/api/v5/ai/providers \
-d '{
    "name": "openai-provider",
    "type": "openai",
    "api_key": "'${EMQX_OPENAI_API_KEY}'"
}'
```

List all providers
```bash
curl -s -u key:secret -X GET -H "Content-Type: application/json" "http://localhost:18083/api/v5/ai/providers" | jq
```

## Create Completion Profile

```bash
curl -s -u key:secret -X POST -H "Content-Type: application/json" http://localhost:18083/api/v5/ai/completion_profiles \
-d '{
    "name": "openai-profile",
    "type": "openai",
    "model": "gpt-4o",
    "provider_name": "openai-provider",
    "system_prompt": "pls add numbers. print the result only."
}'
```

List all completion profiles
```bash
curl -s -u key:secret -X GET -H "Content-Type: application/json" "http://localhost:18083/api/v5/ai/completion_profiles" | jq
```

## Create Rule
```bash
curl -s -u key:secret -X POST -H "Content-Type: application/json" http://localhost:18083/api/v5/rules \
-d '{
    "sql":
        "SELECT
            ai_completion('"'"'openai-profile'"'"', payload) as result
        FROM
            '"'"'t/#'"'"'
    ",
    "name": "ai_completion_rule",
    "description": "AI Completion Rule",
    "actions": [{
        "function": "republish",
        "args": {
            "topic": "republish_topic",
            "qos": 0,
            "retain": false,
            "payload": "${result}",
            "mqtt_properties": {},
            "user_properties": "",
            "direct_dispatch": false
        }
    }]
}' | jq
```

List all rules
```bash
curl -s -u key:secret http://localhost:18083/api/v5/rules | jq
```

## OpenAI Decisions

Use an `openai_decisions` provider with an `openai_decisions` completion profile.
The profile sends text input and configured questions to `POST /v1/decisions`.
The default model is `gpt-6-luna`.

Create the provider:

```bash
curl -s -u key:secret -X POST -H "Content-Type: application/json" \
  http://localhost:18083/api/v5/ai/providers \
  -d '{
    "name": "decisions-provider",
    "type": "openai_decisions",
    "api_key": "'${OPENAI_API_KEY}'"
  }'
```

Create the completion profile:

```bash
curl -s -u key:secret -X POST -H "Content-Type: application/json" \
  http://localhost:18083/api/v5/ai/completion_profiles \
  -d '{
    "name": "detect-fault",
    "type": "openai_decisions",
    "provider_name": "decisions-provider",
    "questions": [{
      "type": "predicate",
      "name": "fault",
      "instructions": "Does this device report indicate a hardware fault?"
    }]
  }'
```

Each question must have a unique, non-empty `name` and non-empty `instructions`.
Configure one of these question types:

- `predicate` returns a `probability` between zero and one.
- `choice` requires a non-empty `choices` array. Each choice has a unique string
  or boolean `value` and an optional `description`.
- `score` requires a non-empty `levels` array ordered from lowest to highest.
  Each level has a non-empty `label` and an optional `description`.
  The returned score is a probability-weighted average of zero-based level indices.

Call the profile from rule SQL:

```sql
SELECT
  json_decode(ai_completion('detect-fault', payload)) AS decision
FROM 'devices/+/status'
```

The function returns a JSON string with an `answers` array:

```json
{"answers":[{"type":"predicate","name":"fault","probability":0.92}]}
```

The result preserves answer names, types, probabilities, confidence values, and
refusals returned by OpenAI. Check the answer type before reading its value.
Apply thresholds in the rule or downstream application.
HTTP and transport failures are logged and return an empty string, as with other
completion profiles. Check for an empty result before calling `json_decode()`.
Decoding an empty string raises a JSON parse error and fails the rule evaluation.

Decision profiles accept text input through `ai_completion(profile, data)`.
The three-argument prompt-override form is rejected because instructions belong
to individual questions. Configure questions in the profile instead.

See the [OpenAI Decisions guide](https://developers.openai.com/api/docs/guides/decisions)
for question examples and API availability.

### Live smoke test

Set `OPENAI_API_KEY` to run the smoke test against the OpenAI Decisions API:

```bash
TERM=dumb SUITES=emqx_ai_completion_decision_smoke_SUITE make apps/emqx_ai_completion-ct
```

The suite skips when `OPENAI_API_KEY` is unset or empty.
It checks that a predicate answer contains the expected name and a probability
between zero and one.

## Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
