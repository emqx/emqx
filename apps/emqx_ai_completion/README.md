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

## Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
