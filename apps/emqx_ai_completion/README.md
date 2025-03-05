# EMQX AI Completion

## Creating Rule

```bash
curl -s -u key:secret -X POST -H "Content-Type: application/json" http://localhost:18083/api/v5/rules \
-d '{
    "sql": "SELECT * FROM \"t/#\"",
    "name": "ai_completion_rule",
    "description": "AI Completion Rule",
    "actions": [{
        "function": "emqx_ai_completion:action",
        "args": {
            "tool_names": ["publish"],
            "prompt": "The user provides MQTT messages in JSON format.\nFor user message, if the payload has comma-separated integer values\n* calculate the sum of the values\n* use the sum as the payload of a new message\n* publish the new message to the topic ai_completion/{original_topic}\n* use the qos of the original message as the qos of the new message.\n\nDo nothing for other messages."
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

## License

EMQ Business Source License 1.1, refer to [LICENSE](BSL.txt).
