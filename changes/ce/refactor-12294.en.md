Refactor routes cleanup implementation:

- avoid creating node monitors by `emqx_router_helper` since it already subscribes to all necessary node events via `ekka:monitor/1`
- eliminate a potential risk of cleaning up the newly added routes of a quickly restarted node.
