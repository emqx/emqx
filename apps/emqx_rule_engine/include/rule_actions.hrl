-compile({parse_transform, emqx_rule_actions_trans}).

-type selected_data() :: map().
-type env_vars() :: map().
-type bindings() :: list(#{atom() => term()}).

-define(BINDING_KEYS, '__bindings__').

-define(bound_v(Key, ENVS0),
    maps:get(Key,
        maps:get(?BINDING_KEYS, ENVS0, #{}))).
