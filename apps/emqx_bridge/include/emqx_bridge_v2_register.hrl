%% This function is called to register a bridge V2. It should be called before
%% the system boots in a function triggered by an -on_load() directive
%% since it should be called before the system boots because the config
%% system depends on that.
%%
%% It is placed in an hrl file instead of in emqx_bridge_v2.erl because emqx_bridge_v2
%% might not be loaded when the bridge module is loaded.
-spec emqx_bridge_v2_register_bridge_type(#{
    %% Should be provided by all bridges. Even if the bridge_v2_type_name is
    %% the same as the bridge_v1_type_named.
    'bridge_v1_type_name' := atom(),
    'bridge_v2_type_name' := atom(),
    'connector_type' := atom(),
    'schema_module' := atom(),
    'schema_struct_field' := atom() | binary()
}) -> ok.
emqx_bridge_v2_register_bridge_type(BridgeTypeInfo) ->
    try
        %% We must prevent overwriting so we take a lock when writing to persistent_term
        global:set_lock(
            {
                internal_emqx_bridge_v2_persistent_term_info_key(),
                internal_emqx_bridge_v2_persistent_term_info_key()
            },
            [node()],
            infinity
        ),
        internal_maybe_create_initial_bridge_v2_info_map(),
        internal_register_bridge_type_with_lock(BridgeTypeInfo)
    catch
        ErrorType:Reason:Stacktrace ->
            %% Print the error on standard output as logger might not be
            %% started yet
            io:format("~p~n", [
                #{
                    'error_type' => ErrorType,
                    'reason' => Reason,
                    'stacktrace' => Stacktrace,
                    'msg' => "Failed to register bridge V2 type"
                }
            ]),
            erlang:raise(ErrorType, Reason, Stacktrace)
    after
        global:del_lock(
            {
                internal_emqx_bridge_v2_persistent_term_info_key(),
                internal_emqx_bridge_v2_persistent_term_info_key()
            },
            [node()]
        )
    end,
    ok.

internal_register_bridge_type_with_lock(BridgeTypeInfo) ->
    InfoMap0 = persistent_term:get(internal_emqx_bridge_v2_persistent_term_info_key()),
    %% The Bridge V1 type is also a bridge V2 type due to backwards compatibility
    InfoMap1 = emqx_utils_maps:deep_force_put(
        [
            bridge_v2_type_names,
            maps:get(bridge_v1_type_name, BridgeTypeInfo)
        ],
        InfoMap0,
        true
    ),
    InfoMap2 = emqx_utils_maps:deep_force_put(
        [
            bridge_v2_type_names,
            maps:get(bridge_v2_type_name, BridgeTypeInfo)
        ],
        InfoMap1,
        true
    ),
    InfoMap3 = emqx_utils_maps:deep_force_put(
        [
            bridge_v1_type_to_bridge_v2_type,
            maps:get(bridge_v1_type_name, BridgeTypeInfo)
        ],
        InfoMap2,
        maps:get(bridge_v2_type_name, BridgeTypeInfo)
    ),
    %% Backwards compatibility
    InfoMap4 = emqx_utils_maps:deep_force_put(
        [
            bridge_v1_type_to_bridge_v2_type,
            maps:get(bridge_v2_type_name, BridgeTypeInfo)
        ],
        InfoMap3,
        maps:get(bridge_v2_type_name, BridgeTypeInfo)
    ),
    InfoMap5 = emqx_utils_maps:deep_force_put(
        [
            bridge_v2_type_to_connector_type,
            maps:get(bridge_v2_type_name, BridgeTypeInfo)
        ],
        InfoMap4,
        maps:get(connector_type, BridgeTypeInfo)
    ),
    %% Backwards compatibility
    InfoMap6 = emqx_utils_maps:deep_force_put(
        [
            bridge_v2_type_to_connector_type,
            maps:get(bridge_v1_type_name, BridgeTypeInfo)
        ],
        InfoMap5,
        maps:get(connector_type, BridgeTypeInfo)
    ),
    InfoMap7 = emqx_utils_maps:deep_force_put(
        [
            bridge_v2_type_to_schema_module,
            maps:get(bridge_v2_type_name, BridgeTypeInfo)
        ],
        InfoMap6,
        maps:get(schema_module, BridgeTypeInfo)
    ),
    InfoMap8 = emqx_utils_maps:deep_force_put(
        [
            bridge_v2_type_to_schema_struct_field,
            maps:get(bridge_v2_type_name, BridgeTypeInfo)
        ],
        InfoMap7,
        maps:get(schema_struct_field, BridgeTypeInfo)
    ),

    ok = persistent_term:put(internal_emqx_bridge_v2_persistent_term_info_key(), InfoMap8).

internal_maybe_create_initial_bridge_v2_info_map() ->
    case persistent_term:get(internal_emqx_bridge_v2_persistent_term_info_key(), undefined) of
        undefined ->
            ok = persistent_term:put(
                internal_emqx_bridge_v2_persistent_term_info_key(),
                #{
                    bridge_v2_type_names => #{},
                    bridge_v1_type_to_bridge_v2_type => #{},
                    bridge_v2_type_to_connector_type => #{},
                    bridge_v2_type_to_schema_module => #{},
                    bridge_v2_type_to_schema_struct_field => #{}
                }
            ),
            ok;
        _ ->
            ok
    end.

internal_emqx_bridge_v2_persistent_term_info_key() ->
    ?FUNCTION_NAME.
