%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_pulsar_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    is_action/1,
    action_convert_from_connector/2
]).

is_action(_) -> true.

bridge_v1_type_name() -> pulsar_producer.

action_type_name() -> pulsar.

connector_type_name() -> pulsar.

schema_module() -> emqx_bridge_pulsar_pubsub_schema.

action_convert_from_connector(ConnectorConfig, ActionConfig) ->
    Dispatch = emqx_utils_conv:bin(maps:get(<<"strategy">>, ConnectorConfig, <<>>)),
    case Dispatch of
        <<"key_dispatch">> ->
            case emqx_utils_maps:deep_find([<<"parameters">>, <<"message">>], ActionConfig) of
                {ok, Message} ->
                    Validator =
                        #{
                            <<"strategy">> => key_dispatch,
                            <<"message">> => emqx_utils_maps:binary_key_map(Message)
                        },
                    case emqx_bridge_pulsar:producer_strategy_key_validator(Validator) of
                        ok ->
                            ActionConfig;
                        {error, Reason} ->
                            throw(#{
                                reason => Reason,
                                kind => validation_error
                            })
                    end;
                {not_found, _, _} ->
                    %% no message field, use the default message template
                    ActionConfig
            end;
        _ ->
            ActionConfig
    end.
