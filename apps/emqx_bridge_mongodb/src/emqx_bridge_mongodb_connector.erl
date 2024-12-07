%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_mongodb_connector).

-behaviour(emqx_resource).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% `emqx_resource' API
-export([
    on_remove_channel/3,
    resource_type/0,
    callback_mode/0,
    on_add_channel/4,
    on_get_channel_status/3,
    on_get_channels/1,
    on_get_status/2,
    on_query/3,
    on_start/2,
    on_stop/2,
    on_format_query_result/1
]).

%%========================================================================================
%% `emqx_resource' API
%%========================================================================================
resource_type() -> emqx_mongodb:resource_type().

callback_mode() -> emqx_mongodb:callback_mode().

on_add_channel(
    _InstanceId,
    #{channels := Channels} = OldState,
    ChannelId,
    #{parameters := Parameters} = ChannelConfig0
) ->
    PayloadTemplate0 = maps:get(payload_template, Parameters, undefined),
    PayloadTemplate = preprocess_template(PayloadTemplate0),
    CollectionTemplateSource = maps:get(collection, Parameters),
    CollectionTemplate = preprocess_template(CollectionTemplateSource),
    ChannelConfig = maps:merge(
        Parameters,
        ChannelConfig0#{
            payload_template => PayloadTemplate,
            collection_template => CollectionTemplate
        }
    ),
    NewState = OldState#{channels => maps:put(ChannelId, ChannelConfig, Channels)},
    {ok, NewState}.

on_get_channel_status(InstanceId, _ChannelId, State) ->
    case on_get_status(InstanceId, State) of
        connected ->
            connected;
        _ ->
            connecting
    end.

on_get_channels(InstanceId) ->
    emqx_bridge_v2:get_channels_for_connector(InstanceId).

on_get_status(InstanceId, #{connector_state := DriverState0}) ->
    emqx_mongodb:on_get_status(InstanceId, DriverState0).

on_query(InstanceId, {Channel, Message0}, #{channels := Channels, connector_state := ConnectorState}) ->
    #{
        payload_template := PayloadTemplate,
        collection_template := CollectionTemplate
    } = ChannelState0 = maps:get(Channel, Channels),
    Collection = emqx_placeholder:proc_tmpl(CollectionTemplate, Message0),
    ChannelState = ChannelState0#{
        collection => Collection
    },
    Message = render_message(PayloadTemplate, Message0),
    emqx_trace:rendered_action_template(Channel, #{
        collection => Collection,
        data => Message
    }),
    Res = emqx_mongodb:on_query(
        InstanceId,
        {Channel, Message},
        maps:merge(ConnectorState, ChannelState)
    ),
    ?tp(mongo_bridge_connector_on_query_return, #{instance_id => InstanceId, result => Res}),
    Res;
on_query(InstanceId, Request, _State = #{connector_state := ConnectorState}) ->
    emqx_mongodb:on_query(InstanceId, Request, ConnectorState).

on_format_query_result({{Result, Info}, Documents}) ->
    #{result => Result, info => Info, documents => Documents};
on_format_query_result(Result) ->
    Result.

on_remove_channel(_InstanceId, #{channels := Channels} = State, ChannelId) ->
    NewState = State#{channels => maps:remove(ChannelId, Channels)},
    {ok, NewState}.

on_start(InstanceId, Config0) ->
    Config = config_transform(Config0),
    case emqx_mongodb:on_start(InstanceId, Config) of
        {ok, ConnectorState} ->
            State = #{
                connector_state => ConnectorState,
                channels => #{}
            },
            {ok, State};
        Error ->
            Error
    end.

config_transform(#{parameters := #{mongo_type := MongoType} = Parameters} = Config) ->
    maps:put(
        type,
        connector_type(MongoType),
        maps:merge(
            maps:remove(parameters, Config),
            Parameters
        )
    ).

connector_type(rs) -> mongodb_rs;
connector_type(sharded) -> mongodb_sharded;
connector_type(single) -> mongodb_single.

on_stop(InstanceId, _State = #{connector_state := ConnectorState}) ->
    ok = emqx_mongodb:on_stop(InstanceId, ConnectorState),
    ?tp(mongodb_stopped, #{instance_id => InstanceId}),
    ok.

%%========================================================================================
%% Helper fns
%%========================================================================================

preprocess_template(undefined = _PayloadTemplate) ->
    undefined;
preprocess_template(PayloadTemplate) ->
    emqx_placeholder:preproc_tmpl(PayloadTemplate).

render_message(undefined = _PayloadTemplate, Message) ->
    Message;
render_message(PayloadTemplate, Message) ->
    %% Note: mongo expects a map as a document, so the rendered result
    %% must be JSON-serializable
    format_data(PayloadTemplate, Message).

%% The following function was originally copied over from
%% https://github.com/emqx/emqx-enterprise/commit/50e3628129720f13f544053600ca1502731e29e0.
%% The rule engine has support for producing fields that are date tuples
%% (produced by the SQL language's built-in functions mongo_date/0,
%% mongo_date/1 and mongo_date/2) which the MongoDB driver recognizes and
%% converts to the MongoDB ISODate type
%% (https://www.compose.com/articles/understanding-dates-in-compose-mongodb/).
%% For this to work we have to replace the tuple values with references, make
%% an instance of the template, convert the instance to map with the help of
%% emqx_utils_json:decode and then finally replace the references with the
%% corresponding tuples in the resulting map.
format_data(PayloadTks, Msg) ->
    % Check the Message for any tuples that need to be extracted before running the template though a json parser
    PreparedTupleMap = create_mapping_of_references_to_tuple_values(Msg),
    case maps:size(PreparedTupleMap) of
        % If no tuples were found simply proceed with the json decoding and be done with it
        0 ->
            emqx_utils_json:decode(emqx_placeholder:proc_tmpl(PayloadTks, Msg), [return_maps]);
        _ ->
            % If tuples were found, replace the tuple values with the references created, run
            % the modified message through the json parser, and then at the end replace the
            % references with the actual tuple values.
            ProcessedMessage = replace_message_values_with_references(Msg, PreparedTupleMap),
            DecodedMap = emqx_utils_json:decode(
                emqx_placeholder:proc_tmpl(PayloadTks, ProcessedMessage), [return_maps]
            ),
            populate_map_with_tuple_values(PreparedTupleMap, DecodedMap)
    end.

replace_message_values_with_references(RawMessage, TupleMap) ->
    % Iterate over every created reference/value pair and inject the reference into the message
    maps:fold(
        fun(Reference, OriginalValue, OriginalMessage) ->
            % Iterate over the Message, which is a map, and look for the element which
            % matches the Value in the map which holds the references/original values and replace
            % with the reference
            maps:fold(
                fun(Key, Value, NewMap) ->
                    case Value == OriginalValue of
                        true ->
                            %% Wrap the reference in a string to make it JSON-serializable
                            StringRef = io_lib:format("\"~s\"", [Reference]),
                            WrappedRef = erlang:iolist_to_binary(StringRef),
                            maps:put(Key, WrappedRef, NewMap);
                        false ->
                            maps:put(Key, Value, NewMap)
                    end
                end,
                #{},
                OriginalMessage
            )
        end,
        RawMessage,
        TupleMap
    ).

create_mapping_of_references_to_tuple_values(Message) ->
    maps:fold(
        fun
            (_Key, Value, TupleMap) when is_tuple(Value) ->
                Ref0 = emqx_guid:to_hexstr(emqx_guid:gen()),
                Ref = <<"MONGO_DATE_REF_", Ref0/binary>>,
                maps:put(Ref, Value, TupleMap);
            (_Key, _Value, TupleMap) ->
                TupleMap
        end,
        #{},
        Message
    ).

populate_map_with_tuple_values(TupleMap, MapToMap) ->
    MappingFun =
        fun
            (_Key, Value) when is_map(Value) ->
                populate_map_with_tuple_values(TupleMap, Value);
            (_Key, Value) ->
                case maps:is_key(Value, TupleMap) of
                    true ->
                        maps:get(Value, TupleMap);
                    false ->
                        Value
                end
        end,
    maps:map(MappingFun, MapToMap).
