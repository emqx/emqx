%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ee_connector_mongodb).

-behaviour(emqx_resource).

-include_lib("emqx_connector/include/emqx_connector_tables.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% `emqx_resource' API
-export([
    callback_mode/0,
    is_buffer_supported/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_get_status/2
]).

%%========================================================================================
%% `emqx_resource' API
%%========================================================================================

callback_mode() -> emqx_connector_mongo:callback_mode().

is_buffer_supported() -> false.

on_start(InstanceId, Config) ->
    case emqx_connector_mongo:on_start(InstanceId, Config) of
        {ok, ConnectorState} ->
            PayloadTemplate0 = maps:get(payload_template, Config, undefined),
            PayloadTemplate = preprocess_template(PayloadTemplate0),
            CollectionTemplateSource = maps:get(collection, Config),
            CollectionTemplate = preprocess_template(CollectionTemplateSource),
            State = #{
                payload_template => PayloadTemplate,
                collection_template => CollectionTemplate,
                connector_state => ConnectorState
            },
            {ok, State};
        Error ->
            Error
    end.

on_stop(InstanceId, _State = #{connector_state := ConnectorState}) ->
    emqx_connector_mongo:on_stop(InstanceId, ConnectorState).

on_query(InstanceId, {send_message, Message0}, State) ->
    #{
        payload_template := PayloadTemplate,
        collection_template := CollectionTemplate,
        connector_state := ConnectorState
    } = State,
    NewConnectorState = ConnectorState#{
        collection => emqx_plugin_libs_rule:proc_tmpl(CollectionTemplate, Message0)
    },
    Message = render_message(PayloadTemplate, Message0),
    Res = emqx_connector_mongo:on_query(InstanceId, {send_message, Message}, NewConnectorState),
    ?tp(mongo_ee_connector_on_query_return, #{result => Res}),
    Res;
on_query(InstanceId, Request, _State = #{connector_state := ConnectorState}) ->
    emqx_connector_mongo:on_query(InstanceId, Request, ConnectorState).

on_get_status(InstanceId, _State = #{connector_state := ConnectorState}) ->
    emqx_connector_mongo:on_get_status(InstanceId, ConnectorState).

%%========================================================================================
%% Helper fns
%%========================================================================================

preprocess_template(undefined = _PayloadTemplate) ->
    undefined;
preprocess_template(PayloadTemplate) ->
    emqx_plugin_libs_rule:preproc_tmpl(PayloadTemplate).

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
            emqx_utils_json:decode(emqx_plugin_libs_rule:proc_tmpl(PayloadTks, Msg), [return_maps]);
        _ ->
            % If tuples were found, replace the tuple values with the references created, run
            % the modified message through the json parser, and then at the end replace the
            % references with the actual tuple values.
            ProcessedMessage = replace_message_values_with_references(Msg, PreparedTupleMap),
            DecodedMap = emqx_utils_json:decode(
                emqx_plugin_libs_rule:proc_tmpl(PayloadTks, ProcessedMessage), [return_maps]
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
            (_key, _value, TupleMap) ->
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
