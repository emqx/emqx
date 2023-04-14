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
    Rendered = emqx_plugin_libs_rule:proc_tmpl(PayloadTemplate, Message),
    emqx_utils_json:decode(Rendered, [return_maps]).
