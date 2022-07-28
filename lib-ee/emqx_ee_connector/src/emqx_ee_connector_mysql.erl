%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_connector_mysql).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

-import(hoconsc, [mk/2, enum/1]).

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    on_start/2,
    on_stop/2,
    on_query/4,
    on_get_status/2
]).

-export([
    roots/0,
    fields/1
]).

-define(SEND_MSG_KEY, send_message).

%% -------------------------------------------------------------------------------------------------
%% resource callback

on_start(InstId, #{egress := #{sql := SQL}} = Config) ->
    {PrepareSQL, ParamsTokens} = emqx_plugin_libs_rule:preproc_sql(SQL, '?'),
    {ok, State} = emqx_connector_mysql:on_start(InstId, Config#{
        prepare_statement => #{?SEND_MSG_KEY => PrepareSQL}
    }),
    {ok, State#{'ParamsTokens' => ParamsTokens}}.

on_stop(InstId, State) ->
    emqx_connector_mysql:on_stop(InstId, State).

on_query(
    InstId,
    {?SEND_MSG_KEY, Msg},
    AfterQuery,
    #{'ParamsTokens' := ParamsTokens} = State
) ->
    Data = emqx_plugin_libs_rule:proc_sql(ParamsTokens, Msg),
    emqx_connector_mysql:on_query(
        InstId, {prepared_query, ?SEND_MSG_KEY, Data}, AfterQuery, State
    ).

on_get_status(InstId, State) ->
    emqx_connector_mysql:on_get_status(InstId, State).

%% -------------------------------------------------------------------------------------------------
%% schema

roots() ->
    fields(config).

fields(config) ->
    emqx_connector_mysql:fields(config) -- emqx_connector_schema_lib:prepare_statement_fields().

%% -------------------------------------------------------------------------------------------------
%% internal functions
