%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_eviction_agent_api).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

%% Swagger specs from hocon schema
-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0
]).

-export([
    fields/1,
    roots/0
]).

-define(TAGS, [<<"Node Eviction">>]).

%% API callbacks
-export([
    '/node_eviction/status'/2
]).

-import(hoconsc, [mk/2, ref/1, ref/2]).

namespace() -> "node_eviction".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/node_eviction/status"
    ].

schema("/node_eviction/status") ->
    #{
        'operationId' => '/node_eviction/status',
        get => #{
            tags => ?TAGS,
            summary => <<"Get node eviction status">>,
            description => ?DESC("node_eviction_status_get"),
            responses => #{
                200 => schema_status()
            }
        }
    }.

'/node_eviction/status'(_Bindings, _Params) ->
    case emqx_eviction_agent:status() of
        disabled ->
            {200, #{status => disabled}};
        {enabled, Stats} ->
            {200, #{
                status => enabled,
                stats => Stats
            }}
    end.

schema_status() ->
    mk(hoconsc:union([ref(status_enabled), ref(status_disabled)]), #{}).

roots() -> [].

fields(status_enabled) ->
    [
        {status, mk(enabled, #{default => enabled})},
        {stats, ref(stats)}
    ];
fields(stats) ->
    [
        {connections, mk(integer(), #{})},
        {sessions, mk(integer(), #{})}
    ];
fields(status_disabled) ->
    [
        {status, mk(disabled, #{default => disabled})}
    ].
