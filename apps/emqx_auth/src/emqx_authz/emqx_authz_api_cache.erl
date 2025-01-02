%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_authz_api_cache).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include("emqx_authz.hrl").

-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([
    fields/1,
    namespace/0
]).

-export([
    clean_cache/2,
    node_cache/2,
    node_cache_status/2,
    node_cache_reset/2
]).

-import(hoconsc, [ref/2]).
-import(emqx_dashboard_swagger, [error_codes/2]).

-define(BAD_REQUEST, 'BAD_REQUEST').
-define(INTERNAL_ERROR, 'INTERNAL_ERROR').

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/authorization/cache",
        "/authorization/node_cache",
        "/authorization/node_cache/status",
        "/authorization/node_cache/reset"
    ].

%%--------------------------------------------------------------------
%% Schema for each URI
%%--------------------------------------------------------------------

schema("/authorization/cache") ->
    #{
        'operationId' => clean_cache,
        delete =>
            #{
                description => ?DESC(authorization_cache_delete),
                responses =>
                    #{
                        204 => <<"No Content">>,
                        400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad Request">>)
                    }
            }
    };
schema("/authorization/node_cache") ->
    #{
        'operationId' => node_cache,
        get => #{
            description => ?DESC(authorization_node_cache_get),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    ref(?MODULE, response_authz_node_cache),
                    authz_node_cache_example()
                )
            }
        },
        put => #{
            description => ?DESC(authorization_node_cache_put),
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                ref(?MODULE, response_authz_node_cache),
                authz_node_cache_example()
            ),
            responses => #{
                204 => <<"Authentication cache updated">>,
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>)
            }
        }
    };
schema("/authorization/node_cache/status") ->
    #{
        'operationId' => node_cache_status,
        get => #{
            description => ?DESC(authorization_node_cache_status_get),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    ref(emqx_auth_cache_schema, status),
                    authz_node_cache_status_example()
                ),
                500 => error_codes([?INTERNAL_ERROR], <<"Internal Service Error">>)
            }
        }
    };
schema("/authorization/node_cache/reset") ->
    #{
        'operationId' => node_cache_reset,
        post =>
            #{
                description => ?DESC(authorization_node_cache_reset_post),
                responses =>
                    #{
                        204 => <<"No Content">>,
                        500 => error_codes([?INTERNAL_ERROR], <<"Internal Service Error">>)
                    }
            }
    }.

%%--------------------------------------------------------------------
%% Handlers
%%--------------------------------------------------------------------

clean_cache(delete, _) ->
    case emqx_mgmt:clean_authz_cache_all() of
        ok ->
            {204};
        {error, Reason} ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message => bin(Reason)
            }}
    end.

node_cache(get, _) ->
    RawConfig = emqx:get_raw_config([authorization, node_cache]),
    {200, emqx_auth_cache_schema:fill_defaults(RawConfig)};
node_cache(put, #{body := Config}) ->
    case update_config([authorization, node_cache], Config) of
        {ok, _} ->
            {204};
        {error, Reason} ->
            serialize_error(Reason)
    end.

node_cache_status(get, _) ->
    Nodes = mria:running_nodes(),
    LookupResult = emqx_auth_cache_proto_v1:metrics(Nodes, ?AUTHZ_CACHE),
    case is_ok(LookupResult) of
        {ok, ResList} ->
            NodeMetrics = lists:map(
                fun({Node, Metrics}) ->
                    #{node => Node, metrics => Metrics}
                end,
                ResList
            ),
            {_, Metrics} = lists:unzip(ResList),
            AggregatedMetrics = aggregate_metrics(Metrics),
            Response = #{
                node_metrics => NodeMetrics,
                metrics => AggregatedMetrics
            },
            {200, Response};
        {error, ErrL} ->
            {500, #{
                code => <<"INTERNAL_ERROR">>,
                message => bin(ErrL)
            }}
    end.

node_cache_reset(post, _) ->
    Nodes = mria:running_nodes(),
    case is_ok(emqx_auth_cache_proto_v1:reset(Nodes, ?AUTHZ_CACHE)) of
        {ok, _} ->
            {204};
        {error, ErrL} ->
            {500, #{
                code => <<"INTERNAL_ERROR">>,
                message => bin(ErrL)
            }}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

update_config(Path, ConfigRequest) ->
    emqx_conf:update(Path, ConfigRequest, #{
        rawconf_with_defaults => true,
        override_to => cluster
    }).

bin(Term) -> erlang:iolist_to_binary(io_lib:format("~0p", [Term])).

serialize_error(Reason) ->
    {400, #{
        code => <<"BAD_REQUEST">>,
        message => bin(Reason)
    }}.

is_ok(ResL) ->
    case
        lists:filter(
            fun
                ({ok, _}) -> false;
                (_) -> true
            end,
            ResL
        )
    of
        [] -> {ok, [Res || {ok, Res} <- ResL]};
        ErrL -> {error, ErrL}
    end.

aggregate_metrics(Metrics) ->
    emqx_authn_api:aggregate_metrics(Metrics).

%%--------------------------------------------------------------------
%% Schema
%%--------------------------------------------------------------------

namespace() -> undefined.

fields(request_authz_node_cache) ->
    emqx_auth_cache_schema:fields(config);
fields(response_authz_node_cache) ->
    emqx_auth_cache_schema:fields(config).

%%--------------------------------------------------------------------
%% Schema examples
%%--------------------------------------------------------------------

authz_node_cache_example() ->
    emqx_auth_cache_schema:cache_settings_example().

authz_node_cache_status_example() ->
    emqx_auth_cache_schema:metrics_example().
