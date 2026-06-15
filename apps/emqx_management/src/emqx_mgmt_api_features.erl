%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_features).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx_utils/include/emqx_http_api.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").

%% `minirest' and `minirest_trails' API
-export([
    namespace/0,
    api_spec/0,
    fields/1,
    paths/0,
    schema/1,
    scopes/0
]).

%% `minirest' handlers
-export([
    '/features'/2
]).

%%-------------------------------------------------------------------------------------------------
%% Type definitions
%%-------------------------------------------------------------------------------------------------

-define(TAGS, [<<"Features">>]).

%%-------------------------------------------------------------------------------------------------
%% `minirest' and `minirest_trails' API
%%-------------------------------------------------------------------------------------------------

namespace() -> undefined.

scopes() -> ?SCOPE_SYSTEM.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{
        check_schema => true, translate_body => {true, atom_keys}
    }).

paths() ->
    [
        "/features"
    ].

schema("/features") ->
    #{
        'operationId' => '/features',
        get => #{
            tags => ?TAGS,
            description => ?DESC("features_list"),
            responses =>
                #{
                    200 =>
                        emqx_dashboard_swagger:schema_with_examples(
                            ref(features_out),
                            example_features_list()
                        )
                }
        }
    }.

fields(features_out) ->
    [
        {preset, mk(binary(), #{})},
        {enabled, mk(array(binary()), #{})},
        {disabled, mk(array(binary()), #{})},
        {bundled, mk(array(binary()), #{})}
    ].

%%-------------------------------------------------------------------------------------------------
%% `minirest' handlers
%%-------------------------------------------------------------------------------------------------

'/features'(get, _Req) ->
    handle_list().

%%-------------------------------------------------------------------------------------------------
%% Examples
%%-------------------------------------------------------------------------------------------------

example_features_list() ->
    #{
        <<"list">> =>
            #{
                summary => <<"List">>,
                value => #{
                    preset => <<"custom">>,
                    enabled => [
                        <<"data_integration">>,
                        <<"dashboard">>,
                        <<"auth">>
                    ],
                    disabled => [
                        <<"message_transformation">>,
                        <<"schema_validation">>,
                        <<"schema_registry">>,
                        <<"gateways">>,
                        <<"cluster_link">>,
                        <<"multi_tenancy">>,
                        <<"ai">>,
                        <<"metrics">>,
                        <<"mqtt_extensions">>
                    ],
                    bundled => []
                }
            }
    }.

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

mk(Type, Props) -> hoconsc:mk(Type, Props).
ref(Struct) -> hoconsc:ref(?MODULE, Struct).
array(Type) -> hoconsc:array(Type).

handle_list() ->
    %% currently, this output matches the api shape.
    Info = emqx_machine_features:info(),
    ?OK(Info).
