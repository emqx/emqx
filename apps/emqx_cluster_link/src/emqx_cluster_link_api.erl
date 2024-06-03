%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/http_api.hrl").

-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([config/2]).

-define(CONF_PATH, [cluster, links]).
-define(TAGS, [<<"Cluster">>]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/cluster/links"
    ].

schema("/cluster/links") ->
    #{
        'operationId' => config,
        get =>
            #{
                description => "Get cluster links configuration",
                tags => ?TAGS,
                responses =>
                    #{200 => links_config_schema()}
            },
        put =>
            #{
                description => "Update cluster links configuration",
                tags => ?TAGS,
                'requestBody' => links_config_schema(),
                responses =>
                    #{
                        200 => links_config_schema(),
                        400 =>
                            emqx_dashboard_swagger:error_codes(
                                [?BAD_REQUEST], <<"Update Config Failed">>
                            )
                    }
            }
    }.

%%--------------------------------------------------------------------
%% API Handler funcs
%%--------------------------------------------------------------------

config(get, _Params) ->
    {200, get_raw()};
config(put, #{body := Body}) ->
    case emqx_cluster_link_config:update(Body) of
        {ok, NewConfig} ->
            {200, NewConfig};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("Update config failed ~p", [Reason])),
            {400, ?BAD_REQUEST, Message}
    end.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

get_raw() ->
    #{<<"links">> := Conf} =
        emqx_config:fill_defaults(
            #{<<"links">> => emqx_conf:get_raw(?CONF_PATH)},
            #{obfuscate_sensitive_values => true}
        ),
    Conf.

links_config_schema() ->
    emqx_cluster_link_schema:links_schema(
        #{
            examples => #{<<"example">> => links_config_example()}
        }
    ).

links_config_example() ->
    [
        #{
            <<"enable">> => true,
            <<"pool_size">> => 10,
            <<"server">> => <<"emqxcl_b.host:1883">>,
            <<"ssl">> => #{<<"enable">> => false},
            <<"topics">> =>
                [
                    <<"t/topic-example">>,
                    <<"t/topic-filter-example/1/#">>
                ],
            <<"upstream">> => <<"emqxcl_b">>
        },
        #{
            <<"enable">> => true,
            <<"pool_size">> => 10,
            <<"server">> => <<"emqxcl_c.host:1883">>,
            <<"ssl">> => #{<<"enable">> => false},
            <<"topics">> =>
                [
                    <<"t/topic-example">>,
                    <<"t/topic-filter-example/1/#">>
                ],
            <<"upstream">> => <<"emqxcl_c">>
        }
    ].
