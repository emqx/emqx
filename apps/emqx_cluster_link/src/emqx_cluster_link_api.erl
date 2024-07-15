%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/http_api.hrl").
-include_lib("emqx_utils/include/emqx_utils_api.hrl").

-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([
    '/cluster/links'/2,
    '/cluster/links/:name'/2
]).

-define(CONF_PATH, [cluster, links]).
-define(TAGS, [<<"Cluster">>]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/cluster/links",
        "/cluster/links/:name"
    ].

schema("/cluster/links") ->
    #{
        'operationId' => '/cluster/links',
        get =>
            #{
                description => "Get cluster links configuration",
                tags => ?TAGS,
                responses =>
                    #{200 => links_config_schema()}
            },
        post =>
            #{
                description => "Create a cluster link configuration",
                tags => ?TAGS,
                'requestBody' => link_config_schema(),
                responses =>
                    #{
                        200 => link_config_schema(),
                        400 =>
                            emqx_dashboard_swagger:error_codes(
                                [?BAD_REQUEST, ?ALREADY_EXISTS],
                                <<"Update Config Failed">>
                            )
                    }
            }
    };
schema("/cluster/links/:name") ->
    #{
        'operationId' => '/cluster/links/:name',
        get =>
            #{
                description => "Get a cluster link configuration",
                tags => ?TAGS,
                parameters => [param_path_name()],
                responses =>
                    #{
                        200 => link_config_schema(),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], <<"Cluster link not found">>
                        )
                    }
            },
        delete =>
            #{
                description => "Delete a cluster link configuration",
                tags => ?TAGS,
                parameters => [param_path_name()],
                responses =>
                    #{
                        204 => <<"Link deleted">>,
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], <<"Cluster link not found">>
                        )
                    }
            },
        put =>
            #{
                description => "Update a cluster link configuration",
                tags => ?TAGS,
                parameters => [param_path_name()],
                'requestBody' => update_link_config_schema(),
                responses =>
                    #{
                        200 => link_config_schema(),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND], <<"Cluster link not found">>
                        ),
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

'/cluster/links'(get, _Params) ->
    ?OK(get_raw());
'/cluster/links'(post, #{body := Body = #{<<"name">> := Name}}) ->
    with_link(
        Name,
        return(?BAD_REQUEST('ALREADY_EXISTS', <<"Cluster link already exists">>)),
        fun() ->
            case emqx_cluster_link_config:create(Body) of
                {ok, Res} ->
                    ?CREATED(Res);
                {error, Reason} ->
                    Message = list_to_binary(io_lib:format("Create link failed ~p", [Reason])),
                    ?BAD_REQUEST(Message)
            end
        end
    ).

'/cluster/links/:name'(get, #{bindings := #{name := Name}}) ->
    with_link(Name, fun(Link) -> ?OK(Link) end, not_found());
'/cluster/links/:name'(put, #{bindings := #{name := Name}, body := Params0}) ->
    with_link(
        Name,
        fun(Link) ->
            Params = Params0#{<<"name">> => Name},
            case emqx_cluster_link_config:update_one_link(Params) of
                {ok, Res} ->
                    ?OK(Res);
                {error, Reason} ->
                    Message = list_to_binary(io_lib:format("Update link failed ~p", [Reason])),
                    ?BAD_REQUEST(Message)
            end
        end,
        not_found()
    );
'/cluster/links/:name'(delete, #{bindings := #{name := Name}}) ->
    with_link(
        Name,
        fun() ->
            case emqx_cluster_link_config:delete(Name) of
                ok ->
                    ?NO_CONTENT;
                {error, Reason} ->
                    Message = list_to_binary(io_lib:format("Delete link failed ~p", [Reason])),
                    ?BAD_REQUEST(Message)
            end
        end,
        not_found()
    ).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

get_raw() ->
    #{<<"cluster">> := #{<<"links">> := Links}} =
        emqx_config:fill_defaults(
            #{<<"cluster">> => #{<<"links">> => emqx_conf:get_raw(?CONF_PATH)}},
            #{obfuscate_sensitive_values => true}
        ),
    Links.

links_config_schema() ->
    emqx_cluster_link_schema:links_schema(
        #{
            examples => #{<<"example">> => links_config_example()}
        }
    ).

link_config_schema() ->
    emqx_cluster_link_schema:link_schema().

param_path_name() ->
    {name,
        hoconsc:mk(
            binary(),
            #{
                in => path,
                required => true,
                example => <<"my_link">>,
                desc => ?DESC("param_path_name")
            }
        )}.

update_link_config_schema() ->
    proplists:delete(name, emqx_cluster_link_schema:fields("link")).

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
            <<"name">> => <<"emqxcl_b">>
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
            <<"name">> => <<"emqxcl_c">>
        }
    ].

with_link(Name, FoundFn, NotFoundFn) ->
    case emqx_cluster_link_config:link_raw(Name) of
        undefined ->
            NotFoundFn();
        Link = #{} ->
            {arity, Arity} = erlang:fun_info(FoundFn, arity),
            case Arity of
                1 -> FoundFn(Link);
                0 -> FoundFn()
            end
    end.

return(Response) ->
    fun() -> Response end.

not_found() ->
    return(?NOT_FOUND(<<"Cluster link not found">>)).
