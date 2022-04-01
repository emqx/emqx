%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_api_sources).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include("emqx_authz.hrl").
-include_lib("emqx/include/logger.hrl").

-import(hoconsc, [mk/1, mk/2, ref/2, array/1, enum/1]).

-define(BAD_REQUEST, 'BAD_REQUEST').
-define(NOT_FOUND, 'NOT_FOUND').

-define(API_SCHEMA_MODULE, emqx_authz_api_schema).

-export([
    get_raw_sources/0,
    get_raw_source/1,
    source_status/2,
    lookup_from_local_node/1,
    lookup_from_all_nodes/1
]).

-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([
    sources/2,
    source/2,
    move_source/2
]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/authorization/sources",
        "/authorization/sources/:type",
        "/authorization/sources/:type/status",
        "/authorization/sources/:type/move"
    ].

%%--------------------------------------------------------------------
%% Schema for each URI
%%--------------------------------------------------------------------

schema("/authorization/sources") ->
    #{
        'operationId' => sources,
        get =>
            #{
                description => <<"List all authorization sources">>,
                responses =>
                    #{
                        200 => mk(
                            array(hoconsc:union(authz_sources_type_refs())),
                            #{desc => <<"Authorization source">>}
                        )
                    }
            },
        post =>
            #{
                description => <<"Add a new source">>,
                'requestBody' => mk(
                    hoconsc:union(authz_sources_type_refs()),
                    #{desc => <<"Source config">>}
                ),
                responses =>
                    #{
                        204 => <<"Authorization source created successfully">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST],
                            <<"Bad Request">>
                        )
                    }
            }
    };
schema("/authorization/sources/:type") ->
    #{
        'operationId' => source,
        get =>
            #{
                description => <<"Get a authorization source">>,
                parameters => parameters_field(),
                responses =>
                    #{
                        200 => mk(
                            hoconsc:union(authz_sources_type_refs()),
                            #{desc => <<"Authorization source">>}
                        ),
                        404 => emqx_dashboard_swagger:error_codes([?NOT_FOUND], <<"Not Found">>)
                    }
            },
        put =>
            #{
                description => <<"Update source">>,
                parameters => parameters_field(),
                'requestBody' => mk(hoconsc:union(authz_sources_type_refs())),
                responses =>
                    #{
                        204 => <<"Authorization source updated successfully">>,
                        400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad Request">>)
                    }
            },
        delete =>
            #{
                description => <<"Delete source">>,
                parameters => parameters_field(),
                responses =>
                    #{
                        204 => <<"Deleted successfully">>,
                        400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad Request">>)
                    }
            }
    };
schema("/authorization/sources/:type/status") ->
    #{
        'operationId' => source_status,
        get =>
            #{
                description => <<"Get a authorization source">>,
                parameters => parameters_field(),
                responses =>
                    #{
                        200 => emqx_dashboard_swagger:schema_with_examples(
                            hoconsc:ref(emqx_authn_schema, "metrics_status_fields"),
                            status_metrics_example()
                        ),
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad request">>
                        ),
                        404 => emqx_dashboard_swagger:error_codes([?NOT_FOUND], <<"Not Found">>)
                    }
            }
    };
schema("/authorization/sources/:type/move") ->
    #{
        'operationId' => move_source,
        post =>
            #{
                description => <<"Change the order of sources">>,
                parameters => parameters_field(),
                'requestBody' =>
                    emqx_dashboard_swagger:schema_with_examples(
                        ref(?API_SCHEMA_MODULE, position),
                        position_example()
                    ),
                responses =>
                    #{
                        204 => <<"No Content">>,
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad Request">>
                        ),
                        404 => emqx_dashboard_swagger:error_codes([?NOT_FOUND], <<"Not Found">>)
                    }
            }
    }.

%%--------------------------------------------------------------------
%% Operation functions
%%--------------------------------------------------------------------

sources(Method, #{bindings := #{type := Type} = Bindings} = Req) when
    is_atom(Type)
->
    sources(Method, Req#{bindings => Bindings#{type => atom_to_binary(Type, utf8)}});
sources(get, _) ->
    Sources = lists:foldl(
        fun
            (
                #{
                    <<"type">> := <<"file">>,
                    <<"enable">> := Enable,
                    <<"path">> := Path
                },
                AccIn
            ) ->
                case file:read_file(Path) of
                    {ok, Rules} ->
                        lists:append(AccIn, [
                            #{
                                type => file,
                                enable => Enable,
                                rules => Rules
                            }
                        ]);
                    {error, _} ->
                        lists:append(AccIn, [
                            #{
                                type => file,
                                enable => Enable,
                                rules => <<"">>
                            }
                        ])
                end;
            (Source, AccIn) ->
                lists:append(AccIn, [read_certs(Source)])
        end,
        [],
        get_raw_sources()
    ),
    {200, #{sources => Sources}};
sources(post, #{body := #{<<"type">> := <<"file">>} = Body}) ->
    create_authz_file(Body);
sources(post, #{body := Body}) ->
    update_config(?CMD_PREPEND, Body).

source(Method, #{bindings := #{type := Type} = Bindings} = Req) when
    is_atom(Type)
->
    source(Method, Req#{bindings => Bindings#{type => atom_to_binary(Type, utf8)}});
source(get, #{bindings := #{type := Type}}) ->
    case get_raw_source(Type) of
        [] ->
            {404, #{message => <<"Not found ", Type/binary>>}};
        [#{<<"type">> := <<"file">>, <<"enable">> := Enable, <<"path">> := Path}] ->
            case file:read_file(Path) of
                {ok, Rules} ->
                    {200, #{
                        type => file,
                        enable => Enable,
                        rules => Rules
                    }};
                {error, Reason} ->
                    {500, #{
                        code => <<"INTERNAL_ERROR">>,
                        message => bin(Reason)
                    }}
            end;
        [Source] ->
            {200, read_certs(Source)}
    end;
source(put, #{bindings := #{type := <<"file">>}, body := #{<<"type">> := <<"file">>} = Body}) ->
    update_authz_file(Body);
source(put, #{bindings := #{type := Type}, body := Body}) ->
    update_config({?CMD_REPLACE, Type}, Body);
source(delete, #{bindings := #{type := Type}}) ->
    update_config({?CMD_DELETE, Type}, #{}).

source_status(get, #{bindings := #{type := Type}}) ->
    BinType = atom_to_binary(Type, utf8),
    case get_raw_source(BinType) of
        [] ->
            {404, #{
                code => <<"NOT_FOUND">>,
                message => <<"Not found", BinType/binary>>
            }};
        [#{<<"type">> := <<"file">>}] ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message => <<"Not Support Status">>
            }};
        [_] ->
            case emqx_authz:lookup(Type) of
                #{annotations := #{id := ResourceId}} -> lookup_from_all_nodes(ResourceId);
                _ -> {400, #{code => <<"BAD_REQUEST">>, message => <<"Resource Disable">>}}
            end
    end.

move_source(Method, #{bindings := #{type := Type} = Bindings} = Req) when
    is_atom(Type)
->
    move_source(Method, Req#{bindings => Bindings#{type => atom_to_binary(Type, utf8)}});
move_source(post, #{bindings := #{type := Type}, body := #{<<"position">> := Position}}) ->
    case parse_position(Position) of
        {ok, NPosition} ->
            try emqx_authz:move(Type, NPosition) of
                {ok, _} ->
                    {204};
                {error, {not_found_source, _Type}} ->
                    {404, #{
                        code => <<"NOT_FOUND">>,
                        message => <<"source ", Type/binary, " not found">>
                    }};
                {error, {emqx_conf_schema, _}} ->
                    {400, #{
                        code => <<"BAD_REQUEST">>,
                        message => <<"BAD_SCHEMA">>
                    }};
                {error, Reason} ->
                    {400, #{
                        code => <<"BAD_REQUEST">>,
                        message => bin(Reason)
                    }}
            catch
                error:{unknown_authz_source_type, Unknown} ->
                    NUnknown = bin(Unknown),
                    {400, #{
                        code => <<"BAD_REQUEST">>,
                        message => <<"Unknown authz Source Type: ", NUnknown/binary>>
                    }}
            end;
        {error, Reason} ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message => bin(Reason)
            }}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

lookup_from_local_node(ResourceId) ->
    NodeId = node(self()),
    case emqx_resource:get_instance(ResourceId) of
        {error, not_found} -> {error, {NodeId, not_found_resource}};
        {ok, _, #{status := Status, metrics := Metrics}} -> {ok, {NodeId, Status, Metrics}}
    end.

lookup_from_all_nodes(ResourceId) ->
    Nodes = mria_mnesia:running_nodes(),
    case is_ok(emqx_authz_proto_v1:lookup_from_all_nodes(Nodes, ResourceId)) of
        {ok, ResList} ->
            {StatusMap, MetricsMap, _} = make_result_map(ResList),
            AggregateStatus = aggregate_status(maps:values(StatusMap)),
            AggregateMetrics = aggregate_metrics(maps:values(MetricsMap)),
            Fun = fun(_, V1) -> restructure_map(V1) end,
            MKMap = fun(Name) -> fun({Key, Val}) -> #{node => Key, Name => Val} end end,
            HelpFun = fun(M, Name) -> lists:map(MKMap(Name), maps:to_list(M)) end,
            case AggregateStatus of
                empty_metrics_and_status ->
                    {400, #{
                        code => <<"BAD_REQUEST">>,
                        message => <<"Resource Not Support Status">>
                    }};
                _ ->
                    {200, #{
                        node_status => HelpFun(StatusMap, status),
                        node_metrics => HelpFun(maps:map(Fun, MetricsMap), metrics),
                        status => AggregateStatus,
                        metrics => restructure_map(AggregateMetrics)
                    }}
            end;
        {error, ErrL} ->
            {500, #{
                code => <<"INTERNAL_ERROR">>,
                message => bin_t(io_lib:format("~p", [ErrL]))
            }}
    end.

aggregate_status([]) ->
    empty_metrics_and_status;
aggregate_status(AllStatus) ->
    Head = fun([A | _]) -> A end,
    HeadVal = Head(AllStatus),
    AllRes = lists:all(fun(Val) -> Val == HeadVal end, AllStatus),
    case AllRes of
        true -> HeadVal;
        false -> inconsistent
    end.

aggregate_metrics([]) ->
    empty_metrics_and_status;
aggregate_metrics([HeadMetrics | AllMetrics]) ->
    CombinerFun =
        fun ComFun(Val1, Val2) ->
            case erlang:is_map(Val1) of
                true -> emqx_map_lib:merge_with(ComFun, Val1, Val2);
                false -> Val1 + Val2
            end
        end,
    Fun = fun(ElemMap, AccMap) ->
        emqx_map_lib:merge_with(CombinerFun, ElemMap, AccMap)
    end,
    lists:foldl(Fun, HeadMetrics, AllMetrics).

make_result_map(ResList) ->
    Fun =
        fun(Elem, {StatusMap, MetricsMap, ErrorMap}) ->
            case Elem of
                {ok, {NodeId, Status, Metrics}} ->
                    {
                        maps:put(NodeId, Status, StatusMap),
                        maps:put(NodeId, Metrics, MetricsMap),
                        ErrorMap
                    };
                {error, {NodeId, Reason}} ->
                    {StatusMap, MetricsMap, maps:put(NodeId, Reason, ErrorMap)}
            end
        end,
    lists:foldl(Fun, {maps:new(), maps:new(), maps:new()}, ResList).

restructure_map(#{
    counters := #{failed := Failed, matched := Match, success := Succ},
    rate := #{matched := #{current := Rate, last5m := Rate5m, max := RateMax}}
}) ->
    #{
        matched => Match,
        success => Succ,
        failed => Failed,
        rate => Rate,
        rate_last5m => Rate5m,
        rate_max => RateMax
    };
restructure_map(Error) ->
    Error.

bin_t(S) when is_list(S) ->
    list_to_binary(S).

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

get_raw_sources() ->
    RawSources = emqx:get_raw_config([authorization, sources], []),
    Schema = #{roots => emqx_authz_schema:fields("authorization"), fields => #{}},
    Conf = #{<<"sources">> => RawSources},
    Options = #{only_fill_defaults => true},
    #{<<"sources">> := Sources} = hocon_tconf:check_plain(Schema, Conf, Options),
    merge_default_headers(Sources).

merge_default_headers(Sources) ->
    lists:map(
        fun(Source) ->
            case maps:find(<<"headers">>, Source) of
                {ok, Headers} ->
                    NewHeaders =
                        case Source of
                            #{<<"method">> := <<"get">>} ->
                                (emqx_authz_schema:headers_no_content_type(converter))(Headers);
                            #{<<"method">> := <<"post">>} ->
                                (emqx_authz_schema:headers(converter))(Headers);
                            _ ->
                                Headers
                        end,
                    Source#{<<"headers">> => NewHeaders};
                error ->
                    Source
            end
        end,
        Sources
    ).

get_raw_source(Type) ->
    lists:filter(
        fun(#{<<"type">> := T}) ->
            T =:= Type
        end,
        get_raw_sources()
    ).

update_config(Cmd, Sources) ->
    case emqx_authz:update(Cmd, Sources) of
        {ok, _} ->
            {204};
        {error, {pre_config_update, emqx_authz, Reason}} ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message => bin(Reason)
            }};
        {error, {post_config_update, emqx_authz, Reason}} ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message => bin(Reason)
            }};
        %% TODO: The `Reason` may cann't be trans to json term. (i.e. ecpool start failed)
        {error, {emqx_conf_schema, _}} ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message => <<"BAD_SCHEMA">>
            }};
        {error, Reason} ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message => bin(Reason)
            }}
    end.

read_certs(#{<<"ssl">> := SSL} = Source) ->
    case emqx_tls_lib:file_content_as_options(SSL) of
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => failed_to_read_ssl_file}),
            throw(failed_to_read_ssl_file);
        {ok, NewSSL} ->
            Source#{<<"ssl">> => NewSSL}
    end;
read_certs(Source) ->
    Source.

parameters_field() ->
    [
        {type,
            mk(
                enum(?API_SCHEMA_MODULE:authz_sources_types(simple)),
                #{in => path, desc => <<"Authorization type">>}
            )}
    ].

parse_position(<<"front">>) ->
    {ok, ?CMD_MOVE_FRONT};
parse_position(<<"rear">>) ->
    {ok, ?CMD_MOVE_REAR};
parse_position(<<"before:">>) ->
    {error, <<"Invalid parameter. Cannot be placed before an empty target">>};
parse_position(<<"after:">>) ->
    {error, <<"Invalid parameter. Cannot be placed after an empty target">>};
parse_position(<<"before:", Before/binary>>) ->
    {ok, ?CMD_MOVE_BEFORE(Before)};
parse_position(<<"after:", After/binary>>) ->
    {ok, ?CMD_MOVE_AFTER(After)};
parse_position(_) ->
    {error, <<"Invalid parameter. Unknow position">>}.

position_example() ->
    #{
        front =>
            #{
                summary => <<"front example">>,
                value => #{<<"position">> => <<"front">>}
            },
        rear =>
            #{
                summary => <<"rear example">>,
                value => #{<<"position">> => <<"rear">>}
            },
        relative_before =>
            #{
                summary => <<"relative example">>,
                value => #{<<"position">> => <<"before:file">>}
            },
        relative_after =>
            #{
                summary => <<"relative example">>,
                value => #{<<"position">> => <<"after:file">>}
            }
    }.

authz_sources_type_refs() ->
    [
        ref(?API_SCHEMA_MODULE, Type)
     || Type <- emqx_authz_api_schema:authz_sources_types(detailed)
    ].

bin(Term) -> erlang:iolist_to_binary(io_lib:format("~p", [Term])).

status_metrics_example() ->
    #{
        metrics => #{
            matched => 0,
            success => 0,
            failed => 0,
            rate => 0.0,
            rate_last5m => 0.0,
            rate_max => 0.0
        },
        node_metrics => [
            #{
                node => node(),
                metrics => #{
                    matched => 0,
                    success => 0,
                    failed => 0,
                    rate => 0.0,
                    rate_last5m => 0.0,
                    rate_max => 0.0
                }
            }
        ],
        status => connected,
        node_status => [
            #{
                node => node(),
                status => connected
            }
        ]
    }.

create_authz_file(Body) ->
    do_update_authz_file(?CMD_PREPEND, Body).

update_authz_file(Body) ->
    do_update_authz_file({?CMD_REPLACE, <<"file">>}, Body).

do_update_authz_file(Cmd, Body) ->
    %% API update will placed in `authz` subdirectory inside EMQX's `data_dir`
    update_config(Cmd, Body).
