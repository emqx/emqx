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

-define(EXAMPLE_REDIS,
        #{type=> redis,
          enable => true,
          server => <<"127.0.0.1:3306">>,
          redis_type => single,
          pool_size => 1,
          auto_reconnect => true,
          cmd => <<"HGETALL mqtt_authz">>}).
-define(EXAMPLE_FILE,
        #{type=> file,
          enable => true,
          rules => <<"{allow,{username,\"^dashboard?\"},subscribe,[\"$SYS/#\"]}.\n",
                     "{allow,{ipaddr,\"127.0.0.1\"},all,[\"$SYS/#\",\"#\"]}.">>
                   }).

-define(EXAMPLE_RETURNED,
        #{sources => [ ?EXAMPLE_REDIS
                     , ?EXAMPLE_FILE
                     ]
        }).

-define(IS_TRUE(Val), ((Val =:= true) or (Val =:= <<"true">>))).

-define(API_SCHEMA_MODULE, emqx_authz_api_schema).

-export([ get_raw_sources/0
        , get_raw_source/1
        , lookup_from_local_node/1
        , lookup_from_all_nodes/1
        ]).

-export([ api_spec/0
        , paths/0
        , schema/1
        ]).

-export([ sources/2
        , source/2
        , move_source/2
        ]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [ "/authorization/sources"
    , "/authorization/sources/:type"
    , "/authorization/sources/:type/move"].

%%--------------------------------------------------------------------
%% Schema for each URI
%%--------------------------------------------------------------------

schema("/authorization/sources") ->
    #{ 'operationId' => sources
     , get =>
           #{ description => <<"List all authorization sources">>
            , responses =>
                  #{ 200 => mk( array(hoconsc:union(
                      [ref(?API_SCHEMA_MODULE, Type) || Type <- authz_sources_types(detailed)]))
                              , #{desc => <<"Authorization source">>})
                   }
            }
     , post =>
           #{ description => <<"Add a new source">>
            , 'requestBody' => mk( hoconsc:union(
                                   [ref(?API_SCHEMA_MODULE, Type)
                                        || Type <- authz_sources_types(detailed)])
                                 , #{desc => <<"Source config">>})
            , responses =>
                  #{ 204 => <<"Authorization source created successfully">>
                   , 400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST],
                                                               <<"Bad Request">>)
                   }
            }
     , put =>
           #{ description => <<"Update all sources">>
            , 'requestBody' => mk( array(hoconsc:union(
                                  [ref(?API_SCHEMA_MODULE, Type)
                                       || Type <- authz_sources_types(detailed)]))
                                 , #{desc => <<"Sources">>})
            , responses =>
                  #{ 204 => <<"Authorization source updated successfully">>
                   , 400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST],
                              <<"Bad Request">>)
                   }
            }
     };
schema("/authorization/sources/:type") ->
    #{ 'operationId' => source
     , get =>
           #{ description => <<"Get a authorization source">>
            , parameters => parameters_field()
            , responses =>
                  #{ 200 => mk( hoconsc:union(
                               [ref(?API_SCHEMA_MODULE, Type)
                                   || Type <- authz_sources_types(detailed)])
                              , #{desc => <<"Authorization source">>})
                   , 404 => emqx_dashboard_swagger:error_codes([?NOT_FOUND], <<"Not Found">>)
                   }
            }
     , put =>
           #{ description => <<"Update source">>
            , parameters => parameters_field()
            , 'requestBody' => mk( hoconsc:union([ref(?API_SCHEMA_MODULE, Type)
                                   || Type <- authz_sources_types(detailed)]))
            , responses =>
                  #{ 204 => <<"Authorization source updated successfully">>
                   , 400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad Request">>)
                   , 404 => emqx_dashboard_swagger:error_codes([?NOT_FOUND], <<"Not Found">>)
                   }
            }
     , delete =>
           #{ description => <<"Delete source">>
            , parameters => parameters_field()
            , responses =>
                  #{ 204 => <<"Deleted successfully">>
                   , 400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad Request">>)
                   }
            }
     };
schema("/authorization/sources/:type/move") ->
    #{ 'operationId' => move_source
     , post =>
           #{ description => <<"Change the order of sources">>
            , parameters => parameters_field()
            , 'requestBody' =>
                  emqx_dashboard_swagger:schema_with_examples(
                    ref(?API_SCHEMA_MODULE, position),
                    position_example())
            , responses =>
                  #{ 204 => <<"No Content">>
                   , 400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad Request">>)
                   , 404 => emqx_dashboard_swagger:error_codes([?NOT_FOUND], <<"Not Found">>)
                   }
            }
     }.


%%--------------------------------------------------------------------
%% Operation functions
%%--------------------------------------------------------------------

sources(Method, #{bindings := #{type := Type} = Bindings } = Req)
  when is_atom(Type) ->
    sources(Method, Req#{bindings => Bindings#{type => atom_to_binary(Type, utf8)}});
sources(get, _) ->
    Sources = lists:foldl(fun (#{<<"type">> := <<"file">>,
                                 <<"enable">> := Enable, <<"path">> := Path}, AccIn) ->
                                  case file:read_file(Path) of
                                      {ok, Rules} ->
                                          lists:append(AccIn, [#{type => file,
                                                                 enable => Enable,
                                                                 rules => Rules
                                                                }]);
                                      {error, _} ->
                                          lists:append(AccIn, [#{type => file,
                                                                 enable => Enable,
                                                                 rules => <<"">>
                                                                }])
                                  end;
                              (Source, AccIn) ->
                                  lists:append(AccIn, [read_certs(Source)])
                          end, [], get_raw_sources()),
    {200, #{sources => Sources}};
sources(post, #{body := #{<<"type">> := <<"file">>, <<"rules">> := Rules}}) ->
    {ok, Filename} = write_file(acl_conf_file(), Rules),
    update_config(?CMD_PREPEND, [#{<<"type">> => <<"file">>,
                                   <<"enable">> => true, <<"path">> => Filename}]);
sources(post, #{body := Body}) when is_map(Body) ->
    update_config(?CMD_PREPEND, [maybe_write_certs(Body)]);
sources(put, #{body := Body}) when is_list(Body) ->
    NBody = [ begin
                case Source of
                    #{<<"type">> := <<"file">>, <<"rules">> := Rules, <<"enable">> := Enable} ->
                        {ok, Filename} = write_file(acl_conf_file(), Rules),
                        #{<<"type">> => <<"file">>, <<"enable">> => Enable, <<"path">> => Filename};
                    _ -> maybe_write_certs(Source)
                end
              end || Source <- Body],
    update_config(?CMD_REPLACE, NBody).

source(Method, #{bindings := #{type := Type} = Bindings } = Req)
  when is_atom(Type) ->
    source(Method, Req#{bindings => Bindings#{type => atom_to_binary(Type, utf8)}});
source(get, #{bindings := #{type := Type}}) ->
    case get_raw_source(Type) of
        [] -> {404, #{message => <<"Not found ", Type/binary>>}};
        [#{<<"type">> := <<"file">>, <<"enable">> := Enable, <<"path">> := Path}] ->
            case file:read_file(Path) of
                {ok, Rules} ->
                    {200, #{type => file,
                            enable => Enable,
                            rules => Rules
                           }
                    };
                {error, Reason} ->
                    {400, #{code => <<"BAD_REQUEST">>,
                            message => bin(Reason)}}
            end;
        [Source] ->
            case emqx_authz:lookup(Type) of
                #{annotations := #{id := ResourceId }} ->
                    StatusAndMetrics = lookup_from_all_nodes(ResourceId),
                    {200, maps:put(status_and_metrics, StatusAndMetrics, read_certs(Source))};
                _ -> {200, maps:put(status_and_metrics, resource_not_found, read_certs(Source))}
            end
    end;
source(put, #{bindings := #{type := <<"file">>}, body := #{<<"type">> := <<"file">>,
                                                           <<"rules">> := Rules,
                                                           <<"enable">> := Enable}}) ->
    {ok, Filename} = write_file(maps:get(path, emqx_authz:lookup(file), ""), Rules),
    case emqx_authz:update({?CMD_REPLACE, <<"file">>}, #{<<"type">> => <<"file">>,
                                                         <<"enable">> => Enable,
                                                         <<"path">> => Filename}) of
        {ok, _} -> {204};
        {error, {emqx_conf_schema, _}} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => <<"BAD_SCHEMA">>}};
        {error, Reason} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => bin(Reason)}}
    end;
source(put, #{bindings := #{type := Type}, body := Body}) when is_map(Body) ->
    update_config({?CMD_REPLACE, Type}, maybe_write_certs(Body#{<<"type">> => Type}));
source(delete, #{bindings := #{type := Type}}) ->
    update_config({?CMD_DELETE, Type}, #{}).

move_source(Method, #{bindings := #{type := Type} = Bindings } = Req)
  when is_atom(Type) ->
    move_source(Method, Req#{bindings => Bindings#{type => atom_to_binary(Type, utf8)}});
move_source(post, #{bindings := #{type := Type}, body := #{<<"position">> := Position}}) ->
    case emqx_authz:move(Type, Position) of
        {ok, _} -> {204};
        {error, not_found_source} ->
            {404, #{code => <<"NOT_FOUND">>,
                    message => <<"source ", Type/binary, " not found">>}};
        {error, {emqx_conf_schema, _}} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => <<"BAD_SCHEMA">>}};
        {error, Reason} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => bin(Reason)}}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

lookup_from_local_node(ResourceId) ->
    NodeId = node(self()),
    case emqx_resource:get_instance(ResourceId) of
        {error, not_found} -> {error, {NodeId, not_found_resource}};
        {ok, _, #{ status := Status, metrics := Metrics }} ->
            {ok, {NodeId, Status, Metrics}}
    end.

lookup_from_all_nodes(ResourceId) ->
    Nodes = mria_mnesia:running_nodes(),
    case is_ok(emqx_authz_proto_v1:lookup_from_all_nodes(Nodes, ResourceId)) of
        {ok, ResList} ->
            {StatusMap, MetricsMap, ErrorMap} = make_result_map(ResList),
            AggregateStatus = aggregate_status(maps:values(StatusMap)),
            AggregateMetrics = aggregate_metrics(maps:values(MetricsMap)),
            Fun = fun(_, V1) -> restructure_map(V1) end,
            #{node_status => StatusMap,
              node_metrics => maps:map(Fun, MetricsMap),
              node_error => ErrorMap,
              status => AggregateStatus,
              metrics => restructure_map(AggregateMetrics)
             };
        {error, ErrL} ->
            {error_msg('INTERNAL_ERROR', ErrL)}
    end.

aggregate_status([]) -> error_some_strange_happen;
aggregate_status(AllStatus) ->
    Head = fun ([A | _]) -> A end,
    HeadVal = Head(AllStatus),
    AllRes = lists:all(fun (Val) -> Val == HeadVal end, AllStatus),
    case AllRes of
        true -> HeadVal;
        false -> inconsistent
    end.

aggregate_metrics([]) -> error_some_strange_happen;
aggregate_metrics([HeadMetrics | AllMetrics]) ->
    CombinerFun =
        fun ComFun(Val1, Val2) ->
            case erlang:is_map(Val1) of
                true -> emqx_map_lib:merge_with(ComFun, Val1, Val2);
                false -> Val1 + Val2
            end
        end,
    Fun = fun (ElemMap, AccMap) ->
        emqx_map_lib:merge_with(CombinerFun, ElemMap, AccMap) end,
    lists:foldl(Fun, HeadMetrics, AllMetrics).

make_result_map(ResList) ->
    Fun =
        fun(Elem, {StatusMap, MetricsMap, ErrorMap}) ->
            case Elem of
                {ok, {NodeId, Status, Metrics}} ->
                    {maps:put(NodeId, Status, StatusMap),
                     maps:put(NodeId, Metrics, MetricsMap),
                     ErrorMap
                    };
                {error, {NodeId, Reason}} ->
                    {StatusMap,
                     MetricsMap,
                     maps:put(NodeId, Reason, ErrorMap)
                    }
            end
        end,
    lists:foldl(Fun, {maps:new(), maps:new(), maps:new()}, ResList).

restructure_map(#{counters := #{failed := Failed, matched := Match, success := Succ},
                  rate := #{matched := #{current := Rate, last5m := Rate5m, max := RateMax}
                           }
                 }
               ) ->
    #{matched => Match,
      success => Succ,
      failed => Failed,
      rate => Rate,
      rate_last5m => Rate5m,
      rate_max => RateMax
     };
restructure_map(Error) ->
     Error.

error_msg(Code, Msg) ->
              #{code => Code, message => bin_t(io_lib:format("~p", [Msg]))}.

bin_t(S) when is_list(S) ->
    list_to_binary(S).

is_ok(ResL) ->
    case lists:filter(fun({ok, _}) -> false; (_) -> true end, ResL) of
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
    lists:map(fun(Source) ->
        case maps:find(<<"headers">>, Source) of
            {ok, Headers} ->
                NewHeaders =
                    case Source of
                        #{<<"method">> := <<"get">>} ->
                            (emqx_authz_schema:headers_no_content_type(converter))(Headers);
                        #{<<"method">> := <<"post">>} ->
                            (emqx_authz_schema:headers(converter))(Headers);
                        _ -> Headers
                    end,
                Source#{<<"headers">> => NewHeaders};
            error -> Source
        end
              end, Sources).

get_raw_source(Type) ->
    lists:filter(fun (#{<<"type">> := T}) ->
                         T =:= Type
                 end, get_raw_sources()).

update_config(Cmd, Sources) ->
    case emqx_authz:update(Cmd, Sources) of
        {ok, _} -> {204};
        {error, {pre_config_update, emqx_authz, Reason}} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => bin(Reason)}};
        {error, {post_config_update, emqx_authz, Reason}} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => bin(Reason)}};
        %% TODO: The `Reason` may cann't be trans to json term. (i.e. ecpool start failed)
        {error, {emqx_conf_schema, _}} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => <<"BAD_SCHEMA">>}};
        {error, Reason} ->
            {400, #{code => <<"BAD_REQUEST">>,
                    message => bin(Reason)}}
    end.

read_certs(#{<<"ssl">> := SSL} = Source) ->
    case emqx_tls_lib:file_content_as_options(SSL) of
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => failed_to_readd_ssl_file}),
            throw(failed_to_readd_ssl_file);
        {ok, NewSSL} ->
            Source#{<<"ssl">> => NewSSL}
    end;
read_certs(Source) -> Source.

maybe_write_certs(#{<<"ssl">> := #{<<"enable">> := True} = SSL} = Source) when ?IS_TRUE(True) ->
    Type = maps:get(<<"type">>, Source),
    {ok, Return} = emqx_tls_lib:ensure_ssl_files(filename:join(["authz", Type]), SSL),
    maps:put(<<"ssl">>, Return, Source);
maybe_write_certs(Source) -> Source.

write_file(Filename, Bytes0) ->
    ok = filelib:ensure_dir(Filename),
    case file:read_file(Filename) of
        {ok, Bytes1} ->
            case crypto:hash(md5, Bytes1) =:= crypto:hash(md5, Bytes0) of
                true -> {ok, iolist_to_binary(Filename)};
                false -> do_write_file(Filename, Bytes0)
            end;
        _ -> do_write_file(Filename, Bytes0)
    end.

do_write_file(Filename, Bytes) ->
    case file:write_file(Filename, Bytes) of
       ok -> {ok, iolist_to_binary(Filename)};
       {error, Reason} ->
           ?SLOG(error, #{filename => Filename, msg => "write_file_error", reason => Reason}),
           error(Reason)
    end.

bin(Term) -> erlang:iolist_to_binary(io_lib:format("~p", [Term])).

acl_conf_file() ->
    emqx_authz:acl_conf_file().

parameters_field() ->
    [ {type, mk( enum(?API_SCHEMA_MODULE:authz_sources_types(simple))
               , #{in => path, desc => <<"Authorization type">>})
      }
    ].

position_example() ->
    #{<<"position">> => #{<<"before">> => <<"file">>}}.

authz_sources_types(Type) ->
    emqx_authz_api_schema:authz_sources_types(Type).
