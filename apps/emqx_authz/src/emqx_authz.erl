%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz).
-behaviour(emqx_config_handler).

-include("emqx_authz.hrl").
-include_lib("emqx/include/logger.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-export([ register_metrics/0
        , init/0
        , lookup/0
        , lookup/1
        , move/2
        , update/2
        , authorize/5
        ]).

-export([post_config_update/4, pre_config_update/2]).

-define(CONF_KEY_PATH, [authorization, sources]).

-spec(register_metrics() -> ok).
register_metrics() ->
    lists:foreach(fun emqx_metrics:ensure/1, ?AUTHZ_METRICS).

init() ->
    ok = register_metrics(),
    emqx_config_handler:add_handler(?CONF_KEY_PATH, ?MODULE),
    NSources = [init_source(Source) || Source <- emqx:get_config(?CONF_KEY_PATH, [])],
    ok = emqx_hooks:add('client.authorize', {?MODULE, authorize, [NSources]}, -1).

lookup() ->
    {_M, _F, [A]}= find_action_in_hooks(),
    A.
lookup(Id) ->
    try find_source_by_id(Id, lookup()) of
        {_, Source} -> Source
    catch
        error:Reason -> {error, Reason}
    end.

move(Id, Position) ->
    emqx:update_config(?CONF_KEY_PATH, {move, Id, Position}).

update(Cmd, Sources) ->
    emqx:update_config(?CONF_KEY_PATH, {Cmd, Sources}).

pre_config_update({move, Id, <<"top">>}, Conf) when is_list(Conf) ->
    {Index, _} = find_source_by_id(Id),
    {List1, List2} = lists:split(Index, Conf),
    {ok, [lists:nth(Index, Conf)] ++ lists:droplast(List1) ++ List2};

pre_config_update({move, Id, <<"bottom">>}, Conf) when is_list(Conf) ->
    {Index, _} = find_source_by_id(Id),
    {List1, List2} = lists:split(Index, Conf),
    {ok, lists:droplast(List1) ++ List2 ++ [lists:nth(Index, Conf)]};

pre_config_update({move, Id, #{<<"before">> := BeforeId}}, Conf) when is_list(Conf) ->
    {Index1, _} = find_source_by_id(Id),
    Conf1 = lists:nth(Index1, Conf),
    {Index2, _} = find_source_by_id(BeforeId),
    Conf2 = lists:nth(Index2, Conf),

    {List1, List2} = lists:split(Index2, Conf),
    {ok, lists:delete(Conf1, lists:droplast(List1))
        ++ [Conf1] ++ [Conf2]
        ++ lists:delete(Conf1, List2)};

pre_config_update({move, Id, #{<<"after">> := AfterId}}, Conf) when is_list(Conf) ->
    {Index1, _} = find_source_by_id(Id),
    Conf1 = lists:nth(Index1, Conf),
    {Index2, _} = find_source_by_id(AfterId),

    {List1, List2} = lists:split(Index2, Conf),
    {ok, lists:delete(Conf1, List1)
        ++ [Conf1]
        ++ lists:delete(Conf1, List2)};

pre_config_update({head, Sources}, Conf) when is_list(Sources), is_list(Conf) ->
    {ok, Sources ++ Conf};
pre_config_update({tail, Sources}, Conf) when is_list(Sources), is_list(Conf) ->
    {ok, Conf ++ Sources};
pre_config_update({{replace_once, Id}, Source}, Conf) when is_map(Source), is_list(Conf) ->
    {Index, _} = find_source_by_id(Id),
    {List1, List2} = lists:split(Index, Conf),
    {ok, lists:droplast(List1) ++ [Source] ++ List2};
pre_config_update({_, Sources}, _Conf) when is_list(Sources)->
    %% overwrite the entire config!
    {ok, Sources}.

post_config_update(_, undefined, _Conf, _AppEnvs) ->
    ok;
post_config_update({move, Id, <<"top">>}, _NewSources, _OldSources, _AppEnvs) ->
    InitedSources = lookup(),
    {Index, Source} = find_source_by_id(Id, InitedSources),
    {Sources1, Sources2 } = lists:split(Index, InitedSources),
    Sources3 = [Source] ++ lists:droplast(Sources1) ++ Sources2,
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [Sources3]}, -1),
    ok = emqx_authz_cache:drain_cache();
post_config_update({move, Id, <<"bottom">>}, _NewSources, _OldSources, _AppEnvs) ->
    InitedSources = lookup(),
    {Index, Source} = find_source_by_id(Id, InitedSources),
    {Sources1, Sources2 } = lists:split(Index, InitedSources),
    Sources3 = lists:droplast(Sources1) ++ Sources2 ++ [Source],
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [Sources3]}, -1),
    ok = emqx_authz_cache:drain_cache();
post_config_update({move, Id, #{<<"before">> := BeforeId}}, _NewSources, _OldSources, _AppEnvs) ->
    InitedSources = lookup(),
    {_, Source0} = find_source_by_id(Id, InitedSources),
    {Index, Source1} = find_source_by_id(BeforeId, InitedSources),
    {Sources1, Sources2} = lists:split(Index, InitedSources),
    Sources3 = lists:delete(Source0, lists:droplast(Sources1))
             ++ [Source0] ++ [Source1]
             ++ lists:delete(Source0, Sources2),
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [Sources3]}, -1),
    ok = emqx_authz_cache:drain_cache();

post_config_update({move, Id, #{<<"after">> := AfterId}}, _NewSources, _OldSources, _AppEnvs) ->
    InitedSources = lookup(),
    {_, Source} = find_source_by_id(Id, InitedSources),
    {Index, _} = find_source_by_id(AfterId, InitedSources),
    {Sources1, Sources2} = lists:split(Index, InitedSources),
    Sources3 = lists:delete(Source, Sources1)
             ++ [Source]
             ++ lists:delete(Source, Sources2),
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [Sources3]}, -1),
    ok = emqx_authz_cache:drain_cache();

post_config_update({head, Sources}, _NewSources, _OldConf, _AppEnvs) ->
    InitedSources = [init_source(R) || R <- check_sources(Sources)],
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [InitedSources ++ lookup()]}, -1),
    ok = emqx_authz_cache:drain_cache();

post_config_update({tail, Sources}, _NewSources, _OldConf, _AppEnvs) ->
    InitedSources = [init_source(R) || R <- check_sources(Sources)],
    emqx_hooks:put('client.authorize', {?MODULE, authorize, [lookup() ++ InitedSources]}, -1),
    ok = emqx_authz_cache:drain_cache();

post_config_update({{replace_once, Id}, Source}, _NewSources, _OldConf, _AppEnvs) when is_map(Source) ->
    OldInitedSources = lookup(),
    {Index, OldSource} = find_source_by_id(Id, OldInitedSources),
    case maps:get(type, OldSource, undefined) of
       undefined -> ok;
       _ ->
            #{annotations := #{id := Id}} = OldSource,
            ok = emqx_resource:remove(Id)
    end,
    {OldSources1, OldSources2 } = lists:split(Index, OldInitedSources),
    InitedSources = [init_source(R#{annotations => #{id => Id}}) || R <- check_sources([Source])],
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [lists:droplast(OldSources1) ++ InitedSources ++ OldSources2]}, -1),
    ok = emqx_authz_cache:drain_cache();

post_config_update(_, NewSources, _OldConf, _AppEnvs) ->
    %% overwrite the entire config!
    OldInitedSources = lookup(),
    InitedSources = [init_source(Source) || Source <- NewSources],
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [InitedSources]}, -1),
    lists:foreach(fun (#{type := _Type, enable := true, annotations := #{id := Id}}) ->
                         ok = emqx_resource:remove(Id);
                      (_) -> ok
                  end, OldInitedSources),
    ok = emqx_authz_cache:drain_cache().

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

check_sources(RawSources) ->
    {ok, Conf} = hocon:binary(jsx:encode(#{<<"authorization">> => #{<<"sources">> => RawSources}}), #{format => richmap}),
    CheckConf = hocon_schema:check(emqx_authz_schema, Conf, #{atom_key => true}),
    #{authorization:= #{sources := Sources}} = hocon_schema:richmap_to_map(CheckConf),
    Sources.

find_source_by_id(Id) -> find_source_by_id(Id, lookup()).
find_source_by_id(Id, Sources) -> find_source_by_id(Id, Sources, 1).
find_source_by_id(_SourceId, [], _N) -> error(not_found_rule);
find_source_by_id(SourceId, [ Source = #{annotations := #{id := Id}} | Tail], N) ->
    case SourceId =:= Id of
        true -> {N, Source};
        false -> find_source_by_id(SourceId, Tail, N + 1)
    end.

find_action_in_hooks() ->
    Callbacks = emqx_hooks:lookup('client.authorize'),
    [Action] = [Action || {callback,{?MODULE, authorize, _} = Action, _, _} <- Callbacks ],
    Action.

gen_id(Type) ->
    iolist_to_binary([io_lib:format("~s_~s",[?APP, Type]), "_", integer_to_list(erlang:system_time())]).

create_resource(#{type := DB,
                  config := Config,
                  annotations := #{id := ResourceID}}) ->
    case emqx_resource:update(
            ResourceID,
            list_to_existing_atom(io_lib:format("~s_~s",[emqx_connector, DB])),
            Config,
            [])
    of
        {ok, _} -> ResourceID;
        {error, Reason} -> {error, Reason}
    end;
create_resource(#{type := DB,
                  config := Config}) ->
    ResourceID = gen_id(DB),
    case emqx_resource:create(
            ResourceID,
            list_to_existing_atom(io_lib:format("~s_~s",[emqx_connector, DB])),
            Config)
    of
        {ok, already_created} -> ResourceID;
        {ok, _} -> ResourceID;
        {error, Reason} -> {error, Reason}
    end.

init_source(#{enable := true,
                type := file,
                path := Path
               } = Source) ->
    Rules = case file:consult(Path) of
                {ok, Terms} ->
                    [emqx_authz_rule:compile(Term) || Term <- Terms];
                {error, eacces} ->
                    ?LOG(alert, "Insufficient permissions to read the ~s file", [Path]),
                    error(eaccess);
                {error, enoent} ->
                    ?LOG(alert, "The ~s file does not exist", [Path]),
                    error(enoent);
                {error, Reason} ->
                    ?LOG(alert, "Failed to read ~s: ~p", [Path, Reason]),
                    error(Reason)
            end,
    Source#{annotations =>
            #{id => gen_id(file),
              rules => Rules
         }};
init_source(#{enable := true,
                type := http,
                config := #{url := Url} = Config
               } = Source) ->
    NConfig = maps:merge(Config, #{base_url => maps:remove(query, Url)}),
    case create_resource(Source#{config := NConfig}) of
        {error, Reason} -> error({load_config_error, Reason});
        Id -> Source#{annotations =>
                      #{id => Id}
                   }
    end;
init_source(#{enable := true,
                type := DB
               } = Source) when DB =:= redis;
                              DB =:= mongo ->
    case create_resource(Source) of
        {error, Reason} -> error({load_config_error, Reason});
        Id -> Source#{annotations =>
                      #{id => Id}
                   }
    end;
init_source(#{enable := true,
                type := DB,
                sql := SQL
               } = Source) when DB =:= mysql;
                              DB =:= pgsql ->
    Mod = list_to_existing_atom(io_lib:format("~s_~s",[?APP, DB])),
    case create_resource(Source) of
        {error, Reason} -> error({load_config_error, Reason});
        Id -> Source#{annotations =>
                      #{id => Id,
                        sql => Mod:parse_query(SQL)
                       }
                   }
    end;
init_source(#{enable := false} = Source) ->Source.

%%--------------------------------------------------------------------
%% AuthZ callbacks
%%--------------------------------------------------------------------

%% @doc Check AuthZ
-spec(authorize(emqx_types:clientinfo(), emqx_types:all(), emqx_topic:topic(), allow | deny, sources())
      -> {stop, allow} | {ok, deny}).
authorize(#{username := Username,
            peerhost := IpAddress
           } = Client, PubSub, Topic, DefaultResult, Sources) ->
    case do_authorize(Client, PubSub, Topic, Sources) of
        {matched, allow} ->
            ?LOG(info, "Client succeeded authorization: Username: ~p, IP: ~p, Topic: ~p, Permission: allow", [Username, IpAddress, Topic]),
            emqx_metrics:inc(?AUTHZ_METRICS(allow)),
            {stop, allow};
        {matched, deny} ->
            ?LOG(info, "Client failed authorization: Username: ~p, IP: ~p, Topic: ~p, Permission: deny", [Username, IpAddress, Topic]),
            emqx_metrics:inc(?AUTHZ_METRICS(deny)),
            {stop, deny};
        nomatch ->
            ?LOG(info, "Client failed authorization: Username: ~p, IP: ~p, Topic: ~p, Reasion: ~p", [Username, IpAddress, Topic, "no-match rule"]),
            {stop, DefaultResult}
    end.

do_authorize(Client, PubSub, Topic,
               [#{type := file,
                  enable := true,
                  annotations := #{rule := Rules}
                 } | Tail] ) ->
    case emqx_authz_rule:match(Client, PubSub, Topic, Rules) of
        nomatch -> do_authorize(Client, PubSub, Topic, Tail);
        Matched -> Matched
    end;
do_authorize(Client, PubSub, Topic,
               [Connector = #{type := Type,
                              enable := true
                             } | Tail] ) ->
    Mod = list_to_existing_atom(io_lib:format("~s_~s",[emqx_authz, Type])),
    case Mod:authorize(Client, PubSub, Topic, Connector) of
        nomatch -> do_authorize(Client, PubSub, Topic, Tail);
        Matched -> Matched
    end;
do_authorize(_Client, _PubSub, _Topic, []) -> nomatch.
