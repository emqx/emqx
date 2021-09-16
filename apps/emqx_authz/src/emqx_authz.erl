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
        , move/3
        , update/2
        , update/3
        , authorize/5
        ]).

-export([post_config_update/4, pre_config_update/2]).

-define(CONF_KEY_PATH, [authorization, sources]).
-define(SOURCE_TYPES, [file, http, mongodb, mysql, postgresql, redis, 'built-in-database']).

-spec(register_metrics() -> ok).
register_metrics() ->
    lists:foreach(fun emqx_metrics:ensure/1, ?AUTHZ_METRICS).

init() ->
    ok = register_metrics(),
    emqx_config_handler:add_handler(?CONF_KEY_PATH, ?MODULE),
    Sources = emqx:get_config(?CONF_KEY_PATH, []),
    ok = check_dup_types(Sources),
    NSources = [init_source(Source) || Source <- Sources],
    ok = emqx_hooks:add('client.authorize', {?MODULE, authorize, [NSources]}, -1).

lookup() ->
    {_M, _F, [A]}= find_action_in_hooks(),
    A.
lookup(Type) ->
    try find_source_by_type(atom(Type), lookup()) of
        {_, Source} -> Source
    catch
        error:Reason -> {error, Reason}
    end.

move(Type, Cmd) ->
    move(Type, Cmd, #{}).

move(Type, #{<<"before">> := Before}, Opts) ->
    emqx:update_config(?CONF_KEY_PATH, {move, atom(Type), #{<<"before">> => atom(Before)}}, Opts);
move(Type, #{<<"after">> := After}, Opts) ->
    emqx:update_config(?CONF_KEY_PATH, {move, atom(Type), #{<<"after">> => atom(After)}}, Opts);
move(Type, Position, Opts) ->
    emqx:update_config(?CONF_KEY_PATH, {move, atom(Type), Position}, Opts).

update(Cmd, Sources) ->
    update(Cmd, Sources, #{}).

update({replace_once, Type}, Sources, Opts) ->
    emqx:update_config(?CONF_KEY_PATH, {{replace_once, atom(Type)}, Sources}, Opts);
update({delete_once, Type}, Sources, Opts) ->
    emqx:update_config(?CONF_KEY_PATH, {{delete_once, atom(Type)}, Sources}, Opts);
update(Cmd, Sources, Opts) ->
    emqx:update_config(?CONF_KEY_PATH, {Cmd, Sources}, Opts).

pre_config_update({move, Type, <<"top">>}, Conf) when is_list(Conf) ->
    {Index, _} = find_source_by_type(Type),
    {List1, List2} = lists:split(Index, Conf),
    NConf = [lists:nth(Index, Conf)] ++ lists:droplast(List1) ++ List2,
    case check_dup_types(NConf) of
        ok -> {ok, NConf};
        Error -> Error
    end;

pre_config_update({move, Type, <<"bottom">>}, Conf) when is_list(Conf) ->
    {Index, _} = find_source_by_type(Type),
    {List1, List2} = lists:split(Index, Conf),
    NConf = lists:droplast(List1) ++ List2 ++ [lists:nth(Index, Conf)],
    case check_dup_types(NConf) of
        ok -> {ok, NConf};
        Error -> Error
    end;

pre_config_update({move, Type, #{<<"before">> := Before}}, Conf) when is_list(Conf) ->
    {Index1, _} = find_source_by_type(Type),
    Conf1 = lists:nth(Index1, Conf),
    {Index2, _} = find_source_by_type(Before),
    Conf2 = lists:nth(Index2, Conf),

    {List1, List2} = lists:split(Index2, Conf),
    NConf = lists:delete(Conf1, lists:droplast(List1))
         ++ [Conf1] ++ [Conf2]
         ++ lists:delete(Conf1, List2),
    case check_dup_types(NConf) of
        ok -> {ok, NConf};
        Error -> Error
    end;

pre_config_update({move, Type, #{<<"after">> := After}}, Conf) when is_list(Conf) ->
    {Index1, _} = find_source_by_type(Type),
    Conf1 = lists:nth(Index1, Conf),
    {Index2, _} = find_source_by_type(After),

    {List1, List2} = lists:split(Index2, Conf),
    NConf = lists:delete(Conf1, List1)
         ++ [Conf1]
         ++ lists:delete(Conf1, List2),
    case check_dup_types(NConf) of
        ok -> {ok, NConf};
        Error -> Error
    end;

pre_config_update({head, Sources}, Conf) when is_list(Sources), is_list(Conf) ->
    NConf = Sources ++ Conf,
    case check_dup_types(NConf) of
        ok -> {ok, Sources ++ Conf};
        Error -> Error
    end;
pre_config_update({tail, Sources}, Conf) when is_list(Sources), is_list(Conf) ->
    NConf = Conf ++ Sources,
    case check_dup_types(NConf) of
        ok -> {ok, Conf ++ Sources};
        Error -> Error
    end;
pre_config_update({{replace_once, Type}, Source}, Conf) when is_map(Source), is_list(Conf) ->
    {Index, _} = find_source_by_type(Type),
    {List1, List2} = lists:split(Index, Conf),
    NConf = lists:droplast(List1) ++ [Source] ++ List2,
    case check_dup_types(NConf) of
        ok -> {ok, NConf};
        Error -> Error
    end;
pre_config_update({{delete_once, Type}, _Source}, Conf) when is_list(Conf) ->
    {Index, _} = find_source_by_type(Type),
    {List1, List2} = lists:split(Index, Conf),
    NConf = lists:droplast(List1)  ++ List2,
    case check_dup_types(NConf) of
        ok -> {ok, NConf};
        Error -> Error
    end;
pre_config_update({_, Sources}, _Conf) when is_list(Sources)->
    %% overwrite the entire config!
    {ok, Sources}.

post_config_update(_, undefined, _Conf, _AppEnvs) ->
    ok;
post_config_update({move, Type, <<"top">>}, _NewSources, _OldSources, _AppEnvs) ->
    InitedSources = lookup(),
    {Index, Source} = find_source_by_type(Type, InitedSources),
    {Sources1, Sources2 } = lists:split(Index, InitedSources),
    Sources3 = [Source] ++ lists:droplast(Sources1) ++ Sources2,
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [Sources3]}, -1),
    ok = emqx_authz_cache:drain_cache();
post_config_update({move, Type, <<"bottom">>}, _NewSources, _OldSources, _AppEnvs) ->
    InitedSources = lookup(),
    {Index, Source} = find_source_by_type(Type, InitedSources),
    {Sources1, Sources2 } = lists:split(Index, InitedSources),
    Sources3 = lists:droplast(Sources1) ++ Sources2 ++ [Source],
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [Sources3]}, -1),
    ok = emqx_authz_cache:drain_cache();
post_config_update({move, Type, #{<<"before">> := Before}}, _NewSources, _OldSources, _AppEnvs) ->
    InitedSources = lookup(),
    {_, Source0} = find_source_by_type(Type, InitedSources),
    {Index, Source1} = find_source_by_type(Before, InitedSources),
    {Sources1, Sources2} = lists:split(Index, InitedSources),
    Sources3 = lists:delete(Source0, lists:droplast(Sources1))
             ++ [Source0] ++ [Source1]
             ++ lists:delete(Source0, Sources2),
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [Sources3]}, -1),
    ok = emqx_authz_cache:drain_cache();

post_config_update({move, Type, #{<<"after">> := After}}, _NewSources, _OldSources, _AppEnvs) ->
    InitedSources = lookup(),
    {_, Source} = find_source_by_type(Type, InitedSources),
    {Index, _} = find_source_by_type(After, InitedSources),
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

post_config_update({{replace_once, Type}, #{type := Type} = Source}, _NewSources, _OldConf, _AppEnvs) when is_map(Source) ->
    OldInitedSources = lookup(),
    {Index, OldSource} = find_source_by_type(Type, OldInitedSources),
    case maps:get(type, OldSource, undefined) of
       undefined -> ok;
       file -> ok;
       _ ->
            #{annotations := #{id := Id}} = OldSource,
            ok = emqx_resource:remove(Id)
    end,
    {OldSources1, OldSources2 } = lists:split(Index, OldInitedSources),
    InitedSources = [init_source(R) || R <- check_sources([Source])],
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [lists:droplast(OldSources1) ++ InitedSources ++ OldSources2]}, -1),
    ok = emqx_authz_cache:drain_cache();
post_config_update({{delete_once, Type}, _Source}, _NewSources, _OldConf, _AppEnvs) ->
    OldInitedSources = lookup(),
    {_, OldSource} = find_source_by_type(Type, OldInitedSources),
    case OldSource of
        #{annotations := #{id := Id}} ->
            ok = emqx_resource:remove(Id);
        _ -> ok
    end,
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [lists:delete(OldSource, OldInitedSources)]}, -1),
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
%% Initialize source
%%--------------------------------------------------------------------

check_dup_types(Sources) ->
    check_dup_types(Sources, ?SOURCE_TYPES).
check_dup_types(_Sources, []) -> ok;
check_dup_types(Sources, [T0 | Tail]) ->
    case lists:foldl(fun (#{type := T1}, AccIn) ->
                             case T0 =:= T1 of
                                 true -> AccIn + 1;
                                 false -> AccIn
                             end;
                         (#{<<"type">> := T1}, AccIn) ->
                             case T0 =:= atom(T1) of
                                 true -> AccIn + 1;
                                 false -> AccIn
                             end
                     end, 0, Sources) > 1 of
        true ->
           ?LOG(error, "The type is duplicated in the Authorization source"),
           {error, 'The type is duplicated in the Authorization source'};
        false -> check_dup_types(Sources, Tail)
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
    Source#{annotations => #{rules => Rules}};
init_source(#{enable := true,
              type := http,
              url := Url
             } = Source) ->
    NSource= maps:put(base_url, maps:remove(query, Url), Source),
    case create_resource(NSource) of
        {error, Reason} -> error({load_config_error, Reason});
        Id -> Source#{annotations => #{id => Id}}
    end;
init_source(#{enable := true,
              type := 'built-in-database'
             } = Source) -> Source;
init_source(#{enable := true,
              type := DB
             } = Source) when DB =:= redis;
                              DB =:= mongodb ->
    case create_resource(Source) of
        {error, Reason} -> error({load_config_error, Reason});
        Id -> Source#{annotations => #{id => Id}}
    end;
init_source(#{enable := true,
              type := DB,
              query := SQL
             } = Source) when DB =:= mysql;
                              DB =:= postgresql ->
    Mod = authz_module(DB),
    case create_resource(Source) of
        {error, Reason} -> error({load_config_error, Reason});
        Id -> Source#{annotations =>
                      #{id => Id,
                        query => Mod:parse_query(SQL)
                       }
                   }
    end;
init_source(#{enable := false} = Source) ->Source.

%%--------------------------------------------------------------------
%% AuthZ callbacks
%%--------------------------------------------------------------------

%% @doc Check AuthZ
-spec(authorize(emqx_types:clientinfo(), emqx_types:all(), emqx_types:topic(), allow | deny, sources())
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

do_authorize(_Client, _PubSub, _Topic, []) ->
    nomatch;
do_authorize(Client, PubSub, Topic, [#{enable := false} | Rest]) ->
    do_authorize(Client, PubSub, Topic, Rest);
do_authorize(Client, PubSub, Topic, [#{type := file} = F | Tail]) ->
    #{annotations := #{rules := Rules}} = F,
    case emqx_authz_rule:matches(Client, PubSub, Topic, Rules) of
        nomatch -> do_authorize(Client, PubSub, Topic, Tail);
        Matched -> Matched
    end;
do_authorize(Client, PubSub, Topic,
               [Connector = #{type := Type} | Tail] ) ->
    Mod = authz_module(Type),
    case Mod:authorize(Client, PubSub, Topic, Connector) of
        nomatch -> do_authorize(Client, PubSub, Topic, Tail);
        Matched -> Matched
    end.

%%--------------------------------------------------------------------
%% Internal function
%%--------------------------------------------------------------------

check_sources(RawSources) ->
    Schema = #{roots => emqx_authz_schema:fields("authorization"), fields => #{}},
    Conf = #{<<"sources">> => RawSources},
    #{sources := Sources} = hocon_schema:check_plain(Schema, Conf, #{atom_key => true}),
    Sources.

find_source_by_type(Type) -> find_source_by_type(Type, lookup()).
find_source_by_type(Type, Sources) -> find_source_by_type(Type, Sources, 1).
find_source_by_type(_, [], _N) -> error(not_found_source);
find_source_by_type(Type, [ Source = #{type := T} | Tail], N) ->
    case Type =:= T of
        true -> {N, Source};
        false -> find_source_by_type(Type, Tail, N + 1)
    end.

find_action_in_hooks() ->
    Callbacks = emqx_hooks:lookup('client.authorize'),
    [Action] = [Action || {callback,{?MODULE, authorize, _} = Action, _, _} <- Callbacks ],
    Action.

gen_id(Type) ->
    iolist_to_binary([io_lib:format("~s_~s",[?APP, Type])]).

create_resource(#{type := DB,
                  annotations := #{id := ResourceID}} = Source) ->
    case emqx_resource:recreate(ResourceID, connector_module(DB), Source, []) of
        {ok, _} -> ResourceID;
        {error, Reason} -> {error, Reason}
    end;
create_resource(#{type := DB} = Source) ->
    ResourceID = gen_id(DB),
    case emqx_resource:create(ResourceID, connector_module(DB), Source) of
        {ok, already_created} -> ResourceID;
        {ok, _} -> ResourceID;
        {error, Reason} -> {error, Reason}
    end.

authz_module(Type) ->
    list_to_existing_atom("emqx_authz_" ++ atom_to_list(Type)).

connector_module(mongodb) ->
    emqx_connector_mongo;
connector_module(postgresql) ->
    emqx_connector_pgsql;
connector_module(Type) ->
    list_to_existing_atom("emqx_connector_" ++ atom_to_list(Type)).

atom(B) when is_binary(B) ->
    try binary_to_existing_atom(B, utf8)
    catch
        _ -> binary_to_atom(B)
    end;
atom(A) when is_atom(A) -> A.
