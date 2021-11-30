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

-export([post_config_update/5, pre_config_update/3]).

-export([acl_conf_file/0]).

-export([ph_to_re/1]).

-spec(register_metrics() -> ok).
register_metrics() ->
    lists:foreach(fun emqx_metrics:ensure/1, ?AUTHZ_METRICS).

init() ->
    ok = register_metrics(),
    emqx_conf:add_handler(?CONF_KEY_PATH, ?MODULE),
    Sources = emqx_conf:get(?CONF_KEY_PATH, []),
    ok = check_dup_types(Sources),
    NSources = init_sources(Sources),
    ok = emqx_hooks:add('client.authorize', {?MODULE, authorize, [NSources]}, -1).

lookup() ->
    {_M, _F, [A]}= find_action_in_hooks(),
    A.

lookup(Type) ->
    {Source, _Front, _Rear} = take(Type),
    Source.

move(Type, Cmd) ->
    move(Type, Cmd, #{}).

move(Type, #{<<"before">> := Before}, Opts) ->
    emqx:update_config( ?CONF_KEY_PATH
                      , {?CMD_MOVE, type(Type), ?CMD_MOVE_BEFORE(type(Before))}, Opts);
move(Type, #{<<"after">> := After}, Opts) ->
    emqx:update_config( ?CONF_KEY_PATH
                      , {?CMD_MOVE, type(Type), ?CMD_MOVE_AFTER(type(After))}, Opts);
move(Type, Position, Opts) ->
    emqx:update_config( ?CONF_KEY_PATH
                      , {?CMD_MOVE, type(Type), Position}, Opts).

update(Cmd, Sources) ->
    update(Cmd, Sources, #{}).

update({?CMD_REPLACE, Type}, Sources, Opts) ->
    emqx:update_config(?CONF_KEY_PATH, {{?CMD_REPLACE, type(Type)}, Sources}, Opts);
update({?CMD_DELETE, Type}, Sources, Opts) ->
    emqx:update_config(?CONF_KEY_PATH, {{?CMD_DELETE, type(Type)}, Sources}, Opts);
update(Cmd, Sources, Opts) ->
    emqx:update_config(?CONF_KEY_PATH, {Cmd, Sources}, Opts).

do_update({?CMD_MOVE, Type, ?CMD_MOVE_TOP}, Conf) when is_list(Conf) ->
    {Source, Front, Rear} = take(Type, Conf),
    [Source | Front] ++ Rear;
do_update({?CMD_MOVE, Type, ?CMD_MOVE_BOTTOM}, Conf) when is_list(Conf) ->
    {Source, Front, Rear} = take(Type, Conf),
    Front ++ Rear ++ [Source];
do_update({?CMD_MOVE, Type, ?CMD_MOVE_BEFORE(Before)}, Conf) when is_list(Conf) ->
    {S1, Front1, Rear1} = take(Type, Conf),
    {S2, Front2, Rear2} = take(Before, Front1 ++ Rear1),
    Front2 ++ [S1, S2] ++ Rear2;
do_update({?CMD_MOVE, Type, ?CMD_MOVE_AFTER(After)}, Conf) when is_list(Conf) ->
    {S1, Front1, Rear1} = take(Type, Conf),
    {S2, Front2, Rear2} = take(After, Front1 ++ Rear1),
    Front2 ++ [S2, S1] ++ Rear2;
do_update({?CMD_PREPEND, Sources}, Conf) when is_list(Sources), is_list(Conf) ->
    NConf = Sources ++ Conf,
    ok = check_dup_types(NConf),
    NConf;
do_update({?CMD_APPEND, Sources}, Conf) when is_list(Sources), is_list(Conf) ->
    NConf = Conf ++ Sources,
    ok = check_dup_types(NConf),
    NConf;
do_update({{?CMD_REPLACE, Type}, #{<<"enable">> := true} = Source}, Conf) when is_map(Source),
                                                                               is_list(Conf) ->
    case create_dry_run(Type, Source)  of
        ok ->
            {_Old, Front, Rear} = take(Type, Conf),
            NConf = Front ++ [Source | Rear],
            ok = check_dup_types(NConf),
            NConf;
        Error -> Error
    end;
do_update({{?CMD_REPLACE, Type}, Source}, Conf) when is_map(Source), is_list(Conf) ->
    {_Old, Front, Rear} = take(Type, Conf),
    NConf = Front ++ [Source | Rear],
    ok = check_dup_types(NConf),
    NConf;
do_update({{?CMD_DELETE, Type}, _Source}, Conf) when is_list(Conf) ->
    {_Old, Front, Rear} = take(Type, Conf),
    NConf = Front ++ Rear,
    NConf;
do_update({_, Sources}, _Conf) when is_list(Sources)->
    %% overwrite the entire config!
    Sources;
do_update({Op, Sources}, Conf) ->
    error({bad_request, #{op => Op, sources => Sources, conf => Conf}}).

pre_config_update(_, Cmd, Conf) ->
    {ok, do_update(Cmd, Conf)}.


post_config_update(_, _, undefined, _Conf, _AppEnvs) ->
    ok;
post_config_update(_, Cmd, NewSources, _OldSource, _AppEnvs) ->
    ok = do_post_update(Cmd, NewSources),
    ok = emqx_authz_cache:drain_cache().

do_post_update({?CMD_MOVE, _Type, _Where} = Cmd, _NewSources) ->
    InitedSources = lookup(),
    MovedSources = do_update(Cmd, InitedSources),
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [MovedSources]}, -1),
    ok = emqx_authz_cache:drain_cache();
do_post_update({?CMD_PREPEND, Sources}, _NewSources) ->
    InitedSources = init_sources(check_sources(Sources)),
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [InitedSources ++ lookup()]}, -1),
    ok = emqx_authz_cache:drain_cache();
do_post_update({?CMD_APPEND, Sources}, _NewSources) ->
    InitedSources = init_sources(check_sources(Sources)),
    emqx_hooks:put('client.authorize', {?MODULE, authorize, [lookup() ++ InitedSources]}, -1),
    ok = emqx_authz_cache:drain_cache();
do_post_update({{?CMD_REPLACE, Type}, Source}, _NewSources) when is_map(Source) ->
    OldInitedSources = lookup(),
    {OldSource, Front, Rear} = take(Type, OldInitedSources),
    ok = ensure_resource_deleted(OldSource),
    InitedSources = init_sources(check_sources([Source])),
    ok = emqx_hooks:put( 'client.authorize'
                       , {?MODULE, authorize, [Front ++ InitedSources ++ Rear]}, -1),
    ok = emqx_authz_cache:drain_cache();
do_post_update({{?CMD_DELETE, Type}, _Source}, _NewSources) ->
    OldInitedSources = lookup(),
    {OldSource, Front, Rear} = take(Type, OldInitedSources),
    ok = ensure_resource_deleted(OldSource),
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [Front ++ Rear]}, -1),
    ok = emqx_authz_cache:drain_cache();
do_post_update(_, NewSources) ->
    %% overwrite the entire config!
    OldInitedSources = lookup(),
    InitedSources = init_sources(NewSources),
    ok = emqx_hooks:put('client.authorize', {?MODULE, authorize, [InitedSources]}, -1),
    lists:foreach(fun ensure_resource_deleted/1, OldInitedSources),
    ok = emqx_authz_cache:drain_cache().

ensure_resource_deleted(#{enable := false}) -> ok;
ensure_resource_deleted(#{type := file}) -> ok;
ensure_resource_deleted(#{type := 'built-in-database'}) -> ok;
ensure_resource_deleted(#{annotations := #{id := Id}}) -> ok = emqx_resource:remove(Id).

check_dup_types(Sources) ->
    check_dup_types(Sources, []).

check_dup_types([], _Checked) -> ok;
check_dup_types([Source | Sources], Checked) ->
    %% the input might be raw or type-checked result, so lookup both 'type' and <<"type">>
    %% TODO: check: really?
    Type = case maps:get(<<"type">>, Source, maps:get(type, Source, undefined)) of
               undefined ->
                   %% this should never happen if the value is type checked by honcon schema
                   error({bad_source_input, Source});
               Type0 ->
                   type(Type0)
           end,
    case lists:member(Type, Checked) of
        true ->
            %% we have made it clear not to support more than one authz instance for each type
            error({duplicated_authz_source_type, Type});
        false ->
            check_dup_types(Sources, [Type | Checked])
    end.

create_dry_run(T, Source) ->
    case is_connector_source(T) of
        true ->
            [CheckedSource] = check_sources([Source]),
            case T of
                http ->
                    URIMap = maps:get(url, CheckedSource),
                    NSource = maps:put(base_url, maps:remove(query, URIMap), CheckedSource)
            end,
            emqx_resource:create_dry_run(connector_module(T), NSource);
        false ->
            ok
end.

is_connector_source(http) -> true;
is_connector_source(mongodb) -> true;
is_connector_source(mysql) -> true;
is_connector_source(postgresql) -> true;
is_connector_source(redis) -> true;
is_connector_source(_) -> false.

init_sources(Sources) ->
    {_Enabled, Disabled} = lists:partition(fun(#{enable := Enable}) -> Enable end, Sources),
    case Disabled =/= [] of
        true -> ?SLOG(info, #{msg => "disabled_sources_ignored", sources => Disabled});
        false -> ok
    end,
    lists:map(fun init_source/1, Sources).

init_source(#{enable := false} = Source) -> Source;
init_source(#{type := file,
              path := Path
             } = Source) ->
    Rules = case file:consult(Path) of
                {ok, Terms} ->
                    [emqx_authz_rule:compile(Term) || Term <- Terms];
                {error, eacces} ->
                    ?SLOG(alert, #{msg => "insufficient_permissions_to_read_file", path => Path}),
                    error(eaccess);
                {error, enoent} ->
                    ?SLOG(alert, #{msg => "file_does_not_exist", path => Path}),
                    error(enoent);
                {error, Reason} ->
                    ?SLOG(alert, #{msg => "failed_to_read_file", path => Path, reason => Reason}),
                    error(Reason)
            end,
    Source#{annotations => #{rules => Rules}};
init_source(#{type := http,
              url := Url
             } = Source) ->
    NSource= maps:put(base_url, maps:remove(query, Url), Source),
    case create_resource(NSource) of
        {error, Reason} -> error({load_config_error, Reason});
        Id -> Source#{annotations => #{id => Id}}
    end;
init_source(#{type := 'built-in-database'
             } = Source) ->
    Source;
init_source(#{type := DB
             } = Source) when DB =:= redis;
                              DB =:= mongodb ->
    case create_resource(Source) of
        {error, Reason} -> error({load_config_error, Reason});
        Id -> Source#{annotations => #{id => Id}}
    end;
init_source(#{type := DB,
              query := SQL
             } = Source) when DB =:= mysql;
                              DB =:= postgresql ->
    Mod = authz_module(DB),
    case create_resource(Source) of
        {error, Reason} -> error({load_config_error, Reason});
        Id -> Source#{annotations =>
                      #{id => Id,
                        query => erlang:apply(Mod, parse_query, [SQL])
                       }
                   }
    end.

%%--------------------------------------------------------------------
%% AuthZ callbacks
%%--------------------------------------------------------------------

%% @doc Check AuthZ
-spec(authorize( emqx_types:clientinfo()
               , emqx_types:all()
               , emqx_types:topic()
               , allow | deny
               , sources())
      -> {stop, allow} | {ok, deny}).
authorize(#{username := Username,
            peerhost := IpAddress
           } = Client, PubSub, Topic, DefaultResult, Sources) ->
    case do_authorize(Client, PubSub, Topic, Sources) of
        {matched, allow} ->
            ?SLOG(info, #{msg => "authorization_permission_allowed",
                          username => Username,
                          ipaddr => IpAddress,
                          topic => Topic}),
            emqx_metrics:inc(?AUTHZ_METRICS(allow)),
            {stop, allow};
        {matched, deny} ->
            ?SLOG(info, #{msg => "authorization_permission_denied",
                          username => Username,
                          ipaddr => IpAddress,
                          topic => Topic}),
            emqx_metrics:inc(?AUTHZ_METRICS(deny)),
            {stop, deny};
        nomatch ->
            ?SLOG(info, #{msg => "authorization_failed_nomatch",
                          username => Username,
                          ipaddr => IpAddress,
                          topic => Topic,
                          reason => "no-match rule"}),
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
    case erlang:apply(Mod, authorize, [Client, PubSub, Topic, Connector]) of
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

take(Type) -> take(Type, lookup()).

%% Take the source of give type, the sources list is split into two parts
%% front part and rear part.
take(Type, Sources) ->
    {Front, Rear} =  lists:splitwith(fun(T) -> type(T) =/= type(Type) end, Sources),
    case Rear =:= [] of
        true ->
            error({authz_source_of_type_not_found, Type});
        _ ->
            {hd(Rear), Front, tl(Rear)}
    end.

find_action_in_hooks() ->
    Callbacks = emqx_hooks:lookup('client.authorize'),
    [Action] = [Action || {callback,{?MODULE, authorize, _} = Action, _, _} <- Callbacks ],
    Action.

gen_id(Type) ->
    iolist_to_binary([io_lib:format("~ts_~ts",[?APP, Type])]).

create_resource(#{type := DB} = Source) ->
    ResourceID = gen_id(DB),
    case emqx_resource:create(ResourceID, connector_module(DB), Source) of
        {ok, already_created} -> ResourceID;
        {ok, _} -> ResourceID;
        {error, Reason} -> {error, Reason}
    end.

authz_module('built-in-database') ->
    emqx_authz_mnesia;
authz_module(Type) ->
    list_to_existing_atom("emqx_authz_" ++ atom_to_list(Type)).

connector_module(mongodb) ->
    emqx_connector_mongo;
connector_module(postgresql) ->
    emqx_connector_pgsql;
connector_module(Type) ->
    list_to_existing_atom("emqx_connector_" ++ atom_to_list(Type)).

type(#{type := Type}) -> type(Type);
type(#{<<"type">> := Type}) -> type(Type);
type(file) -> file;
type(<<"file">>) -> file;
type(http) -> http;
type(<<"http">>) -> http;
type(mongodb) -> mongodb;
type(<<"mongodb">>) -> mongodb;
type(mysql) -> mysql;
type(<<"mysql">>) -> mysql;
type(redis) -> redis;
type(<<"redis">>) -> redis;
type(postgresql) -> postgresql;
type(<<"postgresql">>) -> postgresql;
type('built-in-database') -> 'built-in-database';
type(<<"built-in-database">>) -> 'built-in-database';
%% should never happend if the input is type-checked by hocon schema
type(Unknown) -> error({unknown_authz_source_type, Unknown}).

%% @doc where the acl.conf file is stored.
acl_conf_file() ->
    filename:join([emqx:data_dir(), "authz", "acl.conf"]).

ph_to_re(VarPH) ->
    re:replace(VarPH, "[\\$\\{\\}]", "\\\\&", [global, {return, list}]).
