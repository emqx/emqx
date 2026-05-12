%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_config).

-moduledoc """
Owns the plugin configuration boundary for emqx_agent.

Public CRUD functions work with the raw plugin config shape: binary-keyed maps
as received from the plugin API and as rendered back to the UI. They perform a
read-update-validate-write cycle over the whole plugin config: fetch the raw
config with emqx_plugins:get_config/2, update only the relevant part, validate
the complete config with emqx_agent_schema, and persist it with
emqx_mgmt_api_plugins:put_plugin_config/2.

put_plugin_config/2 is the propagation point. It distributes the raw config
update through the plugin config machinery and eventually calls the plugin's
on_config_changed callback on each node. That callback calls update_config/2,
which parses the propagated raw config through hocon and stores the parsed,
atom-keyed config in persistent_term.

Runtime code should read parsed_config/0,1,2 instead of re-reading raw plugin
config. API code should use the raw CRUD functions so responses preserve the
same external shape that was submitted by users.
""".

-export([
    init_config/0,
    update_config/2,
    parsed_config/0,
    parsed_config/1,
    parsed_config/2
]).

-export([
    create_skill/1,
    list_skills/0,
    lookup_skill/2,
    update_skill/3,
    delete_skill/2
]).

-export([
    create_connection/1,
    list_connections/0,
    lookup_connection/1,
    update_connection/2,
    delete_connection/1
]).

-define(CONFIG_KEY, {?MODULE, parsed_config}).
-define(SKILLS, <<"skills">>).
-define(SKILL_TYPE, <<"type">>).
-define(SKILL_ID, <<"id">>).
-define(CONNECTIONS, <<"connections">>).
-define(CONNECTION_ID, <<"id">>).

-type skill_type() :: binary().
-type skill_id() :: binary().
-type connection_id() :: binary().
-type raw_config() :: map().
-type raw_skill() :: map().
-type raw_connection() :: map().

%%--------------------------------------------------------------------
%% Config lifecycle
%%--------------------------------------------------------------------

-spec init_config() -> ok | {error, term()}.
init_config() ->
    case parse_config(current_config()) of
        {ok, Parsed} ->
            cache_config(Parsed);
        {error, _} = Error ->
            Error
    end.

-spec update_config(raw_config(), raw_config()) -> ok | {error, term()}.
update_config(_OldConfig, NewConfig) ->
    case parse_config(NewConfig) of
        {ok, Parsed} ->
            cache_config(Parsed);
        {error, _} = Error ->
            Error
    end.

-spec parsed_config() -> map().
parsed_config() ->
    persistent_term:get(?CONFIG_KEY, #{}).

-spec parsed_config([term()]) -> term().
parsed_config(Path) ->
    parsed_config(Path, undefined).

-spec parsed_config([term()], term()) -> term().
parsed_config(Path, Default) ->
    emqx_utils_maps:deep_get(Path, parsed_config(), Default).

%%--------------------------------------------------------------------
%% Skill CRUD
%%--------------------------------------------------------------------

-spec create_skill(raw_skill()) -> ok | {error, term()}.
create_skill(Body) when is_map(Body) ->
    case required_skill_key(Body) of
        {ok, Type, SkillId} ->
            update_raw_config(fun(Config0) ->
                Skills0 = entries(Config0, ?SKILLS),
                Pred = skill_pred(Type, SkillId),
                case find_entry(Pred, Skills0) of
                    {ok, _} ->
                        {error, already_exists};
                    {error, not_found} ->
                        {ok, Config0#{?SKILLS => Skills0 ++ [Body]}}
                end
            end);
        {error, _} = Error ->
            Error
    end;
create_skill(_) ->
    {error, invalid_skill}.

-spec list_skills() -> [raw_skill()].
list_skills() ->
    entries(current_config(), ?SKILLS).

-spec lookup_skill(skill_type(), skill_id()) -> {ok, raw_skill()} | {error, not_found}.
lookup_skill(Type, SkillId) ->
    find_entry(skill_pred(Type, SkillId), list_skills()).

-spec update_skill(skill_type(), skill_id(), raw_skill()) ->
    {ok, raw_skill()} | {error, term()}.
update_skill(Type, SkillId, Body) when is_map(Body) ->
    Skill = Body#{?SKILL_TYPE => Type, ?SKILL_ID => SkillId},
    case
        update_raw_config(fun(Config0) ->
            Skills0 = entries(Config0, ?SKILLS),
            case replace_entry(skill_pred(Type, SkillId), Skill, Skills0) of
                {ok, Skills} -> {ok, Config0#{?SKILLS => Skills}};
                {error, _} = Error -> Error
            end
        end)
    of
        ok -> lookup_skill(Type, SkillId);
        {error, _} = Error -> Error
    end;
update_skill(_, _, _) ->
    {error, invalid_skill}.

-spec delete_skill(skill_type(), skill_id()) -> ok | {error, term()}.
delete_skill(Type, SkillId) ->
    update_raw_config(fun(Config0) ->
        Skills0 = entries(Config0, ?SKILLS),
        case remove_entry(skill_pred(Type, SkillId), Skills0) of
            {ok, Skills} -> {ok, Config0#{?SKILLS => Skills}};
            {error, _} = Error -> Error
        end
    end).

%%--------------------------------------------------------------------
%% Connection CRUD
%%--------------------------------------------------------------------

-spec create_connection(raw_connection()) -> ok | {error, term()}.
create_connection(Body) when is_map(Body) ->
    case required_id(Body) of
        {ok, ConnectionId} ->
            update_raw_config(fun(Config0) ->
                Connections0 = entries(Config0, ?CONNECTIONS),
                case find_entry(id_pred(ConnectionId), Connections0) of
                    {ok, _} ->
                        {error, already_exists};
                    {error, not_found} ->
                        {ok, Config0#{?CONNECTIONS => Connections0 ++ [Body]}}
                end
            end);
        {error, _} = Error ->
            Error
    end;
create_connection(_) ->
    {error, invalid_connection}.

-spec list_connections() -> [raw_connection()].
list_connections() ->
    entries(current_config(), ?CONNECTIONS).

-spec lookup_connection(connection_id()) -> {ok, raw_connection()} | {error, not_found}.
lookup_connection(ConnectionId) ->
    find_entry(id_pred(ConnectionId), list_connections()).

-spec update_connection(connection_id(), raw_connection()) ->
    {ok, raw_connection()} | {error, term()}.
update_connection(ConnectionId, Body) when is_map(Body) ->
    Conn = Body#{?CONNECTION_ID => ConnectionId},
    case
        update_raw_config(fun(Config0) ->
            Connections0 = entries(Config0, ?CONNECTIONS),
            case replace_entry(id_pred(ConnectionId), Conn, Connections0) of
                {ok, Connections} -> {ok, Config0#{?CONNECTIONS => Connections}};
                {error, _} = Error -> Error
            end
        end)
    of
        ok -> lookup_connection(ConnectionId);
        {error, _} = Error -> Error
    end;
update_connection(_, _) ->
    {error, invalid_connection}.

-spec delete_connection(connection_id()) -> ok | {error, term()}.
delete_connection(ConnectionId) ->
    update_raw_config(fun(Config0) ->
        Connections0 = entries(Config0, ?CONNECTIONS),
        case remove_entry(id_pred(ConnectionId), Connections0) of
            {ok, Connections} -> {ok, Config0#{?CONNECTIONS => Connections}};
            {error, _} = Error -> Error
        end
    end).

%%--------------------------------------------------------------------
%% Internal config operations
%%--------------------------------------------------------------------

current_config() ->
    try emqx_plugins:get_config(name_vsn(), #{}) of
        Config when is_map(Config) -> Config;
        _ -> #{}
    catch
        _:_ -> #{}
    end.

name_vsn() ->
    {ok, Vsn} = application:get_key(emqx_agent, vsn),
    iolist_to_binary([<<"emqx_agent-">>, Vsn]).

update_raw_config(Fun) ->
    Config0 = current_config(),
    case Fun(Config0) of
        {ok, Config} ->
            case parse_config(Config) of
                {ok, _Parsed} ->
                    emqx_mgmt_api_plugins:put_plugin_config(name_vsn(), Config);
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

parse_config(Config) when is_map(Config) ->
    Schema = #{roots => [{config, emqx_agent_schema:config_type()}]},
    try hocon_tconf:check_plain(Schema, #{<<"config">> => Config}, #{atom_key => true}) of
        #{config := Parsed} -> {ok, Parsed}
    catch
        throw:Error -> {error, Error}
    end.

cache_config(Parsed) ->
    persistent_term:put(?CONFIG_KEY, Parsed),
    ok.

entries(Config, Section) when is_map(Config) ->
    case maps:get(Section, Config, []) of
        Entries when is_list(Entries) -> Entries;
        _ -> []
    end.

required_id(#{?CONNECTION_ID := Id}) when is_binary(Id), Id =/= <<>> ->
    {ok, Id};
required_id(#{?CONNECTION_ID := _}) ->
    {error, {invalid_field, ?CONNECTION_ID}};
required_id(_) ->
    {error, {missing_field, ?CONNECTION_ID}}.

required_skill_key(#{?SKILL_TYPE := Type, ?SKILL_ID := SkillId}) when
    is_binary(Type), Type =/= <<>>, is_binary(SkillId), SkillId =/= <<>>
->
    {ok, Type, SkillId};
required_skill_key(#{?SKILL_TYPE := Type, ?SKILL_ID := _}) when
    not is_binary(Type); Type =:= <<>>
->
    {error, {invalid_field, ?SKILL_TYPE}};
required_skill_key(#{?SKILL_TYPE := _, ?SKILL_ID := _}) ->
    {error, {invalid_field, ?SKILL_ID}};
required_skill_key(#{?SKILL_ID := _}) ->
    {error, {missing_field, ?SKILL_TYPE}};
required_skill_key(#{?SKILL_TYPE := _}) ->
    {error, {missing_field, ?SKILL_ID}};
required_skill_key(_) ->
    {error, {missing_field, ?SKILL_TYPE}}.

id_pred(Id) ->
    fun(Entry) -> maps:get(?CONNECTION_ID, Entry, undefined) =:= Id end.

skill_pred(Type, SkillId) ->
    fun(Skill) ->
        maps:get(?SKILL_TYPE, Skill, undefined) =:= Type andalso
            maps:get(?SKILL_ID, Skill, undefined) =:= SkillId
    end.

find_entry(Pred, Entries) ->
    case lists:filter(Pred, Entries) of
        [Entry | _] -> {ok, Entry};
        [] -> {error, not_found}
    end.

replace_entry(Pred, Entry, Entries) ->
    replace_entry(Pred, Entry, Entries, []).

replace_entry(_Pred, _Entry, [], _Acc) ->
    {error, not_found};
replace_entry(Pred, Entry, [Head | Rest], Acc) ->
    case Pred(Head) of
        true -> {ok, lists:reverse(Acc) ++ [Entry | Rest]};
        false -> replace_entry(Pred, Entry, Rest, [Head | Acc])
    end.

remove_entry(Pred, Entries) ->
    remove_entry(Pred, Entries, []).

remove_entry(_Pred, [], _Acc) ->
    {error, not_found};
remove_entry(Pred, [Head | Rest], Acc) ->
    case Pred(Head) of
        true -> {ok, lists:reverse(Acc) ++ Rest};
        false -> remove_entry(Pred, Rest, [Head | Acc])
    end.
