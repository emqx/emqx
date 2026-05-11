%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_config).

-export([
    init_config/0,
    update_config/2,
    parsed_config/0,
    parsed_config/1,
    parsed_config/2,
    create_connection/1,
    list_connections/0,
    lookup_connection/1,
    update_connection/2,
    delete_connection/1
]).

-define(CONFIG_KEY, {?MODULE, parsed_config}).
-define(CONNECTIONS, <<"connections">>).
-define(CONNECTION_ID, <<"connection_id">>).

-type connection_id() :: binary().
-type raw_config() :: map().
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
%% Connection CRUD
%%--------------------------------------------------------------------

-spec create_connection(raw_connection()) -> ok | {error, term()}.
create_connection(Body) when is_map(Body) ->
    case required_connection_id(Body) of
        {ok, ConnectionId} ->
            update_raw_config(fun(Config0) ->
                Connections0 = connections(Config0),
                case find_connection(ConnectionId, Connections0) of
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
    connections(current_config()).

-spec lookup_connection(connection_id()) -> {ok, raw_connection()} | {error, not_found}.
lookup_connection(ConnectionId) ->
    find_connection(ConnectionId, list_connections()).

-spec update_connection(connection_id(), raw_connection()) ->
    {ok, raw_connection()} | {error, term()}.
update_connection(ConnectionId, Body) when is_map(Body) ->
    Conn = Body#{?CONNECTION_ID => ConnectionId},
    case
        update_raw_config(fun(Config0) ->
            Connections0 = connections(Config0),
            case replace_connection(ConnectionId, Conn, Connections0) of
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
        Connections0 = connections(Config0),
        case remove_connection(ConnectionId, Connections0) of
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

connections(Config) when is_map(Config) ->
    case maps:get(?CONNECTIONS, Config, []) of
        Connections when is_list(Connections) -> Connections;
        _ -> []
    end.

required_connection_id(#{?CONNECTION_ID := ConnectionId}) when
    is_binary(ConnectionId), ConnectionId =/= <<>>
->
    {ok, ConnectionId};
required_connection_id(#{?CONNECTION_ID := _}) ->
    {error, {invalid_field, ?CONNECTION_ID}};
required_connection_id(_) ->
    {error, {missing_field, ?CONNECTION_ID}}.

find_connection(ConnectionId, Connections) ->
    case
        lists:filter(
            fun(Conn) -> maps:get(?CONNECTION_ID, Conn, undefined) =:= ConnectionId end, Connections
        )
    of
        [Conn | _] -> {ok, Conn};
        [] -> {error, not_found}
    end.

replace_connection(ConnectionId, Conn, Connections) ->
    replace_connection(ConnectionId, Conn, Connections, []).

replace_connection(_ConnectionId, _Conn, [], _Acc) ->
    {error, not_found};
replace_connection(ConnectionId, Conn, [#{?CONNECTION_ID := ConnectionId} | Rest], Acc) ->
    {ok, lists:reverse(Acc) ++ [Conn | Rest]};
replace_connection(ConnectionId, Conn, [Head | Rest], Acc) ->
    replace_connection(ConnectionId, Conn, Rest, [Head | Acc]).

remove_connection(ConnectionId, Connections) ->
    remove_connection(ConnectionId, Connections, []).

remove_connection(_ConnectionId, [], _Acc) ->
    {error, not_found};
remove_connection(ConnectionId, [#{?CONNECTION_ID := ConnectionId} | Rest], Acc) ->
    {ok, lists:reverse(Acc) ++ Rest};
remove_connection(ConnectionId, [Head | Rest], Acc) ->
    remove_connection(ConnectionId, Rest, [Head | Acc]).
