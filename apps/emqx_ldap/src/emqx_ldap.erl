%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ldap).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("eldap/include/eldap.hrl").

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_get_status/2
]).

%% ecpool connect & reconnect
-export([connect/1]).

-export([roots/0, fields/1]).

-export([do_get_status/1]).

-define(LDAP_HOST_OPTIONS, #{
    default_port => 389
}).

-type params_tokens() :: #{atom() => list()}.
-type state() ::
    #{
        pool_name := binary(),
        base_tokens := params_tokens(),
        filter_tokens := params_tokens()
    }.

-define(ECS, emqx_connector_schema_lib).

%%=====================================================================
%% Hocon schema
roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [
        {server, server()},
        {pool_size, fun ?ECS:pool_size/1},
        {username, fun ensure_username/1},
        {password, fun ?ECS:password/1},
        {base_object,
            ?HOCON(binary(), #{
                desc => ?DESC(base_object),
                required => true
            })},
        {filter, ?HOCON(binary(), #{desc => ?DESC(filter), default => ""})},
        {auto_reconnect, fun ?ECS:auto_reconnect/1}
    ] ++ emqx_connector_schema_lib:ssl_fields().

server() ->
    Meta = #{desc => ?DESC("server")},
    emqx_schema:servers_sc(Meta, ?LDAP_HOST_OPTIONS).

ensure_username(required) ->
    true;
ensure_username(Field) ->
    ?ECS:username(Field).

%% ===================================================================
callback_mode() -> always_sync.

-spec on_start(binary(), hoconsc:config()) -> {ok, state()} | {error, _}.
on_start(
    InstId,
    #{
        server := Server,
        pool_size := PoolSize,
        ssl := SSL
    } = Config
) ->
    HostPort = emqx_schema:parse_server(Server, ?LDAP_HOST_OPTIONS),
    ?SLOG(info, #{
        msg => "starting_ldap_connector",
        connector => InstId,
        config => emqx_utils:redact(Config)
    }),

    Config2 = maps:merge(Config, HostPort),
    Config3 =
        case maps:get(enable, SSL) of
            true ->
                Config2#{sslopts => emqx_tls_lib:to_client_opts(SSL)};
            false ->
                Config2
        end,
    Options = [
        {pool_size, PoolSize},
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {options, Config3}
    ],

    case emqx_resource_pool:start(InstId, ?MODULE, Options) of
        ok ->
            {ok, prepare_template(Config, #{pool_name => InstId})};
        {error, Reason} ->
            ?tp(
                ldap_connector_start_failed,
                #{error => Reason}
            ),
            {error, Reason}
    end.

on_stop(InstId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_ldap_connector",
        connector => InstId
    }),
    emqx_resource_pool:stop(InstId).

on_query(InstId, {query, Data}, State) ->
    on_query(InstId, {query, Data}, [], State);
on_query(InstId, {query, Data, Attrs}, State) ->
    on_query(InstId, {query, Data}, [{attributes, Attrs}], State);
on_query(InstId, {query, Data, Attrs, Timeout}, State) ->
    on_query(InstId, {query, Data}, [{attributes, Attrs}, {timeout, Timeout}], State).

on_get_status(_InstId, #{pool_name := PoolName} = _State) ->
    case emqx_resource_pool:health_check_workers(PoolName, fun ?MODULE:do_get_status/1) of
        true ->
            connected;
        false ->
            connecting
    end.

do_get_status(Conn) ->
    erlang:is_process_alive(Conn).

%% ===================================================================

connect(Options) ->
    #{host := Host, username := Username, password := Password} =
        Conf = proplists:get_value(options, Options),
    OpenOpts = maps:to_list(maps:with([port, sslopts], Conf)),
    case eldap:open([Host], [{log, fun log/3}, OpenOpts]) of
        {ok, Handle} = Ret ->
            case eldap:simple_bind(Handle, Username, Password) of
                ok -> Ret;
                Error -> Error
            end;
        Error ->
            Error
    end.

on_query(
    InstId,
    {query, Data},
    SearchOptions,
    #{base_tokens := BaseTks, filter_tokens := FilterTks} = State
) ->
    Base = emqx_placeholder:proc_tmpl(BaseTks, Data),
    case FilterTks of
        [] ->
            do_ldap_query(InstId, [{base, Base} | SearchOptions], State);
        _ ->
            FilterBin = emqx_placeholder:proc_tmpl(FilterTks, Data),
            %% TODO
            Filter = FilterBin,
            do_ldap_query(
                InstId,
                [{base, Base}, {filter, Filter} | SearchOptions],
                State
            )
    end.

do_ldap_query(
    InstId,
    SearchOptions,
    #{pool_name := PoolName} = State
) ->
    LogMeta = #{connector => InstId, search => SearchOptions, state => State},
    ?TRACE("QUERY", "ldap_connector_received", LogMeta),
    case
        ecpool:pick_and_do(
            PoolName,
            {eldap, search, SearchOptions},
            handover
        )
    of
        {ok, Result} ->
            ?tp(
                ldap_connector_query_return,
                #{result => Result}
            ),
            {ok,
                case Result#eldap_search_result.entries of
                    [First | _] ->
                        %% TODO Support multi entries?
                        First;
                    _ ->
                        undefined
                end};
        {error, Reason} ->
            ?SLOG(
                error,
                LogMeta#{msg => "ldap_connector_do_sql_query_failed", reason => Reason}
            ),
            {error, {unrecoverable_error, Reason}}
    end.

log(Level, Format, Args) ->
    ?SLOG(
        Level,
        #{
            msg => "ldap_log",
            log => io_lib:format(Format, Args)
        }
    ).

prepare_template(Config, State) ->
    do_prepare_template(maps:to_list(maps:with([base_object, filter], Config)), State).

do_prepare_template([{base_object, V} | T], State) ->
    do_prepare_template(T, State#{base_tokens => emqx_placeholder:preproc_tmpl(V)});
do_prepare_template([{filter, V} | T], State) ->
    do_prepare_template(T, State#{filter_tokens => emqx_placeholder:preproc_tmpl(V)}).
