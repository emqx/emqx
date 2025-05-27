%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ldap).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% ecpool connect & reconnect
-export([
    pool_options/1,
    connect/1,
    get_status_with_poolname/1,
    search/2,
    simple_bind/3
]).

-export([namespace/0, roots/0, fields/1, desc/1]).

%% Internal exports
-export([do_get_status/1]).

-define(LDAP_HOST_OPTIONS, #{
    default_port => 389
}).
-define(REDACT_VAL, "******").

%%--------------------------------------------------------------------
%% Schema
%%--------------------------------------------------------------------

namespace() -> "ldap".

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [
        {server, server()},
        {pool_size, fun emqx_connector_schema_lib:pool_size/1},
        {username, fun ensure_username/1},
        {password, emqx_connector_schema_lib:password_field()},
        {request_timeout,
            ?HOCON(emqx_schema:timeout_duration_ms(), #{
                desc => ?DESC(request_timeout),
                default => <<"10s">>
            })},
        {ssl,
            ?HOCON(?R_REF(?MODULE, ssl), #{
                default => #{<<"enable">> => false},
                desc => ?DESC(emqx_connector_schema_lib, "ssl")
            })}
    ];
fields(ssl) ->
    Schema = emqx_schema:client_ssl_opts_schema(#{}),
    lists:keydelete("user_lookup_fun", 1, Schema);
fields(search_options) ->
    [
        {base_dn,
            ?HOCON(binary(), #{
                desc => ?DESC(base_dn),
                required => true,
                example => <<"uid=${username},ou=testdevice,dc=emqx,dc=io">>,
                validator => fun dn_validator/1
            })},
        {filter,
            ?HOCON(binary(), #{
                desc => ?DESC(filter),
                default => <<"(objectClass=mqttUser)">>,
                example => <<"(& (objectClass=mqttUser) (uid=${username}))">>,
                validator => fun filter_validator/1,
                converter => fun filter_converter/2
            })}
    ].

desc(ssl) ->
    ?DESC(emqx_connector_schema_lib, "ssl");
desc(_) ->
    undefined.

server() ->
    Meta = #{desc => ?DESC("server")},
    emqx_schema:servers_sc(Meta, ?LDAP_HOST_OPTIONS).

ensure_username(required) ->
    true;
ensure_username(Field) ->
    emqx_connector_schema_lib:username(Field).

filter_converter(Filter, _Opts) ->
    case re:run(Filter, "^\\s+$") of
        nomatch -> Filter;
        _ -> <<"(objectClass=mqttUser)">>
    end.

filter_validator(Filter) ->
    maybe
        {ok, _} ?= emqx_ldap_filter:parse(Filter),
        ok
    end.

dn_validator(DN) ->
    maybe
        {ok, _} ?= emqx_ldap_dn:parse(DN),
        ok
    end.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

pool_options(
    #{
        server := Server,
        pool_size := PoolSize,
        ssl := SSL
    } = Config
) ->
    HostPort = emqx_schema:parse_server(Server, ?LDAP_HOST_OPTIONS),
    Config2 = maps:merge(Config, HostPort),
    Config3 =
        case maps:get(enable, SSL) of
            true ->
                Config2#{sslopts => emqx_tls_lib:to_client_opts(SSL)};
            false ->
                Config2
        end,

    [
        {pool_size, PoolSize},
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {options, Config3}
    ].

connect(Options) ->
    #{
        hostname := Host,
        username := Username,
        password := Password,
        request_timeout := RequestTimeout
    } =
        Conf = proplists:get_value(options, Options),
    OpenOpts = maps:to_list(maps:with([port, sslopts], Conf)),
    LogTag = proplists:get_value(log_tag, Options),
    case eldap:open([Host], [{log, mk_log_func(LogTag)}, {timeout, RequestTimeout} | OpenOpts]) of
        {ok, Handle} = Ret ->
            %% TODO: teach `eldap` to accept 0-arity closures as passwords.
            %% TODO: do we need to bind for emqx_ldap_bind_connector?
            case eldap:simple_bind(Handle, Username, emqx_secret:unwrap(Password)) of
                ok -> Ret;
                Error -> Error
            end;
        Error ->
            Error
    end.

search(Pid, SearchOptions) ->
    Result = eldap:search(Pid, SearchOptions),
    handle_ldap_result(Pid, Result).

simple_bind(Pid, DN, Password) ->
    Result = eldap:simple_bind(Pid, DN, Password),
    handle_ldap_result(Pid, Result).

get_status_with_poolname(PoolName) ->
    case emqx_resource_pool:health_check_workers(PoolName, fun ?MODULE:do_get_status/1) of
        true ->
            ?status_connected;
        false ->
            %% Note: here can only return `disconnected` not `connecting`
            %% because the LDAP socket/connection can't be reused
            %% searching on a died socket will never return until timeout
            ?status_disconnected
    end.

do_get_status(Conn) ->
    %% search with an invalid base object
    %% if the server is down, the result is {error, ldap_closed}
    %% otherwise is {error, invalidDNSyntax/timeout}
    {error, ldap_closed} =/=
        eldap:search(Conn, [{base, "cn=checkalive"}, {filter, eldap:'approxMatch'("", "")}]).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% NOTE
%% ldap server closing the socket does not result in
%% process restart, so we need to kill it to trigger a quick reconnect
%% instead of waiting for the next health-check
handle_ldap_result(Pid, {error, ldap_closed}) ->
    _ = exit(Pid, kill),
    {error, ldap_closed};
handle_ldap_result(Pid, {error, {gen_tcp_error, _} = Reason}) ->
    _ = exit(Pid, kill),
    {error, Reason};
handle_ldap_result(_Pid, Result) ->
    Result.

%% Note: the value of the `_Level` here always is 2
mk_log_func(LogTag) ->
    fun(_Level, Format, Args) ->
        ?SLOG(
            debug,
            #{
                msg => LogTag,
                log => io_lib:format(Format, [redact_ldap_log(Arg) || Arg <- Args])
            }
        )
    end.

redact_ldap_log({'BindRequest', Version, Name, {simple, _}}) ->
    {'BindRequest', Version, Name, {simple, ?REDACT_VAL}};
redact_ldap_log(Arg) ->
    Arg.
