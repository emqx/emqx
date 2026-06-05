%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_config_sync_client).

-feature(maybe_expr, enable).

-export([
    default_root_keys/0,
    default_table_sets/0,
    normalize_config/1,
    sync_once/1,
    sync_once/2
]).

-define(DEFAULT_INTERVAL, <<"5m">>).
-define(DEFAULT_TIMEOUT, <<"30s">>).
-define(DEFAULT_SSL, #{
    <<"enable">> => false,
    <<"verify">> => <<"verify_none">>,
    <<"server_name_indication">> => <<"disable">>,
    <<"cacertfile">> => <<>>,
    <<"certfile">> => <<>>,
    <<"keyfile">> => <<>>
}).

-type request_fun() ::
    fun(
        (
            get | post | delete,
            string(),
            [{string(), string()}],
            undefined | binary(),
            pos_integer()
        ) -> {ok, non_neg_integer(), [{term(), term()}], binary()} | {error, term()}
    ).

-type deps() :: #{
    request_fun => request_fun(),
    upload_fun => fun((binary(), binary()) -> ok | {error, term()}),
    import_fun => fun((binary()) -> emqx_mgmt_data_backup:import_res()),
    delete_local_fun => fun((binary()) -> ok | {error, term()}),
    cancelled_fun => fun(() -> boolean())
}.

-spec default_root_keys() -> [binary()].
default_root_keys() ->
    [
        <<"connectors">>,
        <<"actions">>,
        <<"sources">>,
        <<"rule_engine">>,
        <<"listeners">>,
        <<"schema_registry">>
    ].

-spec default_table_sets() -> [binary()].
default_table_sets() ->
    [
        <<"banned">>,
        <<"builtin_authn">>,
        <<"builtin_authz">>
    ].

-spec normalize_config(map()) -> map().
normalize_config(Conf0) ->
    Primary0 = maps:get(<<"primary">>, Conf0, #{}),
    Sync0 = maps:get(<<"sync">>, Conf0, #{}),
    #{
        <<"enable">> => maps:get(<<"enable">>, Conf0, false),
        <<"role">> => to_bin(maps:get(<<"role">>, Conf0, <<"primary">>)),
        <<"primary">> => #{
            <<"base_url">> => to_bin(maps:get(<<"base_url">>, Primary0, <<>>)),
            <<"api_key">> => to_bin(maps:get(<<"api_key">>, Primary0, <<>>)),
            <<"api_secret">> => to_bin(maps:get(<<"api_secret">>, Primary0, <<>>)),
            <<"ssl">> => normalize_ssl(maps:get(<<"ssl">>, Primary0, #{}))
        },
        <<"sync">> => #{
            <<"interval">> => to_bin(maps:get(<<"interval">>, Sync0, ?DEFAULT_INTERVAL)),
            <<"root_keys">> => to_bin_list(
                maps:get(<<"root_keys">>, Sync0, default_root_keys())
            ),
            <<"table_sets">> => to_bin_list(
                maps:get(<<"table_sets">>, Sync0, default_table_sets())
            ),
            <<"timeout">> => to_bin(maps:get(<<"timeout">>, Sync0, ?DEFAULT_TIMEOUT)),
            <<"delete_remote_backup">> => maps:get(<<"delete_remote_backup">>, Sync0, true),
            <<"delete_local_backup">> => maps:get(<<"delete_local_backup">>, Sync0, true)
        }
    }.

-spec sync_once(map()) -> {ok, map()} | {error, term()}.
sync_once(Conf) ->
    sync_once(Conf, #{}).

-spec sync_once(map(), deps() | map()) -> {ok, map()} | {error, term()}.
sync_once(Conf0, Deps0) ->
    Conf = normalize_config(Conf0),
    Deps = maps:merge(default_deps(Conf), Deps0),
    case validate_sync_config(Conf) of
        ok ->
            do_sync_once(Conf, Deps);
        {error, Reason} ->
            {error, Reason}
    end.

do_sync_once(Conf, Deps) ->
    case check_cancelled(Deps) of
        ok ->
            do_sync_once_active(Conf, Deps);
        {error, Reason} ->
            {error, Reason}
    end.

do_sync_once_active(Conf, Deps) ->
    case export_backup(Conf, Deps) of
        {ok, Filename} ->
            finish_sync_once(Conf, Deps, Filename);
        {error, Reason} ->
            {error, Reason}
    end.

finish_sync_once(Conf, Deps, Filename) ->
    Result = download_upload_import(Conf, Deps, Filename),
    Cleanup = cleanup(Conf, Deps, Filename),
    case {Result, cleanup_succeeded(Cleanup)} of
        {ok, true} ->
            {ok, #{filename => Filename, cleanup => Cleanup}};
        {ok, false} ->
            {error, {cleanup_failed, Cleanup}};
        {{error, Reason}, _} ->
            {error, Reason}
    end.

download_upload_import(Conf, Deps, Filename) ->
    maybe
        ok ?= check_cancelled(Deps),
        {ok, BackupBin} ?= download_backup(Conf, Deps, Filename),
        ok ?= check_cancelled(Deps),
        ok ?= upload_backup(Deps, Filename, BackupBin),
        ok ?= check_cancelled(Deps),
        ok ?= import_backup(Deps, Filename)
    end.

check_cancelled(Deps) ->
    CancelledFun = maps:get(cancelled_fun, Deps),
    case CancelledFun() of
        true -> {error, cancelled};
        false -> ok
    end.

export_backup(Conf, Deps) ->
    Sync = maps:get(<<"sync">>, Conf),
    Body = #{
        <<"root_keys">> => maps:get(<<"root_keys">>, Sync),
        <<"table_sets">> => maps:get(<<"table_sets">>, Sync)
    },
    case request_json(post, path(Conf, "/data/export"), Conf, Deps, emqx_utils_json:encode(Body)) of
        {ok, 200, RespBody} ->
            case decode_json(RespBody) of
                {ok, Resp} ->
                    filename_from_response(Resp);
                {error, Reason} ->
                    {error, {bad_export_response, Reason}}
            end;
        {ok, Status, RespBody} ->
            {error, {http_error, post, path(Conf, "/data/export"), Status, RespBody}};
        {error, Reason} ->
            {error, Reason}
    end.

download_backup(Conf, Deps, Filename) ->
    Url = path(Conf, "/data/files/" ++ uri_encode(Filename)),
    RequestFun = maps:get(request_fun, Deps),
    case RequestFun(get, Url, headers(Conf), undefined, timeout_ms(Conf)) of
        {ok, 200, _Headers, Body} ->
            {ok, Body};
        {ok, Status, _Headers, Body} ->
            {error, {http_error, get, Url, Status, Body}};
        {error, Reason} ->
            {error, {http_error, get, Url, Reason}}
    end.

upload_backup(Deps, Filename, BackupBin) ->
    UploadFun = maps:get(upload_fun, Deps),
    case UploadFun(Filename, BackupBin) of
        ok -> ok;
        {error, Reason} -> {error, {upload_failed, Reason}}
    end.

import_backup(Deps, Filename) ->
    ImportFun = maps:get(import_fun, Deps),
    case ImportFun(Filename) of
        {ok, #{db_errors := DbErrors, config_errors := ConfigErrors}} = Result ->
            case maps:size(DbErrors) =:= 0 andalso maps:size(ConfigErrors) =:= 0 of
                true -> ok;
                false -> {error, {import_failed, Result}}
            end;
        {error, Reason} ->
            {error, {import_failed, Reason}}
    end.

cleanup(Conf, Deps, Filename) ->
    #{
        remote => cleanup_remote(Conf, Deps, Filename),
        local => cleanup_local(Conf, Deps, Filename)
    }.

cleanup_remote(Conf, Deps, Filename) ->
    Sync = maps:get(<<"sync">>, Conf),
    case maps:get(<<"delete_remote_backup">>, Sync) of
        true ->
            Url = path(Conf, "/data/files/" ++ uri_encode(Filename)),
            RequestFun = maps:get(request_fun, Deps),
            case RequestFun(delete, Url, headers(Conf), undefined, timeout_ms(Conf)) of
                {ok, Status, _Headers, _Body} when Status =:= 204; Status =:= 404 ->
                    ok;
                {ok, Status, _Headers, Body} ->
                    {error, {http_error, delete, Url, Status, Body}};
                {error, Reason} ->
                    {error, {http_error, delete, Url, Reason}}
            end;
        false ->
            skipped
    end.

cleanup_local(Conf, Deps, Filename) ->
    Sync = maps:get(<<"sync">>, Conf),
    case maps:get(<<"delete_local_backup">>, Sync) of
        true ->
            DeleteLocalFun = maps:get(delete_local_fun, Deps),
            DeleteLocalFun(Filename);
        false ->
            skipped
    end.

cleanup_succeeded(#{remote := Remote, local := Local}) ->
    cleanup_status_ok(Remote) andalso cleanup_status_ok(Local).

cleanup_status_ok(ok) ->
    true;
cleanup_status_ok(skipped) ->
    true;
cleanup_status_ok(_) ->
    false.

request_json(Method, Url, Conf, Deps, Body) ->
    RequestFun = maps:get(request_fun, Deps),
    case RequestFun(Method, Url, headers(Conf), Body, timeout_ms(Conf)) of
        {ok, Status, _Headers, RespBody} -> {ok, Status, RespBody};
        {error, Reason} -> {error, {http_error, Method, Url, Reason}}
    end.

default_deps(Conf) ->
    #{
        request_fun => fun(Method, Url, Headers, Body, Timeout) ->
            request(Method, Url, Headers, Body, http_options(Conf, Timeout))
        end,
        upload_fun => fun emqx_mgmt_data_backup:upload/2,
        import_fun => fun(Filename) ->
            emqx_mgmt_data_backup:import(Filename, #{mnesia_restore_mode => snapshot})
        end,
        delete_local_fun => fun emqx_mgmt_data_backup:delete_file/1,
        cancelled_fun => fun() -> false end
    }.

request(Method, Url, Headers, Body, HTTPOpts) ->
    _ = application:ensure_all_started(ssl),
    _ = application:ensure_all_started(inets),
    Opts = [{body_format, binary}],
    Result =
        case Method of
            post ->
                httpc:request(
                    post,
                    {Url, Headers, "application/json", Body},
                    HTTPOpts,
                    Opts
                );
            get ->
                httpc:request(get, {Url, Headers}, HTTPOpts, Opts);
            delete ->
                httpc:request(delete, {Url, Headers}, HTTPOpts, Opts)
        end,
    case Result of
        {ok, {{_, Status, _}, RespHeaders, RespBody}} ->
            {ok, Status, RespHeaders, RespBody};
        {error, Reason} ->
            {error, Reason}
    end.

headers(Conf) ->
    Primary = maps:get(<<"primary">>, Conf),
    APIKey = maps:get(<<"api_key">>, Primary),
    APISecret = maps:get(<<"api_secret">>, Primary),
    Token = base64:encode_to_string(iolist_to_binary([APIKey, <<":">>, APISecret])),
    [
        {"Authorization", "Basic " ++ Token},
        {"Accept", "application/json"}
    ].

path(Conf, Path) ->
    Primary = maps:get(<<"primary">>, Conf),
    BaseUrl = trim_trailing_slash(binary_to_list(maps:get(<<"base_url">>, Primary))),
    BaseUrl ++ Path.

timeout_ms(Conf) ->
    Sync = maps:get(<<"sync">>, Conf),
    Duration = maps:get(<<"timeout">>, Sync),
    case emqx_schema:to_duration_ms(Duration) of
        {ok, Ms} when is_integer(Ms), Ms > 0 -> Ms;
        _ -> 30000
    end.

http_options(Conf, Timeout) ->
    HTTPOpts0 = [{timeout, Timeout}, {autoredirect, false}],
    case ssl_options(Conf) of
        [] -> HTTPOpts0;
        SSLOpts -> [{ssl, SSLOpts} | HTTPOpts0]
    end.

ssl_options(Conf) ->
    Primary = maps:get(<<"primary">>, Conf),
    SSL = maps:get(<<"ssl">>, Primary, ?DEFAULT_SSL),
    case maps:get(<<"enable">>, SSL, false) of
        true -> emqx_tls_lib:to_client_opts(client_ssl_options(SSL));
        false -> []
    end.

client_ssl_options(SSL) ->
    #{
        enable => maps:get(<<"enable">>, SSL, false),
        verify => to_verify(maps:get(<<"verify">>, SSL, <<"verify_none">>)),
        server_name_indication => to_sni(
            maps:get(<<"server_name_indication">>, SSL, <<"disable">>)
        ),
        cacertfile => to_string(maps:get(<<"cacertfile">>, SSL, <<>>)),
        certfile => to_string(maps:get(<<"certfile">>, SSL, <<>>)),
        keyfile => to_string(maps:get(<<"keyfile">>, SSL, <<>>))
    }.

validate_sync_config(Conf) ->
    Primary = maps:get(<<"primary">>, Conf),
    case
        {
            maps:get(<<"enable">>, Conf),
            maps:get(<<"role">>, Conf),
            maps:get(<<"base_url">>, Primary),
            maps:get(<<"api_key">>, Primary),
            maps:get(<<"api_secret">>, Primary)
        }
    of
        {true, <<"secondary">>, <<>>, _, _} ->
            {error, missing_primary_base_url};
        {true, <<"secondary">>, _, <<>>, _} ->
            {error, missing_primary_api_key};
        {true, <<"secondary">>, _, _, <<>>} ->
            {error, missing_primary_api_secret};
        _ ->
            validate_primary_ssl(Primary)
    end.

validate_primary_ssl(Primary) ->
    SSL = maps:get(<<"ssl">>, Primary, ?DEFAULT_SSL),
    case maps:get(<<"enable">>, SSL, false) of
        true ->
            Verify = maps:get(<<"verify">>, SSL, <<"verify_none">>),
            case valid_verify(Verify) of
                true -> ok;
                false -> {error, {bad_primary_ssl_verify, Verify}}
            end;
        false ->
            ok
    end.

valid_verify(verify_none) -> true;
valid_verify(verify_peer) -> true;
valid_verify(<<"verify_none">>) -> true;
valid_verify(<<"verify_peer">>) -> true;
valid_verify("verify_none") -> true;
valid_verify("verify_peer") -> true;
valid_verify(_) -> false.

filename_from_response(Resp) ->
    case maps:find(<<"filename">>, Resp) of
        {ok, Filename} ->
            {ok, to_bin(Filename)};
        error ->
            case maps:find(filename, Resp) of
                {ok, Filename} -> {ok, to_bin(Filename)};
                error -> {error, missing_filename}
            end
    end.

decode_json(Body) ->
    try
        {ok, emqx_utils_json:decode(Body)}
    catch
        Class:Reason ->
            {error, {Class, Reason}}
    end.

uri_encode(Bin) when is_binary(Bin) ->
    emqx_http_lib:uri_encode(binary_to_list(Bin));
uri_encode(List) when is_list(List) ->
    emqx_http_lib:uri_encode(List).

trim_trailing_slash([]) ->
    [];
trim_trailing_slash("/") ->
    "";
trim_trailing_slash(Str) ->
    case lists:last(Str) of
        $/ -> trim_trailing_slash(lists:droplast(Str));
        _ -> Str
    end.

to_bin(Bin) when is_binary(Bin) -> Bin;
to_bin(List) when is_list(List) -> unicode:characters_to_binary(List);
to_bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

to_bin_list(List) when is_list(List) ->
    [to_bin(Item) || Item <- List].

normalize_ssl(SSL0) ->
    Default = ?DEFAULT_SSL,
    #{
        <<"enable">> => maps:get(<<"enable">>, SSL0, maps:get(<<"enable">>, Default)),
        <<"verify">> => to_bin(maps:get(<<"verify">>, SSL0, maps:get(<<"verify">>, Default))),
        <<"server_name_indication">> =>
            to_bin(
                maps:get(
                    <<"server_name_indication">>,
                    SSL0,
                    maps:get(<<"server_name_indication">>, Default)
                )
            ),
        <<"cacertfile">> => to_bin(
            maps:get(<<"cacertfile">>, SSL0, maps:get(<<"cacertfile">>, Default))
        ),
        <<"certfile">> => to_bin(maps:get(<<"certfile">>, SSL0, maps:get(<<"certfile">>, Default))),
        <<"keyfile">> => to_bin(maps:get(<<"keyfile">>, SSL0, maps:get(<<"keyfile">>, Default)))
    }.

to_verify(verify_none) -> verify_none;
to_verify(verify_peer) -> verify_peer;
to_verify(<<"verify_none">>) -> verify_none;
to_verify(<<"verify_peer">>) -> verify_peer;
to_verify("verify_none") -> verify_none;
to_verify("verify_peer") -> verify_peer.

to_sni(<<"disable">>) -> disable;
to_sni(disable) -> disable;
to_sni(Value) -> to_string(Value).

to_string(<<>>) -> "";
to_string(Bin) when is_binary(Bin) -> unicode:characters_to_list(Bin);
to_string(List) when is_list(List) -> List;
to_string(Atom) when is_atom(Atom) -> atom_to_list(Atom).
