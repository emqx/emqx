%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_rule_udf_client).

-moduledoc """
Client of an emqx-rule-udf/1 function server (see `PROTOCOL.md`).

`call/2` and `try_call/2` are the hot path. They run in the process that
evaluates the rule. They read the client settings from `persistent_term` and the
program from `emqx_rule_udf_store`, and send `POST /v1/functions/{name}/call` through
an `ehttpc` pool.

After a `FUNCTION_NOT_FOUND` or `SHA256_MISMATCH` reply, the call is repeated
once with the program in its body. The server loads it and runs the call in one
request, so this works when a load balancer sends the two requests to different
instances.

The `gen_server` owns the HTTP pool, which it restarts when the plugin config
changes.
""".

-behaviour(gen_server).

%% Hot path
-export([call/2, try_call/2]).

%% Management
-export([
    start_link/0,
    update_config/1,
    upload/3,
    delete/1,
    server_info/0,
    version/2
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("emqx_rule_udf.hrl").
-include_lib("emqx/include/emqx_schema.hrl").

-define(POOL, emqx_rule_udf_http).
-define(PT_CLIENT, {?MODULE, client}).
-define(DEFAULT_URL, <<"http://127.0.0.1:8080">>).
-define(POOL_STOP_TIMEOUT, 5_000).
-define(MAX_ERROR_BODY_BYTES, 256).

-type client() :: #{
    base_path := binary(),
    token := emqx_secret:t(binary()) | undefined,
    timeout := timeout()
}.

%%--------------------------------------------------------------------
%% Hot path
%%--------------------------------------------------------------------

-doc """
Call the program `Name` and return its result. Raise an error when the call
fails, so the rule fails for this message and the broker continues.
""".
-spec call(binary(), [term()]) -> term().
call(Name, Args) ->
    case try_call(Name, Args) of
        {ok, Result} -> Result;
        {error, Reason} -> error({udf_call_failed, Name, Reason})
    end.

-spec try_call(binary(), [term()]) -> {ok, term()} | {error, map()}.
try_call(Name, Args) ->
    maybe
        {ok, Client} ?= client(),
        {ok, Program} ?= lookup(Name),
        {ok, Body} ?= encode_call(#{<<"args">> => Args}),
        call_with_program(Client, Program, Args, Body)
    end.

client() ->
    case persistent_term:get(?PT_CLIENT, undefined) of
        undefined -> {error, #{reason => client_not_ready}};
        Client -> {ok, Client}
    end.

lookup(Name) ->
    case emqx_rule_udf_store:lookup(Name) of
        {ok, _} = Ok -> Ok;
        {error, not_found} -> {error, #{reason => program_not_found}}
    end.

encode_call(Request) ->
    %% No `force_utf8': a binary that is not valid UTF-8 fails the call instead
    %% of reaching the program with replaced characters.
    case emqx_utils_json:safe_encode(Request, []) of
        {ok, Body} -> {ok, Body};
        {error, Reason} -> {error, #{reason => args_not_json, detail => Reason}}
    end.

%% Send the call without the program. When the server does not have this
%% version, send it once more with the program.
call_with_program(Client, #?PROGRAM_TAB{name = Name, sha256 = Sha256} = Program, Args, Body) ->
    case do_call(Client, Name, Sha256, Body) of
        {error, #{code := Code}} when
            Code =:= <<"FUNCTION_NOT_FOUND">>; Code =:= <<"SHA256_MISMATCH">>
        ->
            #?PROGRAM_TAB{runtime = Runtime, source = Source} = Program,
            Function = #{<<"runtime">> => Runtime, <<"source">> => Source},
            {ok, Body2} = encode_call(#{<<"args">> => Args, <<"function">> => Function}),
            do_call(Client, Name, Sha256, Body2);
        Other ->
            Other
    end.

do_call(Client, Name, Sha256, Body) ->
    Path = [<<"/v1/functions/">>, Name, <<"/call">>],
    Headers = [{<<"udf-function-sha256">>, Sha256}],
    case request(Client, post, Path, Headers, Body) of
        {ok, 200, #{<<"result">> := Result}} ->
            {ok, Result};
        {ok, 200, Other} ->
            {error, #{reason => bad_response, body => truncate(Other)}};
        {error, _} = Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% Management
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc "Apply a new plugin config. Return an error, and keep the old settings, when it is invalid.".
-spec update_config(map()) -> ok | {error, term()}.
update_config(RawConfig) ->
    case whereis(?MODULE) of
        undefined ->
            %% The plugin is stopped. `init/1' reads the saved config when it starts.
            case parse_config(RawConfig) of
                {ok, _} -> ok;
                {error, _} = Error -> Error
            end;
        _Pid ->
            gen_server:call(?MODULE, {update_config, RawConfig}, infinity)
    end.

-doc """
Upload a program to the function server. The server checks and loads it before
it replies, so `{ok, Sha256}` means that calls can run it.
""".
-spec upload(binary(), binary(), binary()) -> {ok, binary()} | {error, map()}.
upload(Name, Runtime, Source) ->
    maybe
        {ok, Client} ?= client(),
        Body = emqx_utils_json:encode(#{<<"runtime">> => Runtime, <<"source">> => Source}),
        {ok, 200, Resp} ?= request(Client, put, [<<"/v1/functions/">>, Name], [], Body),
        Sha256 = version(Runtime, Source),
        case maps:get(<<"sha256">>, Resp, undefined) of
            Sha256 ->
                {ok, Sha256};
            Other ->
                %% Client and server disagree on the version rule. Every pinned
                %% call would fail with SHA256_MISMATCH.
                {error, #{reason => version_mismatch, local => Sha256, server => Other}}
        end
    end.

-doc "Delete a program from the function server. A missing program is not an error.".
-spec delete(binary()) -> ok | {error, map()}.
delete(Name) ->
    maybe
        {ok, Client} ?= client(),
        {ok, 200, _} ?= request(Client, delete, [<<"/v1/functions/">>, Name], [], undefined),
        ok
    else
        {error, #{code := <<"FUNCTION_NOT_FOUND">>}} -> ok;
        {error, _} = Error -> Error
    end.

-doc "Send `GET /v1/info` to the function server and return the decoded body.".
-spec server_info() -> {ok, map()} | {error, map()}.
server_info() ->
    maybe
        {ok, Client} ?= client(),
        {ok, 200, Info} ?= request(Client, get, <<"/v1/info">>, [], undefined),
        {ok, Info}
    end.

-doc "Return the emqx-rule-udf/1 version of a program (`PROTOCOL.md`, section 4).".
-spec version(binary(), binary()) -> binary().
version(Runtime, Source) ->
    binary:encode_hex(crypto:hash(sha256, [Runtime, $\n, Source]), lowercase).

%%--------------------------------------------------------------------
%% gen_server
%%--------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    State = #{server => undefined},
    case apply_config(load_plugin_config(), State) of
        {ok, NewState} ->
            {ok, NewState};
        {error, Reason} ->
            ?LOG(error, #{msg => "udf_bad_plugin_config", reason => Reason}),
            {ok, State}
    end.

handle_call({update_config, RawConfig}, _From, State) ->
    case apply_config(RawConfig, State) of
        {ok, NewState} -> {reply, ok, NewState};
        {error, _} = Error -> {reply, Error, State}
    end;
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    stop_pool().

%%--------------------------------------------------------------------
%% Config
%%--------------------------------------------------------------------

load_plugin_config() ->
    {ok, Vsn} = application:get_key(emqx_rule_udf, vsn),
    NameVsn = iolist_to_binary([?PLUGIN_NAME, "-", Vsn]),
    try
        emqx_plugins:get_config(NameVsn, #{})
    catch
        _:_ -> #{}
    end.

apply_config(RawConfig, #{server := OldServer} = State) ->
    maybe
        {ok, NewServer} ?= parse_config(RawConfig),
        case NewServer =:= OldServer andalso is_pool_running() of
            true ->
                {ok, State};
            false ->
                stop_pool(),
                case start_pool(NewServer) of
                    ok ->
                        {ok, State#{server => NewServer}};
                    {error, _} = Error ->
                        restore_pool(OldServer),
                        Error
                end
        end
    end.

is_pool_running() ->
    persistent_term:get(?PT_CLIENT, undefined) =/= undefined.

%% The pool supervisor has `shutdown => infinity'. A worker that does not stop
%% would block the config update, so kill the pool after a timeout.
stop_pool() ->
    _ = persistent_term:erase(?PT_CLIENT),
    PoolSup = pool_sup_pid(),
    {Stopper, MRef} = spawn_monitor(fun() -> exit(ehttpc_sup:stop_pool(?POOL)) end),
    receive
        {'DOWN', MRef, process, Stopper, _} -> ok
    after ?POOL_STOP_TIMEOUT ->
        ?LOG(warning, #{msg => "udf_http_pool_stop_timeout", timeout_ms => ?POOL_STOP_TIMEOUT}),
        is_pid(PoolSup) andalso exit(PoolSup, kill),
        receive
            {'DOWN', MRef, process, Stopper, _} -> ok
        end
    end.

%% `ehttpc_sup' names the pool child `{ehttpc_pool_sup, Pool}'.
pool_sup_pid() ->
    Children = supervisor:which_children(ehttpc_sup),
    case lists:keyfind({ehttpc_pool_sup, ?POOL}, 1, Children) of
        {_, Pid, _, _} when is_pid(Pid) -> Pid;
        _ -> undefined
    end.

%% A failed update keeps the old settings, so it must keep the old pool too.
restore_pool(undefined) ->
    ok;
restore_pool(OldServer) ->
    case start_pool(OldServer) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG(error, #{msg => "udf_restore_http_pool_failed", reason => Reason})
    end.

parse_config(RawConfig) ->
    Server = maps:get(<<"server">>, RawConfig, #{}),
    maybe
        {ok, URI} ?= parse_url(maps:get(<<"url">>, Server, ?DEFAULT_URL)),
        {ok, PoolSize} ?= pos_int(<<"pool_size">>, Server, 8, 1024),
        {ok, ConnectTimeout} ?= pos_int(<<"connect_timeout_ms">>, Server, 5000, 600_000),
        {ok, RequestTimeout} ?= pos_int(<<"request_timeout_ms">>, Server, 2000, 600_000),
        {ok, CACertFile} ?= parse_cacertfile(maps:get(<<"cacertfile">>, Server, <<>>)),
        {ok, #{
            uri => URI,
            %% Wrapped, so the token stays out of crash reports of this process.
            token => wrap_token(maps:get(<<"token">>, Server, <<>>)),
            cacertfile => CACertFile,
            pool_size => PoolSize,
            connect_timeout => ConnectTimeout,
            request_timeout => RequestTimeout
        }}
    end.

parse_url(URL) ->
    case emqx_http_lib:uri_parse(URL) of
        {ok, #{scheme := Scheme} = URI} when Scheme =:= http; Scheme =:= https ->
            {ok, URI};
        {ok, _} ->
            {error, #{reason => bad_url, url => URL, hint => <<"use http:// or https://">>}};
        {error, Reason} ->
            {error, #{reason => bad_url, url => URL, detail => Reason}}
    end.

%% The file is local to each node, so each node checks its own copy.
parse_cacertfile(<<>>) ->
    {ok, ?CACERTFILE_SYSTEM_DEFAULT};
parse_cacertfile(Path) when is_binary(Path) ->
    case filelib:is_regular(Path) of
        true -> {ok, Path};
        false -> {error, #{reason => bad_config, field => <<"cacertfile">>, detail => not_a_file}}
    end;
parse_cacertfile(Other) ->
    {error, #{reason => bad_config, field => <<"cacertfile">>, value => Other}}.

pos_int(Key, Map, Default, Max) ->
    case maps:get(Key, Map, Default) of
        N when is_integer(N), N >= 1, N =< Max ->
            {ok, N};
        Other ->
            {error, #{reason => bad_config, field => Key, value => Other, expected => {1, Max}}}
    end.

start_pool(#{uri := #{scheme := Scheme, host := Host, port := Port} = URI} = Server) ->
    {Transport, TransportOpts} =
        case Scheme of
            http -> {tcp, []};
            https -> {tls, tls_opts(maps:get(cacertfile, Server))}
        end,
    PoolOpts = [
        {host, Host},
        {port, Port},
        {connect_timeout, maps:get(connect_timeout, Server)},
        {keepalive, 30_000},
        {pool_type, random},
        {pool_size, maps:get(pool_size, Server)},
        {transport, Transport},
        {transport_opts, emqx_utils:ipv6_probe(TransportOpts)},
        {enable_pipelining, 1}
    ],
    case ehttpc_sup:start_pool(?POOL, PoolOpts) of
        {ok, _} ->
            Client = #{
                base_path => string:trim(bin(maps:get(path, URI, <<>>)), trailing, "/"),
                token => maps:get(token, Server),
                timeout => maps:get(request_timeout, Server)
            },
            persistent_term:put(?PT_CLIENT, Client);
        {error, Reason} ->
            {error, #{reason => http_pool_start_failed, detail => Reason}}
    end.

%% `system_default' trusts the CAs in `node.trusted_certs', or the OS CAs when
%% it is not set. With `verify_peer', `ssl' sends the URL host as SNI and checks
%% it against the server certificate.
tls_opts(CACertFile) ->
    emqx_tls_lib:to_client_opts(tls, #{
        enable => true,
        verify => verify_peer,
        cacertfile => CACertFile
    }).

wrap_token(<<>>) -> undefined;
wrap_token(Token) -> emqx_secret:wrap(Token).

%%--------------------------------------------------------------------
%% HTTP
%%--------------------------------------------------------------------

-spec request(client(), get | put | post | delete, iodata(), list(), binary() | undefined) ->
    {ok, 200, map()} | {error, map()}.
request(#{base_path := Base, token := Token, timeout := Timeout}, Method, Path, Headers0, Body) ->
    Headers = auth_header(Token) ++ [{<<"content-type">>, <<"application/json">>} | Headers0],
    FullPath = iolist_to_binary([Base, Path]),
    Req =
        case Body of
            undefined -> {FullPath, Headers};
            _ -> {FullPath, Headers, Body}
        end,
    case ehttpc:request(?POOL, Method, Req, Timeout, 0) of
        {ok, Status, _RespHeaders, RespBody} ->
            handle_response(Status, RespBody);
        {ok, Status, _RespHeaders} ->
            handle_response(Status, <<>>);
        {error, Reason} ->
            {error, #{reason => Reason}}
    end.

handle_response(Status, RespBody) ->
    case emqx_utils_json:safe_decode(RespBody) of
        {ok, #{} = Decoded} when Status =:= 200 ->
            {ok, 200, Decoded};
        {ok, #{<<"code">> := Code} = Decoded} ->
            {error, #{
                status => Status,
                code => Code,
                message => maps:get(<<"message">>, Decoded, <<>>),
                detail => maps:without([<<"code">>, <<"message">>], Decoded)
            }};
        _ ->
            {error, #{status => Status, reason => bad_response_body}}
    end.

%% A reply can be up to `max_response_bytes' long: keep errors and logs short.
%% `Term' is decoded JSON, so it encodes.
truncate(Term) ->
    Bin = emqx_utils_json:encode(Term),
    case byte_size(Bin) > ?MAX_ERROR_BODY_BYTES of
        %% `string:slice' counts characters, so the result stays valid UTF-8.
        true -> <<(string:slice(Bin, 0, ?MAX_ERROR_BODY_BYTES))/binary, "...">>;
        false -> Bin
    end.

auth_header(undefined) ->
    [];
auth_header(Token) ->
    [{<<"authorization">>, <<"Bearer ", (emqx_secret:unwrap(Token))/binary>>}].

bin(B) when is_binary(B) -> B;
bin(L) when is_list(L) -> unicode:characters_to_binary(L).
