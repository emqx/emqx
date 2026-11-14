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

-define(POOL, emqx_rule_udf_http).
-define(PT_CLIENT, {?MODULE, client}).
-define(DEFAULT_URL, <<"http://127.0.0.1:8080">>).

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
            {error, #{reason => bad_response, body => Other}};
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
    _ = persistent_term:erase(?PT_CLIENT),
    _ = ehttpc_sup:stop_pool(?POOL),
    ok.

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
    case parse_config(RawConfig) of
        {ok, OldServer} ->
            {ok, State};
        {ok, NewServer} ->
            _ = persistent_term:erase(?PT_CLIENT),
            _ = ehttpc_sup:stop_pool(?POOL),
            case start_pool(NewServer) of
                ok ->
                    {ok, State#{server => NewServer}};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

parse_config(RawConfig) ->
    Server = maps:get(<<"server">>, RawConfig, #{}),
    URL = maps:get(<<"url">>, Server, ?DEFAULT_URL),
    case emqx_http_lib:uri_parse(URL) of
        {ok, #{scheme := Scheme} = URI} when Scheme =:= http; Scheme =:= https ->
            {ok, #{
                uri => URI,
                token => maps:get(<<"token">>, Server, <<>>),
                pool_size => maps:get(<<"pool_size">>, Server, 8),
                connect_timeout => maps:get(<<"connect_timeout_ms">>, Server, 5000),
                request_timeout => maps:get(<<"request_timeout_ms">>, Server, 2000)
            }};
        {ok, _} ->
            {error, #{reason => bad_url, url => URL, hint => <<"use http:// or https://">>}};
        {error, Reason} ->
            {error, #{reason => bad_url, url => URL, detail => Reason}}
    end.

start_pool(#{uri := #{scheme := Scheme, host := Host, port := Port} = URI} = Server) ->
    {Transport, TransportOpts} =
        case Scheme of
            http -> {tcp, []};
            https -> {tls, tls_opts(Host)}
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
                token => wrap_token(maps:get(token, Server)),
                timeout => maps:get(request_timeout, Server)
            },
            persistent_term:put(?PT_CLIENT, Client);
        {error, Reason} ->
            {error, #{reason => http_pool_start_failed, detail => Reason}}
    end.

tls_opts(Host) ->
    [
        {verify, verify_peer},
        {cacerts, public_key:cacerts_get()},
        {server_name_indication, sni(Host)},
        {customize_hostname_check, [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]}
    ].

sni(Host) when is_tuple(Host) -> disable;
sni(Host) -> str(Host).

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

auth_header(undefined) ->
    [];
auth_header(Token) ->
    [{<<"authorization">>, <<"Bearer ", (emqx_secret:unwrap(Token))/binary>>}].

bin(B) when is_binary(B) -> B;
bin(L) when is_list(L) -> unicode:characters_to_binary(L).

str(B) when is_binary(B) -> binary_to_list(B);
str(L) when is_list(L) -> L.
