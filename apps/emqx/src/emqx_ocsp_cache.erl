%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%%
%% @doc EMQX OCSP cache.
%%--------------------------------------------------------------------

-module(emqx_ocsp_cache).

-include("logger.hrl").
-include_lib("public_key/include/public_key.hrl").
-include_lib("ssl/src/ssl_handshake.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_server).

-export([
    start_link/0,
    sni_fun/2,
    fetch_response/1,
    register_listener/2,
    unregister_listener/1,
    inject_sni_fun/2
]).

%% gen_server API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).

%% internal export; only for mocking in tests
-export([http_get/2]).

-define(CACHE_TAB, ?MODULE).
-define(CALL_TIMEOUT, 20_000).
-define(RETRY_TIMEOUT, 5_000).
-define(REFRESH_TIMER(LID), {refresh_timer, LID}).
-ifdef(TEST).
-define(MIN_REFRESH_INTERVAL, timer:seconds(5)).
-else.
-define(MIN_REFRESH_INTERVAL, timer:minutes(1)).
-endif.

%% Allow usage of OTP certificate record fields (camelCase).
-elvis([
    {elvis_style, atom_naming_convention, #{
        regex => "^([a-z][a-z0-9]*_?)([a-zA-Z0-9]*_?)*$",
        enclosed_atoms => ".*"
    }}
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

sni_fun(_ServerName, ListenerID) ->
    Res =
        try
            fetch_response(ListenerID)
        catch
            _:_ -> error
        end,
    case Res of
        {ok, Response} ->
            [
                {certificate_status, #certificate_status{
                    status_type = ?CERTIFICATE_STATUS_TYPE_OCSP,
                    response = Response
                }}
            ];
        error ->
            []
    end.

fetch_response(ListenerID) ->
    case do_lookup(ListenerID) of
        {ok, DERResponse} ->
            {ok, DERResponse};
        {error, invalid_listener_id} ->
            error;
        {error, not_cached} ->
            ?tp(ocsp_cache_miss, #{listener_id => ListenerID}),
            ?SLOG(debug, #{
                msg => "fetching_new_ocsp_response",
                listener_id => ListenerID
            }),
            http_fetch(ListenerID)
    end.

register_listener(ListenerID, Opts) ->
    gen_server:call(?MODULE, {register_listener, ListenerID, Opts}, ?CALL_TIMEOUT).

unregister_listener(ListenerID) ->
    gen_server:cast(?MODULE, {unregister_listener, ListenerID}).

-spec inject_sni_fun(emqx_listeners:listener_id(), map()) -> map().
inject_sni_fun(ListenerID, Conf0) ->
    SNIFun = emqx_const_v1:make_sni_fun(ListenerID),
    Conf = emqx_utils_maps:deep_merge(Conf0, #{ssl_options => #{sni_fun => SNIFun}}),
    ok = ?MODULE:register_listener(ListenerID, Conf),
    Conf.

%%--------------------------------------------------------------------
%% gen_server behaviour
%%--------------------------------------------------------------------

init(_Args) ->
    logger:set_process_metadata(#{domain => [emqx, ocsp, cache]}),
    emqx_utils_ets:new(?CACHE_TAB, [
        named_table,
        public,
        {heir, whereis(emqx_kernel_sup), none},
        {read_concurrency, true}
    ]),
    ?tp(ocsp_cache_init, #{}),
    {ok, #{}}.

handle_call({http_fetch, ListenerID}, _From, State) ->
    case do_lookup(ListenerID) of
        {ok, DERResponse} ->
            {reply, {ok, DERResponse}, State};
        {error, invalid_listener_id} ->
            {reply, error, State};
        {error, not_cached} ->
            Conf = undefined,
            with_refresh_params(ListenerID, Conf, {reply, error, State}, fun(Params) ->
                case do_http_fetch_and_cache(ListenerID, Params) of
                    error -> {reply, error, ensure_timer(ListenerID, State, ?RETRY_TIMEOUT)};
                    {ok, Response} -> {reply, {ok, Response}, ensure_timer(ListenerID, State)}
                end
            end)
    end;
handle_call({register_listener, ListenerID, Conf}, _From, State0) ->
    ?SLOG(debug, #{
        msg => "registering_ocsp_cache",
        listener_id => ListenerID
    }),
    RefreshInterval0 = emqx_utils_maps:deep_get([ssl_options, ocsp, refresh_interval], Conf),
    RefreshInterval = max(RefreshInterval0, ?MIN_REFRESH_INTERVAL),
    State = State0#{{refresh_interval, ListenerID} => RefreshInterval},
    %% we need to pass the config along because this might be called
    %% during the listener's `post_config_update', hence the config is
    %% not yet "commited" and accessible when we need it.
    Message = {refresh, ListenerID, Conf},
    {reply, ok, ensure_timer(ListenerID, Message, State, 0)};
handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast({unregister_listener, ListenerID}, State0) ->
    State2 =
        case maps:take(?REFRESH_TIMER(ListenerID), State0) of
            error ->
                State0;
            {TRef, State1} ->
                emqx_utils:cancel_timer(TRef),
                State1
        end,
    State = maps:remove({refresh_interval, ListenerID}, State2),
    ?tp(ocsp_cache_listener_unregistered, #{listener_id => ListenerID}),
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({timeout, TRef, {refresh, ListenerID}}, State0) ->
    case maps:get(?REFRESH_TIMER(ListenerID), State0, undefined) of
        TRef ->
            ?tp(ocsp_refresh_timer, #{listener_id => ListenerID}),
            ?SLOG(debug, #{
                msg => "refreshing_ocsp_response",
                listener_id => ListenerID
            }),
            Conf = undefined,
            handle_refresh(ListenerID, Conf, State0);
        _ ->
            {noreply, State0}
    end;
handle_info({timeout, TRef, {refresh, ListenerID, Conf}}, State0) ->
    case maps:get(?REFRESH_TIMER(ListenerID), State0, undefined) of
        TRef ->
            ?tp(ocsp_refresh_timer, #{listener_id => ListenerID}),
            ?SLOG(debug, #{
                msg => "refreshing_ocsp_response",
                listener_id => ListenerID
            }),
            handle_refresh(ListenerID, Conf, State0);
        _ ->
            {noreply, State0}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

code_change(_Vsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% internal functions
%%--------------------------------------------------------------------

http_fetch(ListenerID) ->
    %% TODO: configurable call timeout?
    gen_server:call(?MODULE, {http_fetch, ListenerID}, ?CALL_TIMEOUT).

with_listener_config(ListenerID, ConfPath, ErrorResp, Fn) ->
    case emqx_listeners:parse_listener_id(ListenerID) of
        {ok, #{type := Type, name := Name}} ->
            case emqx_config:get_listener_conf(Type, Name, ConfPath, not_found) of
                not_found ->
                    ?SLOG(error, #{
                        msg => "listener_config_missing",
                        listener_id => ListenerID
                    }),
                    ErrorResp;
                Config ->
                    Fn(Config)
            end;
        _Err ->
            ?SLOG(error, #{
                msg => "listener_id_not_found",
                listener_id => ListenerID
            }),
            ErrorResp
    end.

cache_key(ListenerID) ->
    with_listener_config(ListenerID, [ssl_options], error, fun
        (#{certfile := ServerCertPemPath}) ->
            #'Certificate'{
                tbsCertificate =
                    #'TBSCertificate'{
                        signature = Signature
                    }
            } = read_server_cert(ServerCertPemPath),
            {ok, {ocsp_response, Signature}};
        (OtherConfig) ->
            ?SLOG(error, #{
                msg => "listener_config_inconsistent",
                listener_id => ListenerID,
                config => OtherConfig
            }),
            error
    end).

do_lookup(ListenerID) ->
    CacheKey = cache_key(ListenerID),
    case CacheKey of
        error ->
            {error, invalid_listener_id};
        {ok, Key} ->
            %% Respond immediately if a concurrent call already fetched it.
            case ets:lookup(?CACHE_TAB, Key) of
                [{_, DERResponse}] ->
                    ?tp(ocsp_cache_hit, #{listener_id => ListenerID}),
                    {ok, DERResponse};
                [] ->
                    {error, not_cached}
            end
    end.

read_server_cert(ServerCertPemPath0) ->
    ServerCertPemPath = to_bin(ServerCertPemPath0),
    case ets:lookup(ssl_pem_cache, ServerCertPemPath) of
        [{_, [{'Certificate', ServerCertDer, _} | _]}] ->
            public_key:der_decode('Certificate', ServerCertDer);
        [] ->
            case file:read_file(ServerCertPemPath) of
                {ok, ServerCertPem} ->
                    [{'Certificate', ServerCertDer, _} | _] =
                        public_key:pem_decode(ServerCertPem),
                    public_key:der_decode('Certificate', ServerCertDer);
                {error, Error1} ->
                    error({bad_server_cert_file, Error1})
            end
    end.

handle_refresh(ListenerID, Conf, State0) ->
    %% no point in retrying if the config is inconsistent or non
    %% existent.
    State1 = maps:without([{refresh_interval, ListenerID}, ?REFRESH_TIMER(ListenerID)], State0),
    with_refresh_params(ListenerID, Conf, {noreply, State1}, fun(Params) ->
        case do_http_fetch_and_cache(ListenerID, Params) of
            error ->
                ?SLOG(debug, #{
                    msg => "failed_to_fetch_ocsp_response",
                    listener_id => ListenerID
                }),
                {noreply, ensure_timer(ListenerID, State0, ?RETRY_TIMEOUT)};
            {ok, _Response} ->
                ?SLOG(debug, #{
                    msg => "fetched_ocsp_response",
                    listener_id => ListenerID
                }),
                {noreply, ensure_timer(ListenerID, State0)}
        end
    end).

with_refresh_params(ListenerID, Conf, ErrorRet, Fn) ->
    case get_refresh_params(ListenerID, Conf) of
        error ->
            ErrorRet;
        {ok, Params} ->
            try
                Fn(Params)
            catch
                Kind:Error ->
                    ?SLOG(error, #{
                        msg => "error_fetching_ocsp_response",
                        listener_id => ListenerID,
                        error => {Kind, Error}
                    }),
                    ErrorRet
            end
    end.

get_refresh_params(ListenerID, undefined = _Conf) ->
    %% during normal periodic refreshes, we read from the emqx config.
    with_listener_config(ListenerID, [ssl_options], error, fun
        (
            #{
                ocsp := #{
                    issuer_pem := IssuerPemPath,
                    responder_url := ResponderURL,
                    refresh_http_timeout := HTTPTimeout
                },
                certfile := ServerCertPemPath
            }
        ) ->
            {ok, #{
                issuer_pem => IssuerPemPath,
                responder_url => ResponderURL,
                refresh_http_timeout => HTTPTimeout,
                server_certfile => ServerCertPemPath
            }};
        (OtherConfig) ->
            ?SLOG(error, #{
                msg => "listener_config_inconsistent",
                listener_id => ListenerID,
                config => OtherConfig
            }),
            error
    end);
get_refresh_params(_ListenerID, #{
    ssl_options := #{
        ocsp := #{
            issuer_pem := IssuerPemPath,
            responder_url := ResponderURL,
            refresh_http_timeout := HTTPTimeout
        },
        certfile := ServerCertPemPath
    }
}) ->
    {ok, #{
        issuer_pem => IssuerPemPath,
        responder_url => ResponderURL,
        refresh_http_timeout => HTTPTimeout,
        server_certfile => ServerCertPemPath
    }};
get_refresh_params(_ListenerID, _Conf) ->
    error.

do_http_fetch_and_cache(ListenerID, Params) ->
    #{
        issuer_pem := IssuerPemPath,
        responder_url := ResponderURL,
        refresh_http_timeout := HTTPTimeout,
        server_certfile := ServerCertPemPath
    } = Params,
    IssuerPem =
        case file:read_file(IssuerPemPath) of
            {ok, IssuerPem0} -> IssuerPem0;
            {error, Error0} -> error({bad_issuer_pem_file, Error0})
        end,
    ServerCert = read_server_cert(ServerCertPemPath),
    Request = build_ocsp_request(IssuerPem, ServerCert),
    ?tp(ocsp_http_fetch, #{
        listener_id => ListenerID,
        responder_url => ResponderURL,
        timeout => HTTPTimeout
    }),
    RequestURI = iolist_to_binary([ResponderURL, Request]),
    Resp = ?MODULE:http_get(RequestURI, HTTPTimeout),
    case Resp of
        {ok, {{_, 200, _}, _, Body}} ->
            ?SLOG(debug, #{
                msg => "caching_ocsp_response",
                listener_id => ListenerID
            }),
            %% if we got this far, the certfile is correct.
            {ok, CacheKey} = cache_key(ListenerID),
            true = ets:insert(?CACHE_TAB, {CacheKey, Body}),
            ?tp(ocsp_http_fetch_and_cache, #{
                listener_id => ListenerID,
                headers => true
            }),
            {ok, Body};
        {ok, {200, Body}} ->
            ?SLOG(debug, #{
                msg => "caching_ocsp_response",
                listener_id => ListenerID
            }),
            %% if we got this far, the certfile is correct.
            {ok, CacheKey} = cache_key(ListenerID),
            true = ets:insert(?CACHE_TAB, {CacheKey, Body}),
            ?tp(ocsp_http_fetch_and_cache, #{
                listener_id => ListenerID,
                headers => false
            }),
            {ok, Body};
        {ok, {{_, Code, _}, _, Body}} ->
            ?tp(
                error,
                ocsp_http_fetch_bad_code,
                #{
                    listener_id => ListenerID,
                    body => Body,
                    code => Code,
                    headers => true
                }
            ),
            ?SLOG(error, #{
                msg => "error_fetching_ocsp_response",
                listener_id => ListenerID,
                code => Code,
                body => Body
            }),
            error;
        {ok, {Code, Body}} ->
            ?tp(
                error,
                ocsp_http_fetch_bad_code,
                #{
                    listener_id => ListenerID,
                    body => Body,
                    code => Code,
                    headers => false
                }
            ),
            ?SLOG(error, #{
                msg => "error_fetching_ocsp_response",
                listener_id => ListenerID,
                code => Code,
                body => Body
            }),
            error;
        {error, Error} ->
            ?tp(
                error,
                ocsp_http_fetch_error,
                #{
                    listener_id => ListenerID,
                    error => Error
                }
            ),
            ?SLOG(error, #{
                msg => "error_fetching_ocsp_response",
                listener_id => ListenerID,
                error => Error
            }),
            error
    end.

http_get(URL, HTTPTimeout) ->
    httpc:request(
        get,
        {URL, [{"connection", "close"}]},
        [{timeout, HTTPTimeout}],
        [{body_format, binary}]
    ).

ensure_timer(ListenerID, State) ->
    Timeout = maps:get({refresh_interval, ListenerID}, State, timer:minutes(5)),
    ensure_timer(ListenerID, State, Timeout).

ensure_timer(ListenerID, State, Timeout) ->
    ensure_timer(ListenerID, {refresh, ListenerID}, State, Timeout).

ensure_timer(ListenerID, Message, State, Timeout) ->
    emqx_utils:cancel_timer(maps:get(?REFRESH_TIMER(ListenerID), State, undefined)),
    State#{
        ?REFRESH_TIMER(ListenerID) => emqx_utils:start_timer(
            Timeout,
            Message
        )
    }.

build_ocsp_request(IssuerPem, ServerCert) ->
    [{'Certificate', IssuerDer, _} | _] = public_key:pem_decode(IssuerPem),
    #'Certificate'{
        tbsCertificate =
            #'TBSCertificate'{
                serialNumber = SerialNumber,
                issuer = Issuer
            }
    } = ServerCert,
    #'Certificate'{
        tbsCertificate =
            #'TBSCertificate'{
                subjectPublicKeyInfo =
                    #'SubjectPublicKeyInfo'{subjectPublicKey = IssuerPublicKeyDer}
            }
    } = public_key:der_decode('Certificate', IssuerDer),
    IssuerDNHash = crypto:hash(sha, public_key:der_encode('Name', Issuer)),
    IssuerPKHash = crypto:hash(sha, IssuerPublicKeyDer),
    Req = #'OCSPRequest'{
        tbsRequest =
            #'TBSRequest'{
                version = 0,
                requestList =
                    [
                        #'Request'{
                            reqCert =
                                #'CertID'{
                                    hashAlgorithm =
                                        #'AlgorithmIdentifier'{
                                            algorithm = ?'id-sha1',
                                            %% ???
                                            parameters = <<5, 0>>
                                        },
                                    issuerNameHash = IssuerDNHash,
                                    issuerKeyHash = IssuerPKHash,
                                    serialNumber = SerialNumber
                                }
                        }
                    ]
            }
    },
    ReqDer = public_key:der_encode('OCSPRequest', Req),
    B64Encoded = base64:encode_to_string(ReqDer),
    uri_string:quote(B64Encoded).

to_bin(Str) when is_list(Str) -> list_to_binary(Str);
to_bin(Bin) when is_binary(Bin) -> Bin.
