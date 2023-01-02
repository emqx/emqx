%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("public_key/include/public_key.hrl").
-include_lib("ssl/src/ssl_handshake.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(gen_server).

-export([ start_link/0
        , sni_fun/2
        , fetch_response/1
        , register_listener/1
        , inject_sni_fun/2
        ]).

%% gen_server API
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%% internal export; only for mocking in tests
-export([http_get/2]).

-define(LOG(Level, Format, Args),
        logger:log(Level, "[~p] " ++ Format, [?MODULE | Args])).
-define(CACHE_TAB, ?MODULE).
-define(CALL_TIMEOUT, 20_000).
-define(RETRY_TIMEOUT, 5_000).
-define(REFRESH_TIMER(LID), {refresh_timer, LID}).
-ifdef(TEST).
-define(MIN_REFRESH_INTERVAL, timer:seconds(5)).
-else.
-define(MIN_REFRESH_INTERVAL, timer:minutes(1)).
-endif.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

sni_fun(_ServerName, ListenerID) ->
    Res = try
              fetch_response(ListenerID)
          catch
              _:_ -> error
          end,
    case Res of
        {ok, Response} ->
            [{certificate_status, #certificate_status{
                                     status_type = ?CERTIFICATE_STATUS_TYPE_OCSP,
                                     response = Response
                                    }}];
        error ->
            []
    end.

fetch_response(ListenerID) ->
    case ets:lookup(?CACHE_TAB, cache_key(ListenerID)) of
        [{_, DERResponse}] ->
            ?tp(ocsp_cache_hit, #{listener_id => ListenerID}),
            ?LOG(debug, "using cached ocsp response for ~p", [ListenerID]),
            {ok, DERResponse};
        [] ->
            ?tp(ocsp_cache_miss, #{listener_id => ListenerID}),
            ?LOG(debug, "fetching new ocsp response for ~p", [ListenerID]),
            http_fetch(ListenerID)
    end.

register_listener(ListenerID) ->
    gen_server:call(?MODULE, {register_listener, ListenerID}, ?CALL_TIMEOUT).

-spec inject_sni_fun(emqx_listeners:listener_id(), [esockd:option()]) -> [esockd:option()].
inject_sni_fun(ListenerID, Options0) ->
    %% We need to patch `sni_fun' here and not in `emqx.schema'
    %% because otherwise an anonymous function will end up in
    %% `app.*.config'...
    OCSPOpts = proplists:get_value(ocsp_options, Options0, []),
    case proplists:get_bool(ocsp_stapling_enabled, OCSPOpts) of
        false ->
            Options0;
        true ->
            SSLOpts0 = proplists:get_value(ssl_options, Options0, []),
            SNIFun = emqx_const_v1:make_sni_fun(ListenerID),
            Options1 = proplists:delete(ssl_options, Options0),
            Options = [{ssl_options, [{sni_fun, SNIFun} | SSLOpts0]} | Options1],
            %% save to env
            {[ThisListener0], Listeners} =
                lists:partition(
                  fun(L) ->
                    emqx_listeners:identifier(L) =:= ListenerID
                  end,
                  emqx:get_env(listeners)),
            ThisListener = ThisListener0#{opts => Options},
            application:set_env(emqx, listeners, [ThisListener | Listeners]),
            ok = emqx_ocsp_cache:register_listener(ListenerID),
            Options
    end.

%%--------------------------------------------------------------------
%% gen_server behaviour
%%--------------------------------------------------------------------

init(_Args) ->
    logger:set_process_metadata(#{domain => [emqx, ocsp, cache]}),
    _ = ets:new(?CACHE_TAB, [ named_table
                            , protected
                            , {read_concurrency, true}
                            ]),
    ?tp(ocsp_cache_init, #{}),
    {ok, #{}}.

handle_call({http_fetch, ListenerID}, _From, State) ->
    %% Respond immediately if a concurrent call already fetched it.
    case ets:lookup(?CACHE_TAB, cache_key(ListenerID)) of
        [{_, Response}] ->
            {reply, {ok, Response}, State};
        [] ->
            case do_http_fetch_and_cache(ListenerID) of
                error -> {reply, error, ensure_timer(ListenerID, State, ?RETRY_TIMEOUT)};
                {ok, Response} -> {reply, {ok, Response}, ensure_timer(ListenerID, State)}
            end
    end;
handle_call({register_listener, ListenerID}, _From, State0) ->
    ?LOG(debug, "registering ocsp cache for ~p", [ListenerID]),
    #{opts := Opts} = emqx_listeners:find_by_id(ListenerID),
    OCSPOpts = proplists:get_value(ocsp_options, Opts),
    RefreshInterval0 = proplists:get_value(ocsp_refresh_interval, OCSPOpts),
    RefreshInterval = max(RefreshInterval0, ?MIN_REFRESH_INTERVAL),
    State = State0#{{refresh_interval, ListenerID} => RefreshInterval},
    {reply, ok, ensure_timer(ListenerID, State, 0)};
handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({timeout, TRef, {refresh, ListenerID}}, State0) ->
    case maps:get(?REFRESH_TIMER(ListenerID), State0, undefined) of
        TRef ->
            ?tp(ocsp_refresh_timer, #{listener_id => ListenerID}),
            ?LOG(debug, "refreshing ocsp response for ~p", [ListenerID]),
            case do_http_fetch_and_cache(ListenerID) of
                error ->
                    ?LOG(debug, "failed to fetch ocsp response for ~p", [ListenerID]),
                    {noreply, ensure_timer(ListenerID, State0, ?RETRY_TIMEOUT)};
                {ok, _Response} ->
                    ?LOG(debug, "fetched ocsp response for ~p", [ListenerID]),
                    {noreply, ensure_timer(ListenerID, State0)}
            end;
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

cache_key(ListenerID) ->
    #{opts := Options} = emqx_listeners:find_by_id(ListenerID),
    SSLOpts = proplists:get_value(ssl_options, Options, undefined),
    ServerCertPemPath = proplists:get_value(certfile, SSLOpts, undefined),
    #'Certificate'{
       tbsCertificate =
           #'TBSCertificate'{
              signature = Signature
             }} = read_server_cert(ServerCertPemPath),
    {ocsp_response, Signature}.

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
                {error, Error1} -> error({bad_server_cert_file, Error1})
            end
    end.

do_http_fetch_and_cache(ListenerID) ->
    #{opts := Options} = emqx_listeners:find_by_id(ListenerID),
    OCSPOpts = proplists:get_value(ocsp_options, Options),
    ResponderURL0 = proplists:get_value(ocsp_responder_url, OCSPOpts, undefined),
    ResponderURL = uri_string:normalize(ResponderURL0),
    IssuerPemPath = proplists:get_value(ocsp_issuer_pem, OCSPOpts, undefined),
    SSLOpts = proplists:get_value(ssl_options, Options, undefined),
    ServerCertPemPath = proplists:get_value(certfile, SSLOpts, undefined),
    IssuerPem = case file:read_file(IssuerPemPath) of
                    {ok, IssuerPem0} -> IssuerPem0;
                    {error, Error0} -> error({bad_issuer_pem_file, Error0})
                end,
    ServerCert = read_server_cert(ServerCertPemPath),
    Request = build_ocsp_request(IssuerPem, ServerCert),
    HTTPTimeout = proplists:get_value(ocsp_refresh_http_timeout, OCSPOpts),
    ?tp(ocsp_http_fetch, #{ listener_id => ListenerID
                          , responder_url => ResponderURL
                          , timeout => HTTPTimeout
                          }),
    Resp = ?MODULE:http_get(ResponderURL ++ Request, HTTPTimeout),
    case Resp of
        {ok, {{_, 200, _}, _, Body}} ->
            ?LOG(debug, "caching ocsp response for ~p", [ListenerID]),
            true = ets:insert(?CACHE_TAB, {cache_key(ListenerID), Body}),
            ?tp(ocsp_http_fetch_and_cache, #{listener_id => ListenerID}),
            {ok, Body};
        {ok, {{_, Code, _}, _, Body}} ->
            ?tp(error, ocsp_http_fetch_bad_code,
                #{ listener_id => ListenerID
                 , body => Body
                 , code => Code
                 }),
            ?LOG(error, "error fetching ocsp response for ~p: code ~b, body: ~p", [ListenerID, Code, Body]),
            error;
        {error, Error} ->
            ?tp(error, ocsp_http_fetch_error,
                #{ listener_id => ListenerID
                 , error => Error
                 }),
            ?LOG(error, "error fetching ocsp response for ~p: ~p", [ListenerID, Error]),
            error
    end.

http_get(URL, HTTPTimeout) ->
    httpc:request(
      get,
      {URL,
       [{"connection", "close"}]},
      [{timeout, HTTPTimeout}],
      [{body_format, binary}]
     ).

ensure_timer(ListenerID, State) ->
    Timeout = maps:get({refresh_interval, ListenerID}, State, timer:minutes(5)),
    ensure_timer(ListenerID, State, Timeout).

ensure_timer(ListenerID, State, Timeout) ->
    emqx_misc:cancel_timer(maps:get(?REFRESH_TIMER(ListenerID), State, undefined)),
    State#{?REFRESH_TIMER(ListenerID) => emqx_misc:start_timer(
                                           Timeout,
                                           {refresh, ListenerID})}.

build_ocsp_request(IssuerPem, ServerCert) ->
    [{'Certificate', IssuerDer, _} | _] = public_key:pem_decode(IssuerPem),
    #'Certificate'{
       tbsCertificate =
           #'TBSCertificate'{
              serialNumber = SerialNumber,
              issuer = Issuer
             }} = ServerCert,
    #'Certificate'{
       tbsCertificate =
           #'TBSCertificate'{
              subjectPublicKeyInfo =
                  #'SubjectPublicKeyInfo'{subjectPublicKey = IssuerPublicKeyDer}
             }} = public_key:der_decode('Certificate', IssuerDer),
    IssuerDNHash = crypto:hash(sha, public_key:der_encode('Name', Issuer)),
    IssuerPKHash = crypto:hash(sha, IssuerPublicKeyDer),
    Req = #'OCSPRequest'{
             tbsRequest =
                 #'TBSRequest'{
                    version = 0,
                    requestList =
                        [#'Request'{
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
                           }]
                   }
            },
    ReqDer = public_key:der_encode('OCSPRequest', Req),
    base64:encode_to_string(ReqDer).

to_bin(Str) when is_list(Str) -> list_to_binary(Str);
to_bin(Bin) when is_binary(Bin) -> Bin.
