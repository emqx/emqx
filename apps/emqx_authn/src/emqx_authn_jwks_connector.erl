%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_jwks_connector).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include_lib("jose/include/jose_jwk.hrl").

-export([ start_link/1
        , stop/1
        ]).

-export([ get_jwks/1
        , update/2
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

stop(Pid) ->
    gen_server:stop(Pid).

get_jwks(Pid) ->
    gen_server:call(Pid, get_cached_jwks, 5000).

update(Pid, Opts) ->
    gen_server:call(Pid, {update, Opts}, 5000).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Opts]) ->
    ok = jose:json_module(jiffy),
    State = handle_options(Opts),
    {ok, refresh_jwks(State)}.

handle_call(get_cached_jwks, _From, #{jwks := Jwks} = State) ->
    {reply, {ok, Jwks}, State};

handle_call({update, Opts}, _From, State) ->
    State = handle_options(Opts),
    {reply, ok, refresh_jwks(State)};

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({refresh_jwks, _TRef, refresh}, #{request_id := RequestID} = State) ->
    case RequestID of
        undefined -> ok;
        _ ->
            ok = httpc:cancel_request(RequestID),
            receive
				{http, _} -> ok
			after 0 ->
				ok
			end
    end,
    {noreply, refresh_jwks(State)};

handle_info({http, {RequestID, Result}},
            #{request_id := RequestID, endpoint := Endpoint} = State0) ->
    State1 = State0#{request_id := undefined},
    case Result of
        {error, Reason} ->
            ?LOG(error, "Failed to request jwks endpoint(~s): ~p", [Endpoint, Reason]),
            State1;
        {_StatusLine, _Headers, Body} ->
            try
                JWKS = jose_jwk:from(emqx_json:decode(Body, [return_maps])),
                {_, JWKs} = JWKS#jose_jwk.keys,
                State1#{jwks := JWKs}
            catch _:_ ->
                ?LOG(error, "Invalid jwks returned from jwks endpoint(~s): ~p~n", [Endpoint, Body]),
                State1
            end
    end;

handle_info({http, {_, _}}, State) ->
    %% ignore
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    _ = cancel_timer(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_options(#{endpoint := Endpoint,
                 refresh_interval := RefreshInterval0,
                 ssl_opts := SSLOpts}) ->
    #{endpoint => Endpoint,
      refresh_interval => limit_refresh_interval(RefreshInterval0),
      ssl_opts => maps:to_list(SSLOpts),
      jwks => [],
      request_id => undefined};

handle_options(#{enable_ssl := false} = Opts) ->
    handle_options(Opts#{ssl_opts => []}).

refresh_jwks(#{endpoint := Endpoint,
               ssl_opts := SSLOpts} = State) ->
    HTTPOpts = [{timeout, 5000}, {connect_timeout, 5000}, {ssl, SSLOpts}],
    NState = case httpc:request(get, {Endpoint, [{"Accept", "application/json"}]}, HTTPOpts,
                                [{body_format, binary}, {sync, false}, {receiver, self()}]) of
                 {error, Reason} ->
                     ?LOG(error, "Failed to request jwks endpoint(~s): ~p", [Endpoint, Reason]),
                     State;
                 {ok, RequestID} ->
                     State#{request_id := RequestID}
             end,
    ensure_expiry_timer(NState).

ensure_expiry_timer(State = #{refresh_interval := Interval}) ->
    State#{refresh_timer := emqx_misc:start_timer(timer:seconds(Interval), refresh_jwks)}.

cancel_timer(State = #{refresh_timer := undefined}) ->
    State;
cancel_timer(State = #{refresh_timer := TRef}) ->
    _ = emqx_misc:cancel_timer(TRef),
    State#{refresh_timer := undefined}.

limit_refresh_interval(Interval) when Interval < 10 ->
    10;
limit_refresh_interval(Interval) ->
    Interval.
