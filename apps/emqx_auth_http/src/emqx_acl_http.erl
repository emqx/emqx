%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_acl_http).

-include("emqx_auth_http.hrl").

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[ACL http]").

-import(emqx_auth_http_cli,
        [ request/8
        , feedvar/2
        ]).

%% ACL callbacks
-export([ register_metrics/0
        , check_acl/5
        , description/0
        ]).

-spec(register_metrics() -> ok).
register_metrics() ->
    lists:foreach(fun emqx_metrics:ensure/1, ?ACL_METRICS).

%%--------------------------------------------------------------------
%% ACL callbacks
%%--------------------------------------------------------------------

check_acl(ClientInfo, PubSub, Topic, AclResult, State) ->
    return_with(fun inc_metrics/1,
                do_check_acl(ClientInfo, PubSub, Topic, AclResult, State)).

do_check_acl(#{username := <<$$, _/binary>>}, _PubSub, _Topic, _AclResult, _Config) ->
    ok;
do_check_acl(ClientInfo, PubSub, Topic, _AclResult, #{acl_req    := AclReq,
                                                      http_opts  := HttpOpts,
                                                      retry_opts := RetryOpts,
                                                      headers    := Headers}) ->
    ClientInfo1 = ClientInfo#{access => access(PubSub), topic => Topic},
    case check_acl_request(AclReq, ClientInfo1, Headers, HttpOpts, RetryOpts) of
        {ok, 200, "ignore"} -> ok;
        {ok, 200, _Body}    -> {stop, allow};
        {ok, _Code, _Body}  -> {stop, deny};
        {error, Error}      ->
            ?LOG(error, "Request ACL url ~s, error: ~p",
                 [AclReq#http_request.url, Error]),
            ok
    end.

description() -> "ACL with HTTP API".

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

inc_metrics(ok) ->
    emqx_metrics:inc(?ACL_METRICS(ignore));
inc_metrics({stop, allow}) ->
    emqx_metrics:inc(?ACL_METRICS(allow));
inc_metrics({stop, deny}) ->
    emqx_metrics:inc(?ACL_METRICS(deny)).

return_with(Fun, Result) ->
    Fun(Result), Result.

check_acl_request(#http_request{url = Url,
                                method = Method,
                                content_type = ContentType,
                                params = Params,
                                options = Options},
                  ClientInfo, Headers, HttpOpts, RetryOpts) ->
    request(Method, ContentType, Url, feedvar(Params, ClientInfo), Headers, HttpOpts, Options, RetryOpts).

access(subscribe) -> 1;
access(publish)   -> 2.

