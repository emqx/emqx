%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Accept requests like TDengine Cloud REST API and forward them
%% to local TDengine Deployment.
-module(emqx_tdengine_cloud_svr).

-export([
    start_link/3,
    stop/0,
    init/2,
    handle/4
]).

start_link(Port, ExpectedToken, TDOpts) when is_integer(Port) andalso is_binary(ExpectedToken) ->
    application:ensure_started(ranch),
    application:ensure_started(cowboy),
    ok = validate_td_opts(TDOpts),
    Opts = #{expected_token => ExpectedToken, td_opts => TDOpts},
    Dispatch = cowboy_router:compile([
        {'_', [
            {"/rest/sql", ?MODULE, [Opts]},
            {"/rest/sql/:db_name", ?MODULE, [Opts]}
        ]}
    ]),
    TransOpts = [{port, Port}],
    ProtoOpts = #{env => #{dispatch => Dispatch}},
    {ok, _} = cowboy:start_clear(http, TransOpts, ProtoOpts).

validate_td_opts(TDOpts) ->
    MustHave = [host, port, username, password],
    lists:foreach(
        fun(Key) ->
            case lists:keyfind(Key, 1, TDOpts) of
                false -> throw(#{key => Key, reason => "Missing required option"});
                _ -> ok
            end
        end,
        MustHave
    ).

stop() ->
    cowboy:stop_listener(http).

init(Req, Opts) ->
    Path = cowboy_req:path(Req),
    Method = cowboy_req:method(Req),
    handle(Method, Path, Req, Opts),
    {ok, Req, Opts}.

handle(Method, Path, Req, [#{expected_token := ExpectedToken, td_opts := TDOpts}]) ->
    Bindings = cowboy_req:bindings(Req),
    QueryString = cow_qs:parse_qs(cowboy_req:qs(Req)),
    ct:pal(
        "TDengine Cloud Mock Server received request, method: ~p, path: ~p, bindings: ~p, query_string: ~p",
        [Method, Path, Bindings, QueryString]
    ),
    %% assert method
    <<"POST">> = Method,
    %% assert token
    ExpectedToken = proplists:get_value(<<"token">>, QueryString),
    %% forward request to tdengine
    DbName = maps:get(db_name, Bindings, <<>>),
    {ok, Body, Req1} = cowboy_req:read_body(Req),
    Ret = forward_request_to_tdengine(Body, DbName, TDOpts),
    ct:pal("TDengine Cloud Mock Server forward request to tdengine, ret: ~0p", [Ret]),
    case Ret of
        {ok, RespMap} when is_map(RespMap) ->
            reply(200, #{<<"content-type">> => <<"application/json">>}, jsx:encode(RespMap), Req1);
        {error, RespMap} when is_map(RespMap) ->
            reply(200, #{<<"content-type">> => <<"application/json">>}, jsx:encode(RespMap), Req1);
        {error, Error} ->
            ErrorMsg = iolist_to_binary(
                io_lib:format("Forward request to TDengine failed, call response: ~0p~n", [Error])
            ),
            reply(500, #{<<"content-type">> => <<"text/plain">>}, ErrorMsg, Req1)
    end.

reply(Code, Headers, Body, Req) ->
    cowboy_req:reply(Code, Headers, Body, Req).

forward_request_to_tdengine(Sql, DbName, TDOpts) ->
    ct:pal("TDengine Cloud Mock Server forward request, sql: ~p, db_name: ~p, opts: ~0p", [
        Sql, DbName, TDOpts
    ]),
    {ok, Pid} = tdengine:start_link(TDOpts),
    tdengine:insert(Pid, Sql, [{db_name, DbName}]).
