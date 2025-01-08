%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mt_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(NEW_CLIENTID(I),
    iolist_to_binary("c-" ++ atom_to_list(?FUNCTION_NAME) ++ "-" ++ integer_to_list(I))
).

-define(NEW_USERNAME(), iolist_to_binary("u-" ++ atom_to_list(?FUNCTION_NAME))).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, "mqtt.client_attrs_init = [{expression = username, set_as_attr = tns}]"},
            {emqx_mt, "multi_tenancy.default_max_sessions = 10"},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(Case, Config) ->
    snabbkaffe:start_trace(),
    ?MODULE:Case({init, Config}),
    Config.

end_per_testcase(Case, Config) ->
    snabbkaffe:stop(),
    ?MODULE:Case({'end', Config}),
    ok.

connect(ClientId, Username) ->
    Opts = [
        {clientid, ClientId},
        {username, Username},
        {password, "123456"},
        {proto_ver, v5}
    ],
    {ok, Pid} = emqtt:start_link(Opts),
    monitor(process, Pid),
    unlink(Pid),
    case emqtt:connect(Pid) of
        {ok, _} ->
            Pid;
        {error, _Reason} = E ->
            stop_client(Pid),
            erlang:error(E)
    end.

stop_client(Pid) ->
    catch emqtt:stop(Pid),
    receive
        {'DOWN', _, process, Pid, _, _} -> ok
    after 3000 ->
        exit(Pid, kill)
    end.

t_list_apis({init, _Config}) ->
    ok;
t_list_apis({'end', _Config}) ->
    ok;
t_list_apis(_Config) ->
    N = 9,
    ClientIds = [?NEW_CLIENTID(I) || I <- lists:seq(1, N)],
    Ns = ?NEW_USERNAME(),
    Clients = [connect(ClientId, Ns) || ClientId <- ClientIds],
    ?retry(200, 50, ?assertEqual({ok, N}, emqx_mt:count_clients(Ns))),
    ?assertMatch({ok, {_, #{<<"count">> := N}}}, request(get, ns_url(Ns, "client_count"), #{})),
    {ok, {_, ClientIds0}} =
        request(get, ns_url(Ns, "client_list"), #{<<"limit">> => integer_to_binary(N div 2)}),
    LastClientId = lists:last(ClientIds0),
    {ok, {_, ClientIds1}} =
        request(get, ns_url(Ns, "client_list"), #{
            <<"last_clientid">> => LastClientId,
            <<"limit">> => integer_to_binary(N)
        }),
    ?assertEqual(ClientIds, ClientIds0 ++ ClientIds1),
    ok = lists:foreach(fun stop_client/1, Clients),
    ?retry(
        200,
        50,
        ?assertMatch(
            {ok, {{_, 200, _}, #{<<"count">> := 0}}},
            request(get, ns_url(Ns, "client_count"), #{})
        )
    ),
    ?assertMatch(
        {ok, {{_, 200, _}, []}},
        request(get, ns_url(Ns, "client_list"), #{})
    ),
    ?assertMatch(
        {ok, {{_, 200, _}, [Ns]}},
        request(get, url("ns_list"), #{})
    ),
    ?assertMatch(
        {ok, {{_, 200, _}, [Ns]}},
        request(get, url("ns_list"), #{<<"limit">> => <<"2">>})
    ),
    ?assertMatch(
        {ok, {{_, 200, _}, []}},
        request(get, url("ns_list"), #{<<"last_ns">> => Ns, <<"limit">> => <<"1">>})
    ),
    ok.

url(Path) ->
    emqx_mgmt_api_test_util:api_path(["mt", Path]).

ns_url(Ns, Path) ->
    emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns, Path]).

request(Method, Url, Params) ->
    Headers = [],
    Body = "",
    Opts = #{return_all => true},
    case emqx_mgmt_api_test_util:request_api(Method, Url, Params, Headers, Body, Opts) of
        {ok, {Status, _Headers, Body0}} ->
            RspBody = maybe_json_decode(Body0),
            {ok, {Status, RspBody}};
        {error, {Status, _Headers, Body0}} ->
            RspBody =
                case emqx_utils_json:safe_decode(Body0, [return_maps]) of
                    {ok, Decoded0 = #{<<"message">> := Msg0}} ->
                        Msg = maybe_json_decode(Msg0),
                        Decoded0#{<<"message">> := Msg};
                    {ok, Decoded0} ->
                        Decoded0;
                    {error, _} ->
                        Body0
                end,
            {error, {Status, RspBody}};
        Error ->
            error(Error)
    end.

maybe_json_decode(X) ->
    case emqx_utils_json:safe_decode(X, [return_maps]) of
        {ok, Decoded} -> Decoded;
        {error, _} -> X
    end.
