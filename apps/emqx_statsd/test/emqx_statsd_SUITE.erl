-module(emqx_statsd_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-import(emqx_dashboard_api_test_helpers, [request/3, uri/1]).

-define(BASE_CONF, <<
    "\n"
    "statsd {\n"
    "enable = true\n"
    "flush_time_interval = 4s\n"
    "sample_time_interval = 4s\n"
    "server = \"127.0.0.1:8126\"\n"
    "tags {\"t1\" = \"good\", test = 100}\n"
    "}\n"
>>).

init_per_suite(Config) ->
    emqx_common_test_helpers:start_apps(
        [emqx_conf, emqx_dashboard, emqx_statsd],
        fun set_special_configs/1
    ),
    ok = emqx_common_test_helpers:load_config(emqx_statsd_schema, ?BASE_CONF, #{
        raw_with_default => true
    }),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_statsd, emqx_dashboard, emqx_conf]).

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config();
set_special_configs(_) ->
    ok.

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_statsd(_) ->
    {ok, Socket} = gen_udp:open(8126, [{active, true}]),
    receive
        {udp, Socket1, Host, Port, Data} ->
            ct:pal("receive:~p~n", [{Socket, Socket1, Host, Port}]),
            ?assert(length(Data) > 50),
            ?assert(nomatch =/= string:find(Data, "\nemqx.cpu_use:"))
    after 10 * 1000 ->
        error(timeout)
    end,
    gen_udp:close(Socket).

t_management(_) ->
    ?assertMatch(ok, emqx_statsd:start()),
    ?assertMatch(ok, emqx_statsd:start()),
    ?assertMatch(ok, emqx_statsd:stop()),
    ?assertMatch(ok, emqx_statsd:stop()),
    ?assertMatch(ok, emqx_statsd:restart()).

t_rest_http(_) ->
    {ok, Res0} = request(get),
    ?assertEqual(
        #{
            <<"enable">> => true,
            <<"flush_time_interval">> => <<"4s">>,
            <<"sample_time_interval">> => <<"4s">>,
            <<"server">> => <<"127.0.0.1:8126">>,
            <<"tags">> => #{<<"t1">> => <<"good">>, <<"test">> => 100}
        },
        Res0
    ),
    {ok, Res1} = request(put, #{enable => false}),
    ?assertMatch(#{<<"enable">> := false}, Res1),
    ?assertEqual(maps:remove(<<"enable">>, Res0), maps:remove(<<"enable">>, Res1)),
    {ok, Res2} = request(get),
    ?assertEqual(Res1, Res2),
    ?assertEqual(
        error, request(put, #{sample_time_interval => "11s", flush_time_interval => "10s"})
    ),
    {ok, _} = request(put, #{enable => true}),
    ok.

t_kill_exit(_) ->
    {ok, _} = request(put, #{enable => true}),
    Pid = erlang:whereis(emqx_statsd),
    ?assertEqual(ignore, gen_server:call(Pid, whatever)),
    ?assertEqual(ok, gen_server:cast(Pid, whatever)),
    ?assertEqual(Pid, erlang:whereis(emqx_statsd)),
    #{estatsd_pid := Estatsd} = sys:get_state(emqx_statsd),
    ?assert(erlang:exit(Estatsd, kill)),
    ?assertEqual(false, is_process_alive(Estatsd)),
    ct:sleep(150),
    Pid1 = erlang:whereis(emqx_statsd),
    ?assertNotEqual(Pid, Pid1),
    #{estatsd_pid := Estatsd1} = sys:get_state(emqx_statsd),
    ?assertNotEqual(Estatsd, Estatsd1),
    ok.

request(Method) -> request(Method, []).

request(Method, Body) ->
    case request(Method, uri(["statsd"]), Body) of
        {ok, 200, Res} ->
            {ok, emqx_json:decode(Res, [return_maps])};
        {ok, _Status, _} ->
            error
    end.
