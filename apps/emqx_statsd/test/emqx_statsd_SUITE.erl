-module(emqx_statsd_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) ->
    emqx_common_test_helpers:start_apps([emqx_statsd]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_statsd]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_statsd(_) ->
    {ok, Socket} = gen_udp:open(8125),
    receive
        {udp, _Socket, _Host, _Port, Bin} ->
            ?assert(length(Bin) > 50)
    after 11 * 1000 ->
        ?assert(true, failed)
    end,
    gen_udp:close(Socket).

t_management(_) ->
    ?assertMatch(ok, emqx_statsd:start()),
    ?assertMatch(ok, emqx_statsd:stop()),
    ?assertMatch(ok, emqx_statsd:restart()).
