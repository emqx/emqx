-module(http_server).

-import(minirest, [
    return/0,
    return/1
]).

-export([
    start/0,
    stop/0
]).

-rest_api(#{
    name => get_counter,
    method => 'GET',
    path => "/counter",
    func => get_counter,
    descr => "Check counter"
}).
-rest_api(#{
    name => add_counter,
    method => 'POST',
    path => "/counter",
    func => add_counter,
    descr => "Counter plus one"
}).

-export([
    get_counter/2,
    add_counter/2
]).

start() ->
    application:ensure_all_started(minirest),
    _ = spawn(fun ets_owner/0),
    Handlers = [{"/", minirest:handler(#{modules => [?MODULE]})}],
    Dispatch = [{"/[...]", minirest, Handlers}],
    minirest:start_http(?MODULE, #{socket_opts => [inet, {port, 7077}]}, Dispatch).

stop() ->
    ets:delete(relup_test_message),
    minirest:stop_http(?MODULE).

get_counter(_Binding, _Params) ->
    V = ets:info(relup_test_message, size),
    return({ok, V}).

add_counter(_Binding, Params) ->
    case lists:keymember(<<"payload">>, 1, Params) of
        true ->
            {value, {<<"id">>, ID}, Params1} = lists:keytake(<<"id">>, 1, Params),
            ets:insert(relup_test_message, {ID, Params1});
        _ ->
            io:format("discarded: ~p\n", [Params]),
            ok
    end,
    return().

ets_owner() ->
    ets:new(relup_test_message, [named_table, public]),
    receive
        stop -> ok
    end.
