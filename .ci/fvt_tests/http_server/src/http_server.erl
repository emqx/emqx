-module(http_server).

-import(minirest, [
    return/0,
    return/1
]).

-export([
    start/0,
    stop/0
]).

-behavior(minirest_api).

-export([api_spec/0]).
-export([counter/2]).

api_spec() ->
    {
        [counter_api()],
        []
    }.

counter_api() ->
    MetaData = #{
        get => #{
            description => "Get counter",
            summary => "Get counter",
            responses => #{
                200 => #{
                    content => #{
                        'application/json' =>
                            #{
                                type => object,
                                properties => #{
                                    code => #{type => integer, example => 0},
                                    data => #{type => integer, example => 0}
                                }
                            }
                    }
                }
            }
        },
        post => #{
            description => "Add counter",
            summary => "Add counter",
            'requestBody' => #{
                content => #{
                    'application/json' => #{
                        schema =>
                            #{
                                type => object,
                                properties => #{
                                    payload => #{type => string, example => <<"sample payload">>},
                                    id => #{type => integer, example => 0}
                                }
                            }
                    }
                }
            },
            responses => #{
                200 => #{
                    content => #{
                        'application/json' =>
                            #{
                                type => object,
                                properties => #{
                                    code => #{type => integer, example => 0}
                                }
                            }
                    }
                }
            }
        }
    },
    {"/counter", MetaData, counter}.

counter(get, _Params) ->
    V = ets:info(relup_test_message, size),
    {200, #{<<"content-type">> => <<"text/plain">>}, #{<<"code">> => 0, <<"data">> => V}};
counter(post, #{body := Params}) ->
    case Params of
        #{<<"payload">> := _, <<"id">> := Id} ->
            ets:insert(relup_test_message, {Id, maps:remove(<<"id">>, Params)}),
            {200, #{<<"code">> => 0}};
        _ ->
            io:format("discarded: ~p\n", [Params]),
            {200, #{<<"code">> => -1}}
    end.

start() ->
    application:ensure_all_started(minirest),
    _ = spawn(fun ets_owner/0),
    RanchOptions = #{
        max_connections => 512,
        num_acceptors => 4,
        socket_opts => [{send_timeout, 5000}, {port, 7077}, {backlog, 512}]
    },
    Minirest = #{
        base_path => "",
        modules => [?MODULE],
        dispatch => [{"/[...]", ?MODULE, []}],
        protocol => http,
        ranch_options => RanchOptions,
        middlewares => [cowboy_router, cowboy_handler]
    },
    Res = minirest:start(?MODULE, Minirest),
    minirest:update_dispatch(?MODULE),
    Res.

stop() ->
    ets:delete(relup_test_message),
    minirest:stop(?MODULE).

ets_owner() ->
    ets:new(relup_test_message, [named_table, public]),
    receive
        stop -> ok
    end.
