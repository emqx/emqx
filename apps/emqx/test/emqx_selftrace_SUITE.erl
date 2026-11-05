%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
-module(emqx_selftrace_SUITE).
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

end_per_testcase(_Case, _Config) ->
    emqx_selftrace:disable(),
    supervisor:terminate_child(emqx_sys_sup, emqx_selftrace),
    ok.

t_slices(Config) ->
    Events = capture(Config, fun() ->
        ok = emqx_selftrace:enable([]),
        ok = emqx_selftrace:slice_open(outer, #{}),
        ok = emqx_selftrace:slice_open(inner, #{}),
        ok = emqx_selftrace:slice_close(),
        ok = emqx_selftrace:slice_open(work, #{}),
        ok = emqx_selftrace:slice_open(unfinished, #{}),
        ok = emqx_selftrace:slice_close_all(),
        ok = emqx_selftrace:slice_close(),
        ok = emqx_selftrace:slice_close_all(),
        emqx_selftrace:disable(),
        ok = emqx_selftrace:slice_open(hidden, #{}),
        ok = emqx_selftrace:slice_close_all()
    end),
    ?assertEqual(
        [
            {~"B", ~"outer"},
            {~"B", ~"inner"},
            {~"E", ~"inner"},
            {~"B", ~"work"},
            {~"B", ~"unfinished"},
            {~"E", ~"unfinished"},
            {~"E", ~"work"},
            {~"E", ~"outer"}
        ],
        [
            {maps:get(~"ph", E), maps:get(~"name", E)}
         || E <- Events,
            maps:get(~"cat", E, undefined) =:= ~"custom"
        ]
    ).

t_custom(Config) ->
    Events = capture(Config, fun() ->
        ok = emqx_selftrace:enable([]),
        emqx_selftrace:instant(hello, #{value => 42}),
        ok = emqx_selftrace:slice_open(work, #{}),
        ok = emqx_selftrace:slice_close(),
        ok = emqx_selftrace:slice_open(unfinished, #{}),
        emqx_selftrace:disable(),
        emqx_selftrace:instant(hidden, #{})
    end),
    ?assertMatch(
        [
            #{~"ph" := ~"M", ~"name" := ~"process_name"},
            #{~"ph" := ~"M", ~"name" := ~"thread_name"},
            #{
                ~"ph" := ~"i",
                ~"cat" := ~"custom",
                ~"name" := ~"hello",
                ~"args" := #{~"value" := 42}
            },
            #{~"ph" := ~"B", ~"cat" := ~"custom", ~"name" := ~"work"},
            #{~"ph" := ~"E", ~"cat" := ~"custom", ~"name" := ~"work"},
            #{~"ph" := ~"B", ~"cat" := ~"custom", ~"name" := ~"unfinished"},
            #{~"ph" := ~"E", ~"cat" := ~"custom", ~"name" := ~"unfinished"}
        ],
        Events
    ).

t_runtime(Config) ->
    Events = capture(Config, fun() ->
        {Tracee, Ref} = spawn_opt(
            fun() ->
                receive
                    go -> ok
                end,
                traced(),
                ?assertException(error, traced_failure, traced_exception()),
                self() ! sample,
                receive
                    sample -> ok
                end,
                erlang:garbage_collect()
            end,
            [monitor, {min_heap_size, 10000}]
        ),
        _ = emqx_selftrace:trace_function({?MODULE, traced, 0}),
        _ = emqx_selftrace:trace_function({?MODULE, traced_exception, 0}),
        ok = emqx_selftrace:trace(Tracee, [garbage_collection, send, 'receive', call]),
        Tracee ! go,
        receive
            {'DOWN', Ref, process, Tracee, Reason} -> ?assertEqual(normal, Reason)
        after 1000 ->
            error(tracee_timeout)
        end
    end),
    ?assertMatch(
        [
            #{~"ph" := ~"M", ~"name" := ~"process_name"},
            #{~"ph" := ~"M", ~"name" := ~"thread_name", ~"args" := #{~"name" := ~"message"}},
            #{~"ph" := ~"i", ~"name" := ~"receive"},
            #{~"ph" := ~"M", ~"name" := ~"thread_name", ~"args" := #{~"name" := ~"call"}},
            #{~"ph" := ~"B", ~"name" := ~"emqx_selftrace_SUITE:traced/0"},
            #{
                ~"ph" := ~"E",
                ~"name" := ~"emqx_selftrace_SUITE:traced/0",
                ~"args" := #{~"exit" := ~"return_from"}
            },
            #{~"ph" := ~"B", ~"name" := ~"emqx_selftrace_SUITE:traced_exception/0"},
            #{
                ~"ph" := ~"E",
                ~"name" := ~"emqx_selftrace_SUITE:traced_exception/0",
                ~"args" := #{~"exit" := ~"exception_from"}
            },
            #{~"ph" := ~"i", ~"name" := ~"send"},
            #{~"ph" := ~"i", ~"name" := ~"receive"},
            #{~"ph" := ~"M", ~"name" := ~"thread_name", ~"args" := #{~"name" := ~"gc"}},
            #{
                ~"ph" := ~"B",
                ~"name" := ~"gc_major",
                ~"ts" := Begin,
                ~"args" := #{~"heap_size" := Before}
            },
            #{
                ~"ph" := ~"C",
                ~"name" := ~"GC",
                ~"ts" := Begin,
                ~"args" := #{~"heap_size" := Before}
            },
            #{
                ~"ph" := ~"E",
                ~"name" := ~"gc_major",
                ~"ts" := End,
                ~"args" := #{~"heap_size" := After}
            },
            #{
                ~"ph" := ~"C",
                ~"name" := ~"GC",
                ~"ts" := End,
                ~"args" := #{~"heap_size" := After}
            }
        ],
        Events
    ).

traced() ->
    ok.

traced_exception() ->
    error(traced_failure).

t_badarg(Config) ->
    {ok, Collector} = emqx_selftrace:start(trace_path(Config)),
    ?assertError(badarg, emqx_selftrace:trace(self(), [procs])),
    ?assertError(badarg, emqx_selftrace:trace(Collector, [])),
    ?assertError(badarg, emqx_selftrace:trace_function(invalid)),
    {Dead, Ref} = spawn_monitor(fun() -> ok end),
    receive
        {'DOWN', Ref, process, Dead, _} -> ok
    end,
    ?assertEqual({error, dead}, emqx_selftrace:trace(Dead, [])),
    ?assertEqual({error, dead}, emqx_selftrace:untrace(Dead)),
    ?assert(is_process_alive(Collector)).

t_untrace(Config) ->
    {ok, Collector} = emqx_selftrace:start(trace_path(Config)),
    #{session := Session} = sys:get_state(Collector),
    ok = emqx_selftrace:trace(self(), [garbage_collection, send, 'receive', call]),
    ok = emqx_selftrace:untrace(self()),
    ?assertEqual({flags, []}, trace:info(Session, self(), flags)).

t_enable(Config) ->
    {ok, _} = emqx_selftrace:start(trace_path(Config)),
    ?assertNot(emqx_selftrace:enabled()),
    ok = emqx_selftrace:enable([garbage_collection]),
    #{session := Session} = sys:get_state(emqx_selftrace),
    {flags, Flags} = trace:info(Session, self(), flags),
    ?assert(lists:member(garbage_collection, Flags)),
    ?assert(emqx_selftrace:enabled()),
    ok = emqx_selftrace:disable(),
    ?assertNot(emqx_selftrace:enabled()).

capture(Config, Fun) ->
    Path = trace_path(Config),
    {ok, _} = emqx_selftrace:start(Path),
    Fun(),
    ok = emqx_selftrace:stop(),
    {ok, Data} = file:read_file(Path),
    maps:get(<<"traceEvents">>, json:decode(Data)).

trace_path(Config) ->
    filename:join(
        proplists:get_value(priv_dir, Config),
        "selftrace" ++ integer_to_list(erlang:unique_integer()) ++ ".json"
    ).
