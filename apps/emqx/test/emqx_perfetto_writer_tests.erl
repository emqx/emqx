%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
-module(emqx_perfetto_writer_tests).

-include_lib("eunit/include/eunit.hrl").

events_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Path) -> ?_test(events(Path)) end}.

setup() ->
    "emqx-perfetto-writer" ++ integer_to_list(erlang:unique_integer()) ++ ".json".

cleanup(Path) ->
    file:delete(Path).

events(Path) ->
    {ok, Writer0} = emqx_perfetto_writer:open(Path),
    Ts = maps:get(origin, Writer0),
    Publisher = list_to_pid("<0.100.0>"),
    Subscriber = list_to_pid("<0.200.0>"),
    MkEvent = fun(Pid, Category, Kind, Name, Args, Offset) ->
        #{
            pid => Pid,
            category => Category,
            kind => Kind,
            name => Name,
            args => Args,
            timestamp => Ts + Offset
        }
    end,
    MkPubEvent = fun(Category, Kind, Name, Args, Offset) ->
        MkEvent(Publisher, Category, Kind, Name, Args, Offset)
    end,
    MkSubEvent = fun(Category, Kind, Name, Args, Offset) ->
        MkEvent(Subscriber, Category, Kind, Name, Args, Offset)
    end,
    Writer = lists:foldl(fun emqx_perfetto_writer:event/2, Writer0, [
        MkPubEvent(custom, slice_begin, data, #{bytes => 1024}, 0),
        MkPubEvent(
            call,
            slice_begin,
            {emqx_channel, handle_in, 2},
            #{nested => #{valid => true, missing => null}, invalid_utf8 => <<255>>},
            1000
        ),
        MkSubEvent(custom, counter, queue, #{depth => 2}, 1500),
        MkPubEvent(gc, slice_begin, gc_minor, #{heap_size => 233}, 2000),
        MkPubEvent(gc, slice_end, gc_minor, #{heap_size => 54}, 3000),
        MkPubEvent(call, slice_end, {emqx_channel, handle_in, 2}, #{}, 4000),
        MkSubEvent(custom, instant, delivered, #{from => Publisher, qos => 1}, 4500),
        MkPubEvent(custom, counter, queue, #{depth => 0}, 5000),
        MkPubEvent(custom, slice_end, data, #{}, 6000)
    ]),
    ok = emqx_perfetto_writer:close(Writer),
    {ok, Data} = file:read_file(Path),
    Expected = <<"""
    {"traceEvents":[
      {"ph":"M","pid":1,"name":"process_name","args":{"name":"<0.100.0>"}},
      {"ph":"M","pid":1,"tid":1,"name":"thread_name","args":{"name":"custom"}},
      {"ph":"B","pid":1,"tid":1,"cat":"custom","name":"data","ts":0.0,"args":{"bytes":1024}},
      {"ph":"M","pid":1,"tid":2,"name":"thread_name","args":{"name":"call"}},
      {"ph":"B","pid":1,"tid":2,"cat":"call","name":"emqx_channel:handle_in/2","ts":1.0,
       "args":{"nested":{"valid":true,"missing":null},"invalid_utf8":"<<255>>"}},
      {"ph":"M","pid":2,"name":"process_name","args":{"name":"<0.200.0>"}},
      {"ph":"M","pid":2,"tid":3,"name":"thread_name","args":{"name":"custom"}},
      {"ph":"C","pid":2,"tid":3,"cat":"custom","name":"queue","ts":1.5,"args":{"depth":2}},
      {"ph":"M","pid":1,"tid":4,"name":"thread_name","args":{"name":"gc"}},
      {"ph":"B","pid":1,"tid":4,"cat":"gc","name":"gc_minor","ts":2.0,"args":{"heap_size":233}},
      {"ph":"E","pid":1,"tid":4,"cat":"gc","name":"gc_minor","ts":3.0,"args":{"heap_size":54}},
      {"ph":"E","pid":1,"tid":2,"cat":"call","name":"emqx_channel:handle_in/2","ts":4.0,"args":{}},
      {"ph":"i","pid":2,"tid":3,"cat":"custom","name":"delivered","ts":4.5,"s":"t",
       "args":{"from":"<0.100.0>","qos":1}},
      {"ph":"C","pid":1,"tid":1,"cat":"custom","name":"queue","ts":5.0,"args":{"depth":0}},
      {"ph":"E","pid":1,"tid":1,"cat":"custom","name":"data","ts":6.0,"args":{}}
    ]}
    """>>,
    ?assertEqual(json:decode(Expected), json:decode(Data)).
