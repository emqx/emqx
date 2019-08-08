%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mocker).

-record(fakeq,
        { max_len = 1000 :: non_neg_integer()
        , q = [] :: list()
        , len = 0 :: non_neg_integer()
        , dropped = 0 :: non_neg_integer()
        }).

-export([ load/1
        , unload/1
        ]).

load(Modules) ->
    [mock(Module) || Module <- Modules],
    ok.

unload(Modules) ->
    lists:foreach(fun(Module) ->
                          ok = meck:unload(Module)
                  end, Modules),
    ok.

mock(Module) ->
    ok = meck:new(Module, [passthrough, no_history]),
    do_mock(Module, expect(Module)).

do_mock(emqx_metrics, Expect) ->
    Expect(inc, fun(_Anything) -> ok end);
do_mock(emqx_mqueue, Expect) ->
    Expect(init, fun new/1),
    Expect(len, fun len/1),
    Expect(max_len, fun max_len/1),
    Expect(dropped, fun dropped/1),
    Expect(is_empty, fun is_empty/1),
    Expect(out, fun out/1),
    Expect(in, fun in/2);
do_mock(emqx_inflight, Expect) ->
    Expect(new, fun new/1),
    Expect(size, fun len/1),
    Expect(max_size, fun max_len/1),
    Expect(insert, fun insert/3),
    Expect(delete, fun delete/2),
    Expect(lookup, fun lookup/2),
    Expect(update, fun update/3),
    Expect(is_empty, fun is_empty/1),
    Expect(is_full, fun is_full/1),
    Expect(to_list, fun to_list/1),
    Expect(contain, fun contain/2);
do_mock(emqx_broker, Expect) ->
    Expect(subscribe, fun(_, _, _) -> ok end),
    Expect(set_subopts, fun(_, _) -> ok end),
    Expect(unsubscribe, fun(_) -> ok end),
    Expect(publish, fun(_) -> ok end);
do_mock(emqx_misc, Expect) ->
    Expect(start_timer, fun(_, _) -> tref end);
do_mock(emqx_message, Expect) ->
    Expect(set_header, fun(_Hdr, _Val, Msg) -> Msg end),
    Expect(is_expired, fun(_Msg) -> (rand:uniform(16) > 8) end);
do_mock(emqx_hooks, Expect) ->
    Expect(run, fun(_Hook, _Args) -> ok end);
do_mock(emqx_zone, Expect) ->
    Expect(get_env, fun(Env, Key, Default) -> maps:get(Key, Env, Default) end);
do_mock(emqx_pd, Expect) ->
    Expect(update_counter, fun(_stats, _num) -> ok end).

expect(Module) ->
    fun(OldFun, NewFun) ->
            ok = meck:expect(Module, OldFun, NewFun)
    end.

new(MaxLen) when is_integer(MaxLen) ->
    #fakeq{max_len = MaxLen};
new(_) ->
    #fakeq{}.

in(Item, MQ = #fakeq{q = Q, len = Len, max_len = MaxLen})
    when Len < MaxLen ->
    {undefined, MQ#fakeq{q = Q ++ [Item], len = Len + 1}};
in(Item, MQ = #fakeq{q = [Head | Tail], dropped = Dropped}) ->
    {Head, MQ#fakeq{q = Tail ++ [Item], dropped = Dropped + 1}}.

out(MQ = #fakeq{q = [Head | Tail], len = Len}) ->
    {{value, Head}, MQ#fakeq{q = Tail, len = Len - 1}};
out(MQ = #fakeq{q = []}) ->
    {empty, MQ}.

len(#fakeq{len = Length}) ->
    Length.

max_len(#fakeq{max_len = MaxLen}) ->
    MaxLen.

dropped(#fakeq{dropped = Dropped}) ->
    Dropped.

is_empty(#fakeq{q = []}) ->
    true;
is_empty(_FakeQ) ->
    false.

insert(K, V, MQ = #fakeq{q = Q}) ->
    MQ#fakeq{q = [{K, V} |Q]}.

delete(K, MQ = #fakeq{q = Q, len = Len}) ->
    MQ#fakeq{q = lists:keydelete(K, 1, Q), len = Len - 1}.

update(K, Value, MQ = #fakeq{q = Q}) ->
    MQ#fakeq{q = lists:keyreplace(K, 1, Q, {K, Value})}.

is_full(#fakeq{len = Len, max_len = MaxLen})
  when Len =:= MaxLen ->
    true;
is_full(_MQ) ->
    false.

to_list(#fakeq{q = Q}) ->
    Q.

lookup(K, #fakeq{q = Q}) ->
    case lists:keyfind(K, 1, Q) of
        {K, V} -> {value, V};
        false -> none
    end.

contain(K, Q) ->
    case lookup(K, Q) of
        {value, _V} ->
            true;
        none ->
            false
    end.
