%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_buffer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

-doc """
Checks that dropping a handler without delivering messages keeps the message tree as is,
and that dropping a handler with buffered messages removes only its messages.
""".
t_drop_handler(_Config) ->
    %% Handler `a' delivers its only message, handler `b' keeps 20 messages in the buffer.
    Buffer0 = emqx_extsub_buffer:add_new(emqx_extsub_buffer:new(), a, [msg(a, 0)]),
    {[{a, SeqId, _}], Buffer1} = emqx_extsub_buffer:take(Buffer0, 1),
    Buffer2 = emqx_extsub_buffer:set_delivered(Buffer1, a, SeqId),
    Buffer3 = emqx_extsub_buffer:add_new(Buffer2, b, [msg(b, N) || N <- lists:seq(1, 20)]),
    ?assertEqual(0, emqx_extsub_buffer:delivering_count(Buffer3, a)),

    %% Dropping `a' changes only its delivering count.
    Buffer4 = emqx_extsub_buffer:drop_handler(Buffer3, a),
    ?assertEqual(
        maps:remove(delivering, emqx_extsub_buffer:inspect(Buffer3)),
        maps:remove(delivering, emqx_extsub_buffer:inspect(Buffer4))
    ),
    %% The message tree is the same term, so it was not rebuilt.
    ?assertEqual(element(3, Buffer3), element(3, Buffer4)),
    ?assertEqual(#{b => 20}, delivering(Buffer4)),

    %% Dropping `b' removes its messages.
    Buffer5 = emqx_extsub_buffer:drop_handler(Buffer4, b),
    ?assertEqual(0, emqx_extsub_buffer:size(Buffer5)),
    ?assertEqual(#{}, delivering(Buffer5)),

    %% Dropping a handler with buffered messages keeps the messages of other handlers.
    Buffer6 = emqx_extsub_buffer:add_new(Buffer3, c, [msg(c, 21)]),
    Buffer7 = emqx_extsub_buffer:drop_handler(Buffer6, b),
    ?assertMatch({[{c, _, _}], _}, emqx_extsub_buffer:take(Buffer7, infinity)).

msg(Handler, N) ->
    emqx_message:make(Handler, <<"t/", (integer_to_binary(N))/binary>>, <<>>).

delivering(Buffer) ->
    #{delivering := Delivering} = emqx_extsub_buffer:inspect(Buffer),
    Delivering.
