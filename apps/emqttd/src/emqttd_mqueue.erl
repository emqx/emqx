
-module(emqttd_mqueue).

-export([init/1, in/1]).

-record(queue_state, {
        max_queued_messages = 1000
}).

init(Opts) ->
    {ok, #queue_state{}}.

in(Msg, Q = #queue_state{}) ->
    Q.

