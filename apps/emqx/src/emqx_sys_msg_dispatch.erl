%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sys_msg_dispatch).

-moduledoc """
One `message.publish` callback for all `$`-prefixed system topics.

Features that consume their own system topics (for example `$SETOPTS/`)
register a topic prefix and a handler here, instead of adding one
`message.publish` callback each. A publish to a topic that does not start
with `$` then costs one callback, however many features are registered.

The callback runs at priority `HP_SYS_MSGS`. It is in the hook list only
while at least one handler is registered. The handlers are carried in the
callback's own arguments, so a publish reads no other state.
""".

-include("emqx.hrl").
-include("emqx_hooks.hrl").

-export([
    register/2,
    unregister/1,
    handlers/0
]).

%% Hook callback
-export([on_message_publish/2]).

-export_type([prefix/0, handler/0]).

-define(HOOK, 'message.publish').

-type prefix() :: <<_:8, _:_*8>>.
-type handler() :: {module(), atom(), [term()]}.

-doc """
Register `Handler` for publishes to topics that start with `Prefix`.
`Prefix` must start with `$`. The handler is called as
`apply(M, F, [Message | A])` and returns a `message.publish` fold result.

Registering the same prefix again replaces its handler. A prefix that is
a prefix of another registered one, or the other way around, is refused.
""".
-spec register(prefix(), handler()) -> ok | {error, {prefix_conflict, prefix()}}.
register(<<"$", _/binary>> = Prefix, {M, F, A} = Handler) when
    is_atom(M), is_atom(F), is_list(A)
->
    with_lock(fun() ->
        Handlers = handlers(),
        case conflict(Prefix, Handlers) of
            false ->
                put_hook(lists:keystore(Prefix, 1, Handlers, {Prefix, Handler}));
            {true, Other} ->
                {error, {prefix_conflict, Other}}
        end
    end).

-doc """
Remove the handler of `Prefix`. Removing an absent prefix returns `ok`.
""".
-spec unregister(prefix()) -> ok.
unregister(Prefix) ->
    with_lock(fun() ->
        case lists:keydelete(Prefix, 1, handlers()) of
            [] -> emqx_hooks:del(?HOOK, {?MODULE, on_message_publish});
            Handlers -> put_hook(Handlers)
        end
    end).

-doc """
Return the registered `{Prefix, Handler}` pairs.
""".
-spec handlers() -> [{prefix(), handler()}].
handlers() ->
    Actions = [emqx_hooks:callback_action(CB) || CB <- emqx_hooks:lookup(?HOOK)],
    case [Handlers || {?MODULE, on_message_publish, [Handlers]} <- Actions] of
        [Handlers] -> Handlers;
        [] -> []
    end.

-doc """
`message.publish` callback. Pass a `$`-prefixed message to the handler
of the matching prefix. Other messages pass through.
""".
on_message_publish(#message{topic = <<"$", _/binary>> = Topic} = Msg, Handlers) ->
    dispatch(Topic, Msg, Handlers);
on_message_publish(_Msg, _Handlers) ->
    ok.

dispatch(Topic, Msg, [{Prefix, {M, F, A}} | Handlers]) ->
    case is_prefix(Prefix, Topic) of
        true -> erlang:apply(M, F, [Msg | A]);
        false -> dispatch(Topic, Msg, Handlers)
    end;
dispatch(_Topic, _Msg, []) ->
    ok.

is_prefix(Prefix, Bin) ->
    Size = byte_size(Prefix),
    case Bin of
        <<Prefix:Size/binary, _/binary>> -> true;
        _ -> false
    end.

conflict(Prefix, Handlers) ->
    case
        [
            Other
         || {Other, _} <- Handlers,
            Other =/= Prefix,
            is_prefix(Other, Prefix) orelse is_prefix(Prefix, Other)
        ]
    of
        [] -> false;
        [Other | _] -> {true, Other}
    end.

put_hook(Handlers) ->
    emqx_hooks:put(?HOOK, {?MODULE, on_message_publish, [Handlers]}, ?HP_SYS_MSGS).

%% Serialize the read-modify-write of the handler list on this node.
with_lock(Fun) ->
    global:trans({?MODULE, self()}, Fun, [node()]).
