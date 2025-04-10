%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(prop_emqx_psk).

-include_lib("proper/include/proper.hrl").

-define(ALL(Vars, Types, Exprs),
    ?SETUP(
        fun() ->
            State = do_setup(),
            fun() -> do_teardown(State) end
        end,
        ?FORALL(Vars, Types, Exprs)
    )
).

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

prop_lookup() ->
    ?ALL(
        {ClientPSKID, UserState},
        {client_pskid(), user_state()},
        begin
            case emqx_tls_psk:lookup(psk, ClientPSKID, UserState) of
                {ok, _Result} -> true;
                error -> true;
                _Other -> false
            end
        end
    ).

%%--------------------------------------------------------------------
%% Helper
%%--------------------------------------------------------------------

do_setup() ->
    ok = emqx_logger:set_log_level(emergency),
    ok = meck:new(emqx_hooks, [passthrough, no_history]),
    ok = meck:expect(
        emqx_hooks,
        run_fold,
        fun('tls_handshake.psk_lookup', [ClientPSKID], not_found) ->
            unicode:characters_to_binary(ClientPSKID)
        end
    ).

do_teardown(_) ->
    ok = emqx_logger:set_log_level(error),
    ok = meck:unload(emqx_hooks).

%%--------------------------------------------------------------------
%% Generator
%%--------------------------------------------------------------------

client_pskid() -> oneof([string(), integer(), [1, [-1]]]).

user_state() -> term().
