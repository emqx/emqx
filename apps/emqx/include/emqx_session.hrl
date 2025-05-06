%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_SESSION_HRL).
-define(EMQX_SESSION_HRL, true).

-define(IS_SESSION_IMPL_MEM(S), (is_tuple(S) andalso element(1, S) =:= session)).
-define(IS_SESSION_IMPL_DS(S), (is_map_key(id, S))).

%% (Erlang) messages that a connection process should forward to the
%% session handler.
-define(session_message(MSG), MSG).

-endif.
