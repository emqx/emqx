%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_AUTHN_CHAINS_HRL).
-define(EMQX_AUTHN_CHAINS_HRL, true).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_access_control.hrl").

-define(GLOBAL, 'mqtt:global').

-define(TRACE_AUTHN_PROVIDER(Msg), ?TRACE_AUTHN_PROVIDER(Msg, #{})).
-define(TRACE_AUTHN_PROVIDER(Msg, Meta), ?TRACE_AUTHN_PROVIDER(debug, Msg, Meta)).
-define(TRACE_AUTHN_PROVIDER(Level, Msg, Meta),
    ?TRACE_AUTHN(Level, Msg, (begin
        Meta
    end)#{
        provider => ?MODULE
    })
).

-define(TRACE_AUTHN(Msg, Meta), ?TRACE_AUTHN(debug, Msg, Meta)).
-define(TRACE_AUTHN(Level, Msg, Meta), ?TRACE(Level, ?AUTHN_TRACE_TAG, Msg, Meta)).

%% config root name all auth providers have to agree on.
-define(EMQX_AUTHENTICATION_CONFIG_ROOT_NAME, "authentication").
-define(EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_ATOM, authentication).
-define(EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_BINARY, <<"authentication">>).

%% authentication move cmd
-define(CMD_MOVE_FRONT, front).
-define(CMD_MOVE_REAR, rear).
-define(CMD_MOVE_BEFORE(Before), {before, Before}).
-define(CMD_MOVE_AFTER(After), {'after', After}).
-define(CMD_MERGE, merge).

-endif.
