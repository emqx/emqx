%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_MT_HRL).
-define(EMQX_MT_HRL, true).

-define(DEFAULT_PAGE_SIZE, 100).
-define(MAX_PAGE_SIZE, 1000).
-define(MIN_NS, <<>>).
-define(MIN_CLIENTID, <<>>).

-include_lib("emqx/include/logger.hrl").

-define(LOG(Level, Msg), ?SLOG(Level, Msg, #{domain => [emqx, mt]})).

-endif.
