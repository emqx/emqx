%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_PERSISTENT_MESSAGE_HRL).
-define(EMQX_PERSISTENT_MESSAGE_HRL, true).

-include_lib("emqx_utils/include/emqx_ds_dbs.hrl").
-define(PERSISTENCE_ENABLED, emqx_message_persistence_enabled).

-define(WITH_DURABILITY_ENABLED(DO),
    case is_persistence_enabled() of
        true -> DO;
        false -> {skipped, disabled}
    end
).

-endif.
