%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_SCHEMA_HRL).
-define(EMQX_SCHEMA_HRL, true).

-define(TOMBSTONE_TYPE, marked_for_deletion).
-define(TOMBSTONE_VALUE, <<"marked_for_deletion">>).
-define(TOMBSTONE_CONFIG_CHANGE_REQ, mark_it_for_deletion).
-define(CONFIG_NOT_FOUND_MAGIC, '$0tFound').

-endif.
