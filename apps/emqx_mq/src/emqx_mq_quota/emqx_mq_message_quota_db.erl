%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_message_quota_db).

-moduledoc """
This module is used for reading and writing the quota index to the DS.
""".

-include("emqx_mq_quota.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    read/4,
    write/2
]).
