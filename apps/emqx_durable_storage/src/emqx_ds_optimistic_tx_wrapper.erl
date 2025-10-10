%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_optimistic_tx_wrapper).
-moduledoc """
The entrypoint for `emqx_ds_optimistic_tx` process.

The purpose of this wrapper module is to serve as a simple process
entypoint, used to facilitate hot update of the main module containing
all the business logic.
""".

%% API:
-export([init/4]).

-include("emqx_ds_optimistic_tx_internals.hrl").

%%================================================================================
%% API functions
%%================================================================================

-spec init(pid(), emqx_ds:db(), emqx_ds:shard(), module()) -> no_return().
init(Parent, DB, Shard, CBM) ->
    yes = gproc:register_name(?name(DB, Shard), self()),
    try
        emqx_ds_optimistic_tx:init(Parent, DB, Shard, CBM)
    after
        gen:unregister_name(?via(DB, Shard))
    end.
