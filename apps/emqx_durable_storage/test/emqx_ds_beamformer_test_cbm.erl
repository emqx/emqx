%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_beamformer_test_cbm).

%% API:
-export([update_iterator/3]).

update_iterator(_Shard, It0, NextKey) ->
    {ok, setelement(2, It0, NextKey)}.
