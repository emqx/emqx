%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_DS_BITMASK_HRL).
-define(EMQX_DS_BITMASK_HRL, true).

-record(filter_scan_action, {
    offset :: emqx_ds_bitmask_keymapper:offset(),
    size :: emqx_ds_bitmask_keymapper:bitsize(),
    min :: non_neg_integer(),
    max :: non_neg_integer()
}).

-record(filter, {
    size :: non_neg_integer(),
    bitfilter :: non_neg_integer(),
    bitmask :: non_neg_integer(),
    %% Ranges (in _bitsource_ basis), array of `#filter_scan_action{}':
    bitsource_ranges :: tuple(),
    range_min :: non_neg_integer(),
    range_max :: non_neg_integer()
}).

-endif.
