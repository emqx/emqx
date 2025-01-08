%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
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
