%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_FT_STORAGE_FS_HRL).
-define(EMQX_FT_STORAGE_FS_HRL, true).

-record(gcstats, {
    started_at :: integer(),
    finished_at :: integer() | undefined,
    files = 0 :: non_neg_integer(),
    directories = 0 :: non_neg_integer(),
    space = 0 :: non_neg_integer(),
    errors = #{} :: #{_GCSubject => {error, _}}
}).

-endif.
