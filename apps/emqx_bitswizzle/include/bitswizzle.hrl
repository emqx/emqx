%%--------------------------------------------------------------------
%% Copyright (c) 2022, 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-ifndef(BITSWIZZLE_HRL).
-define(BITSWIZZLE_HRL, true).

-record(smatrix_diag, {
    offset :: non_neg_integer(),
    mask :: non_neg_integer()
}).

-record(smatrix_cell, {
    row :: non_neg_integer(),
    column :: non_neg_integer(),
    size :: non_neg_integer(),
    diags :: [#smatrix_diag{}]
}).

-record(sparse_matrix, {
    n_dim :: non_neg_integer(),
    cells :: [#smatrix_cell{}]
}).

-endif.
