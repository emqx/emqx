%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Transactional wrapper for `emqx_ds_storage_layer'
-module(emqx_ds_storage_layer_tx).

%% API:
-export([
    new_kv_tx_ctx/4,
    verify_preconditions/3
]).

%% internal exports:
-export([]).

-export_type([
    ctx/0
]).

-include("emqx_ds.hrl").
-include("emqx_ds_storage_layer_tx.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-opaque ctx() :: #kv_tx_ctx{}.

%%================================================================================
%% API functions
%%================================================================================

-spec new_kv_tx_ctx(emqx_ds:db(), emqx_ds:shard(), emqx_ds:transaction_opts(), emqx_ds:time()) ->
    ctx().
new_kv_tx_ctx(DB, Shard, Options, _Now) ->
    %% Add current generation as a guard to the context to make sure
    %% the transaction doesn't span multiple generations:
    {Generation, _} = emqx_ds_storage_layer:find_generation({DB, Shard}, current),
    case Options of
        #{generation := G} when G =/= Generation ->
            ?err_unrec({generation_is_not_current, G});
        _ ->
            {ok, #kv_tx_ctx{
                shard = Shard,
                generation = Generation,
                opts = Options
            }}
    end.

-spec verify_preconditions(emqx_ds:db(), ctx(), emqx_ds:kv_tx_ops()) -> ok | emqx_ds:error().
verify_preconditions(DB, #kv_tx_ctx{shard = Shard, generation = Gen}, Ops) ->
    ErrAcc0 = lists:foldl(
        fun({Topic, ValueMatcher}, Acc) ->
            case emqx_ds_storage_layer:lookup_kv({DB, Shard}, Gen, Topic) of
                {ok, {_, Value}} when
                    ValueMatcher =:= '_';
                    ValueMatcher =:= Value
                ->
                    Acc;
                {ok, {_, Value}} ->
                    [#{topic => Topic, expected => ValueMatcher, got => Value} | Acc];
                undefined ->
                    [#{topic => Topic, expected => ValueMatcher, got => undefined} | Acc]
            end
        end,
        [],
        maps:get(?ds_tx_expected, Ops, [])
    ),
    Errors = lists:foldl(
        fun(Topic, Acc) ->
            case emqx_ds_storage_layer:lookup_kv({DB, Shard}, Gen, Topic) of
                {ok, Value} ->
                    [#{topic => Topic, unexpected => Value} | Acc];
                undefined ->
                    Acc
            end
        end,
        ErrAcc0,
        maps:get(?ds_tx_unexpected, Ops, [])
    ),
    case Errors of
        [] ->
            ok;
        _ ->
            ?err_unrec({precondition_failed, Errors})
    end.

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
