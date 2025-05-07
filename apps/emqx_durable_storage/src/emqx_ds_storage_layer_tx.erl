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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-type ctrie() :: term().

-opaque ctx() :: #kv_tx_ctx{}.

%% For simplicity we don't support full MQTT topic filtering. Only '#'
%% type of wildcard is allowed. First '+' encountered is replaced with
%% '#'.
-type conflict_domain() :: [binary() | [] | '#'].

%%================================================================================
%% API functions
%%================================================================================

-spec new_kv_tx_ctx(emqx_ds:db(), emqx_ds:shard(), emqx_ds:transaction_opts(), emqx_ds:tx_serial()) ->
    ctx().
new_kv_tx_ctx(DB, Shard, Options, _Serial) ->
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

%% @doc Replace all topic levels in a filter that follow a wildcard
%% levels with '#'
-spec topic_filter_to_conflict_domain(emqx_ds:topic_filter()) -> conflict_domain().
topic_filter_to_conflict_domain(TF) ->
    tf2cd(TF, []).

tf2cd([], Acc) ->
    lists:reverse(Acc);
tf2cd(['#' | _], Acc) ->
    lists:reverse(['#' | Acc]);
tf2cd(['+' | _], Acc) ->
    lists:reverse(['#' | Acc]);
tf2cd([Const | Rest], Acc) ->
    tf2cd(Rest, [Const | Acc]).

%% @doc Create a conflict detection trie.
-spec ctrie_new(emqx_ds:tx_serial()) -> ctrie().
ctrie_new(_MinSerial) ->
    error(not_implemented).

%% @doc Insert conflict into the trie
-spec ctrie_push(conflict_domain(), emqx_ds:tx_serial(), ctrie()) -> ctrie().
ctrie_push(_CD, _Serial, _Ctrie) ->
    error(not_implemented).

%% @doc Check for the conflicts in the topic filter. Return `false' if
%% there is no conflicts or `true' if there is a conflict, or serial
%% is out of the tracking range.
-spec ctrie_check(conflict_domain(), emqx_ds:tx_serial(), ctrie()) -> boolean().
ctrie_check(_TopicFilter, _Serial, _Ctrie) ->
    error(not_implemented).

%% @doc Trunkate ctrie to the given size by deleting the oldest
%% entries.
-spec ctrie_trunk(pos_integer(), ctrie()) -> ctrie().
ctrie_trunk(Length, _Ctrie) when Length > 0 ->
    error(not_implemented).

cd_match([], []) ->
    true;
cd_match(['#'], _) ->
    true;
cd_match(_, ['#']) ->
    true;
cd_match([A | L1], [A | L2]) ->
    cd_match(L1, L2);
cd_match(_, _) ->
    false.

%%================================================================================
%% Tests
%%================================================================================

-ifdef(TEST).

%% Reference implementation of the collision checker. It's simply the
%% full list of operations.
ctref_new(MinSerial) ->
    {MinSerial, []}.

ctref_push(CD, Serial, {MinSerial, CT0}) ->
    CT = [{Serial, CD} | CT0],
    {MinSerial, CT}.

ctref_check(_CD, Serial, {MinSerial, _}) when Serial < MinSerial ->
    true;
ctref_check(CD, Serial, {_MinSerial, CT}) ->
    Matches = [{S, D} || {S, D} <- CT, S >= Serial, cd_match(CD, D)],
    case Matches of
        [] ->
            false;
        _ ->
            true
    end.

ctref_drop(MinSerial, {_, CT0}) ->
    CT = [{S, D} || {S, D} <- CT0, S >= MinSerial],
    {MinSerial, CT}.

tf2cd_test() ->
    ?assertEqual([], topic_filter_to_conflict_domain([])),
    ?assertEqual(
        [<<"foo">>, <<"bar">>],
        topic_filter_to_conflict_domain([<<"foo">>, <<"bar">>])
    ),
    ?assertEqual(
        [<<"foo">>, <<"bar">>],
        topic_filter_to_conflict_domain([<<"foo">>, <<"bar">>, '+', <<"quux">>])
    ),
    ?assertEqual(
        [<<"1">>, <<"2">>],
        topic_filter_to_conflict_domain([<<"1">>, <<"2">>, '#'])
    ).

-endif.
