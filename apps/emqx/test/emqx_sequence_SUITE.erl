%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sequence_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-import(emqx_sequence,
        [ nextval/2
        , currval/2
        , reclaim/2
        ]).

all() -> emqx_ct:all(?MODULE).

% t_currval(_) ->
%     error('TODO').

% t_delete(_) ->
%     error('TODO').

% t_create(_) ->
%     error('TODO').

% t_reclaim(_) ->
%     error('TODO').

% t_nextval(_) ->
%     error('TODO').

t_generate(_) ->
    ok = emqx_sequence:create(seqtab),
    ?assertEqual(0, currval(seqtab, key)),
    ?assertEqual(1, nextval(seqtab, key)),
    ?assertEqual(1, currval(seqtab, key)),
    ?assertEqual(2, nextval(seqtab, key)),
    ?assertEqual(2, currval(seqtab, key)),
    ?assertEqual(3, nextval(seqtab, key)),
    ?assertEqual(2, reclaim(seqtab, key)),
    ?assertEqual(1, reclaim(seqtab, key)),
    ?assertEqual(0, reclaim(seqtab, key)),
    ?assertEqual(1, nextval(seqtab, key)),
    ?assertEqual(0, reclaim(seqtab, key)),
    ?assertEqual(0, reclaim(seqtab, key)),
    ?assertEqual(false, ets:member(seqtab, key)),
    ?assert(emqx_sequence:delete(seqtab)),
    ?assertNot(emqx_sequence:delete(seqtab)).

