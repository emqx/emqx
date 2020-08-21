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


-module(prop_emqx_dummy_topics).

-include_lib("proper/include/proper.hrl").

-export([prop_dummy_topic_match/0]).


-define(ALL(Vars, Types, Exprs),
        ?SETUP(fun() ->
            State = do_setup(),
            fun() -> do_teardown(State) end
         end, ?FORALL(Vars, Types, Exprs))).

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

prop_dummy_topic_match() ->
    ?ALL({Topic, Filter},
         {topic(), topic_filter()},
         %% '#' should always be the last level
         %% but I don't want to comply with that
         case re:run(Filter, "#/.*") of
            nomatch ->
                ok = emqx_dummy_topics:add(Filter),
                Expected = emqx_topic:match(Topic, Filter),
                Got = emqx_dummy_topics:match(Topic),
                %io:format("topic: ~p, fileter: ~p~nres: ~p, ~p~n",
                %    [Topic, Filter, Expected, Got]),
                ok = emqx_dummy_topics:del(Filter),
                Expected =:= Got;
            {match, _} ->
                true
         end).

%%--------------------------------------------------------------------
%% Helper
%%--------------------------------------------------------------------

do_setup() ->
    ok = ekka:start(),
    %% for coverage
    ok = emqx_dummy_topics:mnesia(boot),
    ok = emqx_dummy_topics:mnesia(copy),
    ok = emqx_logger:set_log_level(emergency),
    {ok, _} = emqx_dummy_topics:start_link().

do_teardown(_) ->
    ok = emqx_dummy_topics:stop(),
    ekka:stop(),
    ekka_mnesia:ensure_stopped(),
    ok = emqx_logger:set_log_level(error).

%%--------------------------------------------------------------------
%% Generator
%%--------------------------------------------------------------------
topic_filter() ->
    CharSet = list(oneof([
            range(48, 57), %% [0-9]
            range(65, 90), %% [A-Z]
            range(97, 122), %% [a-z]
            $/,
            $$,
            $#,
            $+
          ])),
    ?LET(Chars, CharSet, iolist_to_binary(Chars)).

topic() ->
    CharSet = list(oneof([
            range(48, 57), %% [0-9]
            range(65, 90), %% [A-Z]
            range(97, 122), %% [a-z]
            $/,
            $$
          ])),
    ?LET(Chars, CharSet, iolist_to_binary(Chars)).