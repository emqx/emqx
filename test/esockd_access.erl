%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(esockd_access).

-type cidr() :: string().

-type rule() :: {allow, all} |
                {allow, cidr()} |
                {deny,  all} |
                {deny,  cidr()}.

-type range() :: {cidr(), pos_integer(), pos_integer()}.

-export_type([cidr/0, range/0, rule/0]).

-type range_rule() :: {allow, all} |
                      {allow, range()} |
                      {deny,  all} |
                      {deny,  range()}.

-export([rule/1, match/2, range/1,  mask/1, atoi/1, itoa/1]).

%%------------------------------------------------------------------------------
%% @doc
%% Build CIDR, Make rule.
%%
%% @end
%%------------------------------------------------------------------------------
-spec rule(rule()) -> range_rule().
rule({allow, all}) ->
    {allow, all};
rule({allow, CIDR}) when is_list(CIDR) ->
    rule(allow, CIDR);
rule({deny, CIDR}) when is_list(CIDR) ->
    rule(deny, CIDR);
rule({deny, all}) ->
    {deny, all}.

rule(Type, CIDR) when is_list(CIDR) ->
    {Start, End} = range(CIDR), {Type, {CIDR, Start, End}}.

%%------------------------------------------------------------------------------
%% @doc
%% Match Addr with Access Rules.
%%
%% @end
%%------------------------------------------------------------------------------
-spec match(inet:ip_address(), [range_rule()]) -> {matched, allow} | {matched, deny} | nomatch.
match(Addr, Rules) when is_tuple(Addr) ->
    match2(atoi(Addr), Rules).

match2(_I, []) ->
    nomatch;
match2(_I, [{allow, all}|_]) ->
    {matched, allow};
match2(I, [{allow, {_, Start, End}}|_]) when I >= Start, I =< End ->
    {matched, allow};
match2(I, [{allow, {_, _Start, _End}}|Rules]) ->
    match2(I, Rules);
match2(I, [{deny, {_, Start, End}}|_]) when I >= Start, I =< End ->
    {matched, deny};
match2(I, [{deny, {_, _Start, _End}}|Rules]) ->
    match2(I, Rules);
match2(_I, [{deny, all}|_]) ->
    {matched, deny}.

%%------------------------------------------------------------------------------
%% @doc
%% CIDR range.
%%
%% @end
%%------------------------------------------------------------------------------
-spec range(cidr()) -> {pos_integer(), pos_integer()}.
range(CIDR) ->
    case string:tokens(CIDR, "/") of
        [Addr] ->
            {ok, IP} = inet:getaddr(Addr, inet),
            {atoi(IP), atoi(IP)};
        [Addr, Mask] ->
            {ok, IP} = inet:getaddr(Addr, inet),
            {Start, End} = subnet(IP, mask(list_to_integer(Mask))),
            {Start, End}
    end.

subnet(IP, Mask) ->
    Start = atoi(IP) band Mask,
    End = Start bor (Mask bxor 16#FFFFFFFF),
    {Start, End}.

%%------------------------------------------------------------------------------
%% @doc
%% Mask Int
%%
%% @end
%%------------------------------------------------------------------------------
-spec mask(0..32) -> 0..16#FFFFFFFF.
mask(0) ->
    16#00000000;
mask(32) ->
    16#FFFFFFFF;
mask(N) when N >= 1, N =< 31 ->
    lists:foldl(fun(I, Mask) -> (1 bsl I) bor Mask end, 0, lists:seq(32 - N, 31)).

%%------------------------------------------------------------------------------
%% @doc
%% Addr to Integer.
%%
%% @end
%%------------------------------------------------------------------------------
atoi({A, B, C, D}) ->
    (A bsl 24) + (B bsl 16) + (C bsl 8) + D.

%%------------------------------------------------------------------------------
%% @doc
%% Integer to Addr.
%%
%% @end
%%------------------------------------------------------------------------------
itoa(I) ->
    A = (I bsr 24) band 16#FF,
    B = (I bsr 16) band 16#FF,
    C = (I bsr 8)  band 16#FF,
    D = I band 16#FF,
    {A, B, C, D}.


