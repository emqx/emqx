%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Ratelimit or Quota checker
-module(emqx_limiter).

-include("types.hrl").

-export([ init/2
        , init/4 %% XXX: Compatible with before 4.2 version
        , info/1
        , check/2
        , update_overall_limiter/3
        , delete_overall_limiter/1
        ]).

-record(limiter, {
          %% Zone
          zone :: emqx_zone:zone(),
          %% Checkers
          checkers :: [checker()]
         }).

-type(checker() :: #{ name     := name()
                    , capacity := non_neg_integer()
                    , interval := non_neg_integer()
                    , consumer := esockd_rate_limit:bucket() | emqx_zone:zone()
                    }).

-type(name() :: conn_bytes_in
              | conn_messages_in
              | conn_messages_routing
              | overall_messages_routing
              ).

-type(policy() :: [{name(), esockd_rate_limit:config()}]).

-type(info() :: #{name() :=
                  #{tokens   := non_neg_integer(),
                    capacity := non_neg_integer(),
                    interval := non_neg_integer()}}).

-type(limiter() :: #limiter{}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec(init(emqx_zone:zone(),
           maybe(esockd_rate_limit:config()),
           maybe(esockd_rate_limit:config()), policy())
     -> maybe(limiter())).
init(Zone, PubLimit, BytesIn, Specs) ->
    Merged = maps:merge(#{conn_messages_in => PubLimit,
                          conn_bytes_in => BytesIn}, maps:from_list(Specs)),
    Filtered = maps:filter(fun(_, V) -> V /= undefined end, Merged),
    init(Zone, maps:to_list(Filtered)).

-spec(init(emqx_zone:zone(), policy()) -> maybe(limiter())).
init(_Zone, []) ->
    undefined;
init(Zone, Specs) ->
    #limiter{zone = Zone, checkers = [do_init_checker(Zone, Spec) || Spec <- Specs]}.

%% @private
do_init_checker(Zone, {Name, {Capacity, Interval}}) ->
    Ck = #{name => Name, capacity => Capacity, interval => Interval},
    case is_overall_limiter(Name) of
        true ->
            case catch esockd_limiter:lookup({Zone, Name}) of
                _Info when is_map(_Info) ->
                    ignore;
                _ ->
                    esockd_limiter:create({Zone, Name}, Capacity, Interval)
            end,
            Ck#{consumer => Zone};
        _ ->
            Ck#{consumer => esockd_rate_limit:new(Capacity / Interval, Capacity)}
    end.

-spec(info(limiter()) -> info()).
info(#limiter{zone = Zone, checkers = Cks}) ->
    maps:from_list([get_info(Zone, Ck) || Ck <- Cks]).

-spec(check(#{cnt := Cnt :: non_neg_integer(),
              oct := Oct :: non_neg_integer()},
            Limiter :: limiter())
      -> {ok, NLimiter :: limiter()}
       | {pause, MilliSecs :: non_neg_integer(), NLimiter :: limiter()}).
check(#{cnt := Cnt, oct := Oct}, Limiter = #limiter{checkers = Cks}) ->
    {Pauses, NCks} = do_check(Cnt, Oct, Cks, [], []),
    case lists:max(Pauses) of
        I when I > 0 ->
            {pause, I, Limiter#limiter{checkers = NCks}};
        _ ->
            {ok, Limiter#limiter{checkers = NCks}}
    end.

%% @private
do_check(_, _, [], Pauses, NCks) ->
    {Pauses, lists:reverse(NCks)};
do_check(Pubs, Bytes, [Ck|More], Pauses, Acc) ->
    {I, NConsumer} = consume(Pubs, Bytes, Ck),
    do_check(Pubs, Bytes, More, [I|Pauses], [Ck#{consumer := NConsumer}|Acc]).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

consume(Pubs, Bytes, #{name := Name, consumer := Cons}) ->
    Tokens = case is_message_limiter(Name) of true -> Pubs; _ -> Bytes end,
    case Tokens =:= 0 of
        true ->
            {0, Cons};
        _ ->
            case is_overall_limiter(Name) of
                true ->
                    {_, Intv} = esockd_limiter:consume({Cons, Name}, Tokens),
                    {Intv, Cons};
                _ ->
                    esockd_rate_limit:check(Tokens, Cons)
            end
    end.

get_info(Zone, #{name := Name, capacity := Cap,
                 interval := Intv, consumer := Cons}) ->
    Info = case is_overall_limiter(Name) of
               true -> esockd_limiter:lookup({Zone, Name});
               _ -> esockd_rate_limit:info(Cons)
           end,
    {Name, #{capacity => Cap,
             interval => Intv,
             tokens => maps:get(tokens, Info)}}.

is_overall_limiter(overall_messages_routing) -> true;
is_overall_limiter(_) -> false.

is_message_limiter(conn_messages_in) -> true;
is_message_limiter(conn_messages_routing) -> true;
is_message_limiter(overall_messages_routing) -> true;
is_message_limiter(_) -> false.

update_overall_limiter(Zone, Capacity, Interval) ->
    try
        esockd_limiter:update({Zone, overall_messages_routing}, Capacity, Interval),
        true
    catch _:_ ->
        false
    end.

delete_overall_limiter(Zone) ->
    try
        esockd_limiter:delete({Zone, overall_messages_routing}),
        true
    catch _:_ ->
        false
    end.
