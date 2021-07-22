%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_api_subscriptions).

-behavior(minirest_api).

-include_lib("emqx/include/emqx.hrl").

-export([api_spec/0]).

-export([subscriptions/2]).

-export([ query/3
        , format/1
        ]).

-define(SUBS_QS_SCHEMA, {emqx_suboption,
        [{<<"clientid">>, binary},
        {<<"topic">>, binary},
        {<<"share">>, binary},
        {<<"qos">>, integer},
        {<<"match_topic">>, binary}]}).

-define(query_fun, {?MODULE, query}).
-define(format_fun, {?MODULE, format}).

api_spec() ->
    {
        [subscriptions_api()],
        [subscription_schema()]
    }.

subscriptions_api() ->
    MetaData = #{
        get => #{
            description => "List subscriptions",
            parameters => [
                #{
                    name => page,
                    in => query,
                    description => <<"Page">>,
                    schema => #{type => integer}
                },
                #{
                    name => limit,
                    in => query,
                    description => <<"Page size">>,
                    schema => #{type => integer}
                },
                #{
                    name => clientid,
                    in => query,
                    description => <<"Client ID">>,
                    schema => #{type => string}
                },
                #{
                    name => qos,
                    in => query,
                    description => <<"QoS">>,
                    schema => #{type => integer}
                },
                #{
                    name => share,
                    in => query,
                    description => <<"Shared subscription">>,
                    schema => #{type => boolean}
                },
                #{
                    name => topic,
                    in => query,
                    description => <<"Topic">>,
                    schema => #{type => string}
                }
                #{
                    name => match_topic,
                    in => query,
                    description => <<"Match topic string">>,
                    schema => #{type => string}
                }
            ],
            responses => #{
                <<"200">> => emqx_mgmt_util:response_page_schema(<<"subscription">>)}}},
    {"/subscriptions", MetaData, subscriptions}.

subscription_schema() ->
    #{
        subscription => #{
            type => object,
            properties => #{
                topic => #{
                    type => string},
                clientid => #{
                    type => string},
                qos => #{
                    type => integer,
                    enum => [0,1,2]}}}
    }.

subscriptions(get, Request) ->
    Qs = cowboy_req:parse_qs(Request),
    list(Qs).

list(Params) ->
    {200, emqx_mgmt_api:cluster_query(Params, ?SUBS_QS_SCHEMA, ?query_fun)}.


format(Items) when is_list(Items) ->
    [format(Item) || Item <- Items];

format({{Subscriber, Topic}, Options}) ->
    format({Subscriber, Topic, Options});

format({_Subscriber, Topic, Options = #{share := Group}}) ->
    QoS = maps:get(qos, Options),
    #{topic => filename:join([<<"$share">>, Group, Topic]), clientid => maps:get(subid, Options), qos => QoS};
format({_Subscriber, Topic, Options}) ->
    QoS = maps:get(qos, Options),
    #{topic => Topic, clientid => maps:get(subid, Options), qos => QoS}.

%%--------------------------------------------------------------------
%% Query Function
%%--------------------------------------------------------------------

query({Qs, []}, Start, Limit) ->
    Ms = qs2ms(Qs),
    emqx_mgmt_api:select_table(emqx_suboption, Ms, Start, Limit, fun format/1);

query({Qs, Fuzzy}, Start, Limit) ->
    Ms = qs2ms(Qs),
    MatchFun = match_fun(Ms, Fuzzy),
    emqx_mgmt_api:traverse_table(emqx_suboption, MatchFun, Start, Limit, fun format/1).

match_fun(Ms, Fuzzy) ->
    MsC = ets:match_spec_compile(Ms),
    fun(Rows) ->
         case ets:match_spec_run(Rows, MsC) of
             [] -> [];
             Ls -> lists:filter(fun(E) -> run_fuzzy_match(E, Fuzzy) end, Ls)
         end
    end.

run_fuzzy_match(_, []) ->
    true;
run_fuzzy_match(E = {{_, Topic}, _}, [{topic, match, TopicFilter}|Fuzzy]) ->
    emqx_topic:match(Topic, TopicFilter) andalso run_fuzzy_match(E, Fuzzy).

%%--------------------------------------------------------------------
%% Query String to Match Spec

qs2ms(Qs) ->
    MtchHead = qs2ms(Qs, {{'_', '_'}, #{}}),
    [{MtchHead, [], ['$_']}].

qs2ms([], MtchHead) ->
    MtchHead;
qs2ms([{Key, '=:=', Value} | More], MtchHead) ->
    qs2ms(More, update_ms(Key, Value, MtchHead)).

update_ms(clientid, X, {{Pid, Topic}, Opts}) ->
    {{Pid, Topic}, Opts#{subid => X}};
update_ms(topic, X, {{Pid, _Topic}, Opts}) ->
    {{Pid, X}, Opts};
update_ms(share, X, {{Pid, Topic}, Opts}) ->
    {{Pid, Topic}, Opts#{share => X}};
update_ms(qos, X, {{Pid, Topic}, Opts}) ->
    {{Pid, Topic}, Opts#{qos => X}}.
