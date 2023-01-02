%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_api_clients).

-include("emqx_mgmt.hrl").

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(CLIENT_QS_SCHEMA, {emqx_channel_info,
        [{<<"clientid">>, binary},
         {<<"username">>, binary},
         {<<"zone">>, atom},
         {<<"ip_address">>, ip},
         {<<"conn_state">>, atom},
         {<<"clean_start">>, atom},
         {<<"proto_name">>, binary},
         {<<"proto_ver">>, integer},
         {<<"_like_clientid">>, binary},
         {<<"_like_username">>, binary},
         {<<"_gte_created_at">>, timestamp},
         {<<"_lte_created_at">>, timestamp},
         {<<"_gte_connected_at">>, timestamp},
         {<<"_gte_mqueue_len">>, integer},
         {<<"_lte_mqueue_len">>, integer},
         {<<"_gte_mqueue_dropped">>, integer},
         {<<"_lte_mqueue_dropped">>, integer},
         {<<"_lte_connected_at">>, timestamp}]}).

-rest_api(#{name   => list_clients,
            method => 'GET',
            path   => "/clients/",
            func   => list,
            descr  => "A list of clients on current node"}).

-rest_api(#{name   => list_node_clients,
            method => 'GET',
            path   => "nodes/:atom:node/clients/",
            func   => list,
            descr  => "A list of clients on specified node"}).

-rest_api(#{name   => lookup_client,
            method => 'GET',
            path   => "/clients/:bin:clientid",
            func   => lookup,
            descr  => "Lookup a client in the cluster"}).

-rest_api(#{name   => lookup_node_client,
            method => 'GET',
            path   => "nodes/:atom:node/clients/:bin:clientid",
            func   => lookup,
            descr  => "Lookup a client on the node"}).

-rest_api(#{name   => lookup_client_via_username,
            method => 'GET',
            path   => "/clients/username/:bin:username",
            func   => lookup,
            descr  => "Lookup a client via username in the cluster"
           }).

-rest_api(#{name   => lookup_node_client_via_username,
            method => 'GET',
            path   => "/nodes/:atom:node/clients/username/:bin:username",
            func   => lookup,
            descr  => "Lookup a client via username on the node "
           }).

-rest_api(#{name   => kickout_client,
            method => 'DELETE',
            path   => "/clients/:bin:clientid",
            func   => kickout,
            descr  => "Kick out the client in the cluster"}).

-rest_api(#{name   => clean_acl_cache,
            method => 'DELETE',
            path   => "/clients/:bin:clientid/acl_cache",
            func   => clean_acl_cache,
            descr  => "Clear the ACL cache of a specified client in the cluster"}).

-rest_api(#{name   => list_acl_cache,
            method => 'GET',
            path   => "/clients/:bin:clientid/acl_cache",
            func   => list_acl_cache,
            descr  => "List the ACL cache of a specified client in the cluster"}).

-rest_api(#{name   => set_ratelimit_policy,
            method => 'POST',
            path   => "/clients/:bin:clientid/ratelimit",
            func   => set_ratelimit_policy,
            descr  => "Set the client ratelimit policy"}).

-rest_api(#{name   => clean_ratelimit,
            method => 'DELETE',
            path   => "/clients/:bin:clientid/ratelimit",
            func   => clean_ratelimit,
            descr  => "Clear the ratelimit policy"}).

-rest_api(#{name   => set_quota_policy,
            method => 'POST',
            path   => "/clients/:bin:clientid/quota",
            func   => set_quota_policy,
            descr  => "Set the client quota policy"}).

-rest_api(#{name   => clean_quota,
            method => 'DELETE',
            path   => "/clients/:bin:clientid/quota",
            func   => clean_quota,
            descr  => "Clear the quota policy"}).

-rest_api(#{name   => set_keepalive,
            method => 'PUT',
            path   => "/clients/:bin:clientid/keepalive",
            func   => set_keepalive,
            descr  => "Set the client keepalive"}).

-import(emqx_mgmt_util, [ ntoa/1
                        , strftime/1
                        ]).

-export([ list/2
        , lookup/2
        , kickout/2
        , clean_acl_cache/2
        , list_acl_cache/2
        , set_ratelimit_policy/2
        , set_quota_policy/2
        , clean_ratelimit/2
        , clean_quota/2
        , set_keepalive/2
        ]).

-export([ query/3
        , format_channel_info/1
        ]).

-define(QUERY_FUN, {?MODULE, query}).
-define(FORMAT_FUN, {?MODULE, format_channel_info}).

list(Bindings, Params) when map_size(Bindings) == 0 ->
    fence(fun() ->
        emqx_mgmt_api:cluster_query(Params, ?CLIENT_QS_SCHEMA, ?QUERY_FUN)
    end);

list(#{node := Node}, Params) when Node =:= node() ->
    fence(fun() ->
        emqx_mgmt_api:node_query(Node, Params, ?CLIENT_QS_SCHEMA, ?QUERY_FUN)
    end);

list(Bindings = #{node := Node}, Params) ->
    case rpc:call(Node, ?MODULE, list, [Bindings, Params]) of
        {badrpc, Reason} -> minirest:return({error, ?ERROR1, Reason});
        Res -> Res
    end.

%% @private
fence(Func) ->
    try
        minirest:return({ok, Func()})
    catch
        throw : {bad_value_type, {_Key, Type, Value}} ->
            Reason = iolist_to_binary(
                       io_lib:format("Can't convert ~p to ~p type",
                                     [Value, Type])
                      ),
            minirest:return({error, ?ERROR8, Reason})
    end.

lookup(#{node := Node, clientid := ClientId}, _Params) ->
    minirest:return({ok, emqx_mgmt:lookup_client(Node,
        {clientid, emqx_mgmt_util:urldecode(ClientId)}, ?FORMAT_FUN)});

lookup(#{clientid := ClientId}, _Params) ->
    minirest:return({ok, emqx_mgmt:lookup_client(
        {clientid, emqx_mgmt_util:urldecode(ClientId)}, ?FORMAT_FUN)});

lookup(#{node := Node, username := Username}, _Params) ->
    minirest:return({ok, emqx_mgmt:lookup_client(Node,
        {username, emqx_mgmt_util:urldecode(Username)}, ?FORMAT_FUN)});

lookup(#{username := Username}, _Params) ->
    minirest:return({ok, emqx_mgmt:lookup_client({username,
        emqx_mgmt_util:urldecode(Username)}, ?FORMAT_FUN)}).

kickout(#{clientid := ClientId}, _Params) ->
    case emqx_mgmt:kickout_client(emqx_mgmt_util:urldecode(ClientId)) of
        ok -> minirest:return();
        {error, not_found} -> minirest:return({error, ?ERROR12, not_found});
        {error, Reason} -> minirest:return({error, ?ERROR1, Reason})
    end.

clean_acl_cache(#{clientid := ClientId}, _Params) ->
    case emqx_mgmt:clean_acl_cache(emqx_mgmt_util:urldecode(ClientId)) of
        ok -> minirest:return();
        {error, not_found} -> minirest:return({error, ?ERROR12, not_found});
        {error, Reason} -> minirest:return({error, ?ERROR1, Reason})
    end.

list_acl_cache(#{clientid := ClientId}, _Params) ->
    case emqx_mgmt:list_acl_cache(emqx_mgmt_util:urldecode(ClientId)) of
        {error, not_found} -> minirest:return({error, ?ERROR12, not_found});
        {error, Reason} -> minirest:return({error, ?ERROR1, Reason});
        Caches -> minirest:return({ok, [format_acl_cache(Cache) || Cache <- Caches]})
    end.

set_ratelimit_policy(#{clientid := ClientId}, Params) ->
    P = [{conn_bytes_in, proplists:get_value(<<"conn_bytes_in">>, Params)},
         {conn_messages_in, proplists:get_value(<<"conn_messages_in">>, Params)}],
    case filter_ratelimit_params(P) of
        [] -> minirest:return();
        Policy ->
            case emqx_mgmt:set_ratelimit_policy(emqx_mgmt_util:urldecode(ClientId), Policy) of
                ok -> minirest:return();
                {error, not_found} -> minirest:return({error, ?ERROR12, not_found});
                {error, Reason} -> minirest:return({error, ?ERROR1, Reason})
            end
    end.

clean_ratelimit(#{clientid := ClientId}, _Params) ->
    case emqx_mgmt:set_ratelimit_policy(emqx_mgmt_util:urldecode(ClientId), []) of
        ok -> minirest:return();
        {error, not_found} -> minirest:return({error, ?ERROR12, not_found});
        {error, Reason} -> minirest:return({error, ?ERROR1, Reason})
    end.

set_quota_policy(#{clientid := ClientId}, Params) ->
    P = [{conn_messages_routing, proplists:get_value(<<"conn_messages_routing">>, Params)}],
    case filter_ratelimit_params(P) of
        [] -> minirest:return();
        Policy ->
            case emqx_mgmt:set_quota_policy(emqx_mgmt_util:urldecode(ClientId), Policy) of
                ok -> minirest:return();
                {error, not_found} -> minirest:return({error, ?ERROR12, not_found});
                {error, Reason} -> minirest:return({error, ?ERROR1, Reason})
            end
    end.


clean_quota(#{clientid := ClientId}, _Params) ->
    case emqx_mgmt:set_quota_policy(emqx_mgmt_util:urldecode(ClientId), []) of
        ok -> minirest:return();
        {error, not_found} -> minirest:return({error, ?ERROR12, not_found});
        {error, Reason} -> minirest:return({error, ?ERROR1, Reason})
    end.

set_keepalive(#{clientid := ClientId}, Params) ->
    case proplists:get_value(<<"interval">>, Params) of
        undefined ->
            minirest:return({error, ?ERROR7, params_not_found});
        Interval0 ->
            Interval = to_integer(Interval0),
            case emqx_mgmt:set_keepalive(emqx_mgmt_util:urldecode(ClientId), Interval) of
                ok -> minirest:return();
                {error, not_found} -> minirest:return({error, ?ERROR12, not_found});
                {error, Code, Reason} -> minirest:return({error, Code, Reason});
                {error, Reason} -> minirest:return({error, ?ERROR1, Reason})
            end
    end.

to_integer(Int)when is_integer(Int) -> Int;
to_integer(Bin) when is_binary(Bin) -> binary_to_integer(Bin).

%% @private
%% S = 100,1s
%%   | 100KB, 1m
parse_ratelimit_str(S) when is_binary(S) ->
    parse_ratelimit_str(binary_to_list(S));
parse_ratelimit_str(S) ->
    [L, D] = string:tokens(S, ", "),
    Limit = case cuttlefish_bytesize:parse(L) of
                Sz when is_integer(Sz) -> Sz;
                {error, Reason1} -> error(Reason1)
            end,
    Duration = case cuttlefish_duration:parse(D, s) of
                   Secs when is_integer(Secs) -> Secs;
                   {error, Reason} -> error(Reason)
               end,
    {Limit, Duration}.

%%--------------------------------------------------------------------
%% Format

format_channel_info({_Key, Info, Stats0}) ->
    Stats = maps:from_list(Stats0),
    ClientInfo = maps:get(clientinfo, Info, #{}),
    ConnInfo = maps:get(conninfo, Info, #{}),
    Session = case maps:get(session, Info, #{}) of
                  undefined -> #{};
                  Sess -> Sess
              end,
    SessCreated = maps:get(created_at, Session, maps:get(connected_at, ConnInfo)),
    Connected = case maps:get(conn_state, Info, connected) of
                    connected -> true;
                    accepted -> true;  %% for exproto anonymous clients
                    _ -> false
                end,
    NStats = Stats#{max_subscriptions => maps:get(subscriptions_max, Stats, 0),
                    max_inflight => maps:get(inflight_max, Stats, 0),
                    max_awaiting_rel => maps:get(awaiting_rel_max, Stats, 0),
                    max_mqueue => maps:get(mqueue_max, Stats, 0),
                    inflight => maps:get(inflight_cnt, Stats, 0),
                    awaiting_rel => maps:get(awaiting_rel_cnt, Stats, 0)},
    format(
    lists:foldl(fun(Items, Acc) ->
                    maps:merge(Items, Acc)
                end, #{connected => Connected},
                [maps:with([ subscriptions_cnt, max_subscriptions,
                             inflight, max_inflight, awaiting_rel,
                             max_awaiting_rel, mqueue_len, mqueue_dropped,
                             max_mqueue, heap_size, reductions, mailbox_len,
                             recv_cnt,
                             recv_msg, 'recv_msg.qos0', 'recv_msg.qos1', 'recv_msg.qos2',
                            'recv_msg.dropped', 'recv_msg.dropped.expired',
                             recv_oct, recv_pkt, send_cnt,
                             send_msg, 'send_msg.qos0', 'send_msg.qos1', 'send_msg.qos2',
                            'send_msg.dropped', 'send_msg.dropped.expired',
                            'send_msg.dropped.queue_full', 'send_msg.dropped.too_large',
                             send_oct, send_pkt], NStats),
                 maps:with([clientid, username, mountpoint, is_bridge, zone], ClientInfo),
                 maps:with([clean_start, keepalive, expiry_interval, proto_name,
                            proto_ver, peername, connected_at, disconnected_at], ConnInfo),
                 #{created_at => SessCreated}])).

format(Data) when is_map(Data)->
    {IpAddr, Port} = maps:get(peername, Data),
    ConnectedAt = maps:get(connected_at, Data),
    CreatedAt = maps:get(created_at, Data),
    Data1 = maps:without([peername], Data),
    maps:merge(Data1#{node         => node(),
                      ip_address   => iolist_to_binary(ntoa(IpAddr)),
                      port         => Port,
                      connected_at => iolist_to_binary(strftime(ConnectedAt div 1000)),
                      created_at   => iolist_to_binary(strftime(CreatedAt div 1000))},
               case maps:get(disconnected_at, Data, undefined) of
                   undefined -> #{};
                   DisconnectedAt -> #{disconnected_at =>
                                       iolist_to_binary(strftime(DisconnectedAt div 1000))}
               end).

format_acl_cache({{PubSub, Topic}, {AclResult, Timestamp}}) ->
    #{access => PubSub,
      topic => Topic,
      result => AclResult,
      updated_time => Timestamp}.

%%--------------------------------------------------------------------
%% Query Functions
%%--------------------------------------------------------------------

query({Qs, Fuzzy}, Start, Limit) ->
    case qs2ms(Qs) of
        {Ms, []} when Fuzzy =:= [] ->
            emqx_mgmt_api:select_table(emqx_channel_info, Ms, Start, Limit, fun format_channel_info/1);
        {Ms, FuzzyStats} ->
            MatchFun = match_fun(Ms, Fuzzy ++ FuzzyStats),
            emqx_mgmt_api:traverse_table(emqx_channel_info, MatchFun, Start, Limit, fun format_channel_info/1)
    end.

%%--------------------------------------------------------------------
%% Match funcs

match_fun(Ms, Fuzzy) ->
    MsC = ets:match_spec_compile(Ms),
    fun(Rows) ->
         case ets:match_spec_run(Rows, MsC) of
             [] -> [];
             Ls ->
                 lists:filter(fun(E) ->
                    run_fuzzy_match(E, Fuzzy)
                 end, Ls)
         end
    end.

run_fuzzy_match(_, []) ->
    true;
run_fuzzy_match(E = {_, #{clientinfo := ClientInfo}, _}, [{Key, like, SubStr} | Fuzzy]) ->
    Val = case maps:get(Key, ClientInfo, undefined) of
              undefined -> <<>>;
              V -> V
          end,
    binary:match(Val, SubStr) /= nomatch andalso run_fuzzy_match(E, Fuzzy);
run_fuzzy_match(E = {_, _, Stats}, [{Key, '>=', Int}|Fuzzy]) ->
    case lists:keyfind(Key, 1, Stats) of
        {_, Val} when Val >= Int -> run_fuzzy_match(E, Fuzzy);
        _ -> false
    end;
run_fuzzy_match(E = {_, _, Stats}, [{Key, '=<', Int}|Fuzzy]) ->
    case lists:keyfind(Key, 1, Stats) of
        {_, Val} when Val =< Int -> run_fuzzy_match(E, Fuzzy);
        _ -> false
    end.

%%--------------------------------------------------------------------
%% QueryString to Match Spec

-spec qs2ms(list()) -> {ets:match_spec(), [{_Key, _Symbol, _Val}]}.
qs2ms(Qs) ->
    {MatchHead, Conds, FuzzyStats} = qs2ms(Qs, 2, #{}, [], []),
    {[{{'$1', MatchHead, '_'}, Conds, ['$_']}], FuzzyStats}.

qs2ms([], _, MatchHead, Conds, FuzzyStats) ->
    {MatchHead, lists:reverse(Conds), FuzzyStats};

qs2ms([{Key, '=:=', Value} | Rest], N, MatchHead, Conds, FuzzyStats) ->
    NMatchHead = emqx_mgmt_util:merge_maps(MatchHead, ms(Key, Value)),
    qs2ms(Rest, N, NMatchHead, Conds, FuzzyStats);
qs2ms([Qs | Rest], N, MatchHead, Conds, FuzzyStats) ->
    Holder = binary_to_atom(iolist_to_binary(["$", integer_to_list(N)]), utf8),
    case ms(element(1, Qs), Holder) of
        fuzzy_stats ->
            FuzzyStats1 =
                case Qs of
                    {_Key, _Symbol, _Val} -> [Qs | FuzzyStats];
                    {Key, Symbol1, Val1, Symbol2, Val2} ->
                        [{Key, Symbol1, Val1}, {Key, Symbol2, Val2} | FuzzyStats]
                end,
            qs2ms(Rest, N, MatchHead, Conds, FuzzyStats1);
        Ms ->
            NMatchHead = emqx_mgmt_util:merge_maps(MatchHead, Ms),
            NConds = put_conds(Qs, Holder, Conds),
            qs2ms(Rest, N+1, NMatchHead, NConds, FuzzyStats)
    end.

put_conds({_, Op, V}, Holder, Conds) ->
    [{Op, Holder, V} | Conds];
put_conds({_, Op1, V1, Op2, V2}, Holder, Conds) ->
    [{Op2, Holder, V2},
     {Op1, Holder, V1} | Conds].

ms(clientid, X) ->
    #{clientinfo => #{clientid => X}};
ms(username, X) ->
    #{clientinfo => #{username => X}};
ms(zone, X) ->
    #{clientinfo => #{zone => X}};
ms(ip_address, X) ->
    #{clientinfo => #{peerhost => X}};
ms(conn_state, X) ->
    #{conn_state => X};
ms(clean_start, X) ->
    #{conninfo => #{clean_start => X}};
ms(proto_name, X) ->
    #{conninfo => #{proto_name => X}};
ms(proto_ver, X) ->
    #{conninfo => #{proto_ver => X}};
ms(connected_at, X) ->
    #{conninfo => #{connected_at => X}};
ms(created_at, X) ->
    #{session => #{created_at => X}};
ms(mqueue_len, _X) ->
    fuzzy_stats;
ms(mqueue_dropped, _X) ->
    fuzzy_stats.

filter_ratelimit_params(P) ->
    [{K, parse_ratelimit_str(V)} || {K, V} <- P, V =/= undefined].

%%--------------------------------------------------------------------
%% EUnits
%%--------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

params2qs_test() ->
    QsSchema = element(2, ?CLIENT_QS_SCHEMA),
    Params = [{<<"clientid">>, <<"abc">>},
              {<<"username">>, <<"def">>},
              {<<"zone">>, <<"external">>},
              {<<"ip_address">>, <<"127.0.0.1">>},
              {<<"conn_state">>, <<"connected">>},
              {<<"clean_start">>, true},
              {<<"proto_name">>, <<"MQTT">>},
              {<<"proto_ver">>, 4},
              {<<"_gte_created_at">>, 1},
              {<<"_lte_created_at">>, 5},
              {<<"_gte_connected_at">>, 1},
              {<<"_lte_connected_at">>, 5},
              {<<"_lte_mqueue_len">>, 10},
              {<<"_gte_mqueue_len">>, 5},
              {<<"_lte_mqueue_dropped">>, 100},
              {<<"_gte_mqueue_dropped">>, 50},
              {<<"_like_clientid">>, <<"a">>},
              {<<"_like_username">>, <<"e">>}
             ],
    ExpectedMtchHead =
        #{clientinfo => #{clientid => <<"abc">>,
                          username => <<"def">>,
                          zone => external,
                          peerhost => {127,0,0,1}
                         },
          conn_state => connected,
          conninfo => #{clean_start => true,
                        proto_name => <<"MQTT">>,
                        proto_ver => 4,
                        connected_at => '$3'},
          session => #{created_at => '$2'}},
    ExpectedCondi = [{'>=','$2', 1000},
                     {'=<','$2', 5000},
                     {'>=','$3', 1000},
                     {'=<','$3', 5000}],
    ExpectedFuzzyStats = [{mqueue_dropped,'=<',100},
                          {mqueue_dropped,'>=',50},
                          {mqueue_len,'=<',10},
                          {mqueue_len,'>=',5}],
    {12, {Qs1, []}} = emqx_mgmt_api:params2qs(Params, QsSchema),
    {[{{'$1', MtchHead, _}, Condi, _}], FuzzyStats} = qs2ms(Qs1),
    ?assertEqual(ExpectedMtchHead, MtchHead),
    ?assertEqual(ExpectedCondi, Condi),
    ?assertEqual(ExpectedFuzzyStats, lists:sort(FuzzyStats)),

    {[{{'$1', #{}, '_'}, [], ['$_']}], []} = qs2ms([]).

fuzzy_match_test() ->
    Info = {emqx_channel_info,
            #{clientinfo =>
              #{ clientid => <<"abcde">>
               , username => <<"abc\\name*[]()">>
               }}, []
           },
    true = run_fuzzy_match(Info, [{clientid, like, <<"abcde">>}]),
    true = run_fuzzy_match(Info, [{clientid, like, <<"bcd">>}]),
    false = run_fuzzy_match(Info, [{clientid, like, <<"defh">>}]),

    true = run_fuzzy_match(Info, [{username, like, <<"\\name">>}]),
    true = run_fuzzy_match(Info, [{username, like, <<"*">>}]),
    true = run_fuzzy_match(Info, [{username, like, <<"[]">>}]),
    true = run_fuzzy_match(Info, [{username, like, <<"()">>}]),
    false = run_fuzzy_match(Info, [{username, like, <<"))">>}]),

    true = run_fuzzy_match(Info, [{clientid, like, <<"de">>},
                                  {username, like, <<"[]">>}]).

fuzzy_stats_test() ->
    Fun = fun(Len, Dropped) ->
        {emqx_channel_info,
            #{clientinfo =>
            #{ clientid => <<"abcde">>, username => <<"abc\\name*[]()">>}},
            [{mqueue_len, Len}, {mqueue_max,1000}, {mqueue_dropped, Dropped}]
        }
          end,
    false = run_fuzzy_match(Fun(0, 100), [{mqueue_len, '>=', 1}]),
    true = run_fuzzy_match(Fun(1, 100), [{mqueue_len, '>=', 1}]),
    false = run_fuzzy_match(Fun(1, 100), [{mqueue_len, '>=', 2}]),
    true = run_fuzzy_match(Fun(99, 100), [{mqueue_len, '=<', 100}, {mqueue_len, '>=', 98}]),
    false = run_fuzzy_match(Fun(99, 100), [{mqueue_len, '=<', 101}, {mqueue_len, '>=', 100}]),

    false = run_fuzzy_match(Fun(1000, 0), [{mqueue_dropped, '>=', 1}]),
    true = run_fuzzy_match(Fun(1000, 1), [{mqueue_dropped, '>=', 1}]),
    false = run_fuzzy_match(Fun(1000, 1), [{mqueue_dropped, '>=', 2}]),
    true = run_fuzzy_match(Fun(1000, 99), [{mqueue_dropped, '=<', 100}, {mqueue_dropped, '>=', 98}]),
    false = run_fuzzy_match(Fun(1000, 99), [{mqueue_dropped, '=<', 98}, {mqueue_dropped, '>=', 97}]),
    false = run_fuzzy_match(Fun(1000, 102), [{mqueue_dropped, '=<', 104}, {mqueue_dropped, '>=', 103}]),

    true = run_fuzzy_match(Fun(1000, 103), [{mqueue_dropped, '=<', 104}, {mqueue_dropped, '>=', 103},
        {mqueue_len, '>=', 1000}]),
    false = run_fuzzy_match(Fun(1000, 199), [{mqueue_dropped, '>=', 198},
        {mqueue_len, '=<', 99}, {mqueue_len, '>=', 1}]),
    ok.

-endif.
