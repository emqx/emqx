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

-module(emqx_mgmt_api_clients).

-include("emqx_mgmt.hrl").

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx.hrl").

-import(proplists, [get_value/2]).

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
        ]).

-export([ query/3
        , format_channel_info/1
        ]).

-define(query_fun, {?MODULE, query}).
-define(format_fun, {?MODULE, format_channel_info}).

list(Bindings, Params) when map_size(Bindings) == 0 ->
    minirest:return({ok, emqx_mgmt_api:cluster_query(Params, ?CLIENT_QS_SCHEMA, ?query_fun)});

list(#{node := Node}, Params) when Node =:= node() ->
    minirest:return({ok, emqx_mgmt_api:node_query(Node, Params, ?CLIENT_QS_SCHEMA, ?query_fun)});

list(Bindings = #{node := Node}, Params) ->
    case rpc:call(Node, ?MODULE, list, [Bindings, Params]) of
        {badrpc, Reason} -> minirest:return({error, ?ERROR1, Reason});
        Res -> Res
    end.

lookup(#{node := Node, clientid := ClientId}, _Params) ->
    minirest:return({ok, emqx_mgmt:lookup_client(Node, {clientid, emqx_mgmt_util:urldecode(ClientId)}, ?format_fun)});

lookup(#{clientid := ClientId}, _Params) ->
    minirest:return({ok, emqx_mgmt:lookup_client({clientid, emqx_mgmt_util:urldecode(ClientId)}, ?format_fun)});

lookup(#{node := Node, username := Username}, _Params) ->
    minirest:return({ok, emqx_mgmt:lookup_client(Node, {username, emqx_mgmt_util:urldecode(Username)}, ?format_fun)});

lookup(#{username := Username}, _Params) ->
    minirest:return({ok, emqx_mgmt:lookup_client({username, emqx_mgmt_util:urldecode(Username)}, ?format_fun)}).

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
    P = [{conn_bytes_in, get_value(<<"conn_bytes_in">>, Params)},
         {conn_messages_in, get_value(<<"conn_messages_in">>, Params)}],
    case [{K, parse_ratelimit_str(V)} || {K, V} <- P, V =/= undefined] of
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
    P = [{conn_messages_routing, get_value(<<"conn_messages_routing">>, Params)}],
    case [{K, parse_ratelimit_str(V)} || {K, V} <- P, V =/= undefined] of
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
                  _Sess -> _Sess
              end,
    SessCreated = maps:get(created_at, Session, maps:get(connected_at, ConnInfo)),
    Connected = case maps:get(conn_state, Info, connected) of
                    connected -> true;
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
                             recv_cnt, recv_msg, recv_oct, recv_pkt, send_cnt,
                             send_msg, send_oct, send_pkt], NStats),
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
                   DisconnectedAt -> #{disconnected_at => iolist_to_binary(strftime(DisconnectedAt div 1000))}
               end).

format_acl_cache({{PubSub, Topic}, {AclResult, Timestamp}}) ->
    #{access => PubSub,
      topic => Topic,
      result => AclResult,
      updated_time => Timestamp}.

%%--------------------------------------------------------------------
%% Query Functions
%%--------------------------------------------------------------------

query({Qs, []}, Start, Limit) ->
    Ms = qs2ms(Qs),
    emqx_mgmt_api:select_table(emqx_channel_info, Ms, Start, Limit, fun format_channel_info/1);

query({Qs, Fuzzy}, Start, Limit) ->
    Ms = qs2ms(Qs),
    MatchFun = match_fun(Ms, Fuzzy),
    emqx_mgmt_api:traverse_table(emqx_channel_info, MatchFun, Start, Limit, fun format_channel_info/1).

%%--------------------------------------------------------------------
%% Match funcs

match_fun(Ms, Fuzzy) ->
    MsC = ets:match_spec_compile(Ms),
    REFuzzy = lists:map(fun({K, like, S}) ->
                  {ok, RE} = re:compile(S),
                  {K, like, RE}
              end, Fuzzy),
    fun(Rows) ->
         case ets:match_spec_run(Rows, MsC) of
             [] -> [];
             Ls ->
                 lists:filter(fun(E) ->
                    run_fuzzy_match(E, REFuzzy)
                 end, Ls)
         end
    end.

run_fuzzy_match(_, []) ->
    true;
run_fuzzy_match(E = {_, #{clientinfo := ClientInfo}, _}, [{Key, _, RE}|Fuzzy]) ->
    Val = case maps:get(Key, ClientInfo, "") of
              undefined -> "";
              V -> V
          end,
    re:run(Val, RE, [{capture, none}]) == match andalso run_fuzzy_match(E, Fuzzy).

%%--------------------------------------------------------------------
%% QueryString to Match Spec

-spec qs2ms(list()) -> ets:match_spec().
qs2ms(Qs) ->
    {MtchHead, Conds} = qs2ms(Qs, 2, {#{}, []}),
    [{{'$1', MtchHead, '_'}, Conds, ['$_']}].

qs2ms([], _, {MtchHead, Conds}) ->
    {MtchHead, lists:reverse(Conds)};

qs2ms([{Key, '=:=', Value} | Rest], N, {MtchHead, Conds}) ->
    NMtchHead = emqx_mgmt_util:merge_maps(MtchHead, ms(Key, Value)),
    qs2ms(Rest, N, {NMtchHead, Conds});
qs2ms([Qs | Rest], N, {MtchHead, Conds}) ->
    Holder = binary_to_atom(iolist_to_binary(["$", integer_to_list(N)]), utf8),
    NMtchHead = emqx_mgmt_util:merge_maps(MtchHead, ms(element(1, Qs), Holder)),
    NConds = put_conds(Qs, Holder, Conds),
    qs2ms(Rest, N+1, {NMtchHead, NConds}).

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
    #{session => #{created_at => X}}.

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
    ExpectedCondi = [{'>=','$2', 1},
                     {'=<','$2', 5},
                     {'>=','$3', 1},
                     {'=<','$3', 5}],
    {10, {Qs1, []}} = emqx_mgmt_api:params2qs(Params, QsSchema),
    [{{'$1', MtchHead, _}, Condi, _}] = qs2ms(Qs1),
    ?assertEqual(ExpectedMtchHead, MtchHead),
    ?assertEqual(ExpectedCondi, Condi),

    [{{'$1', #{}, '_'}, [], ['$_']}] = qs2ms([]).

-endif.
