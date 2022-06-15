%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rewrite).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-ifdef(TEST).
-export([
    compile/1,
    match_and_rewrite/3
]).
-endif.

%% APIs
-export([
    rewrite_subscribe/4,
    rewrite_unsubscribe/4,
    rewrite_publish/2
]).

-export([
    enable/0,
    disable/0
]).

-export([
    list/0,
    update/1,
    post_config_update/5
]).

%% exported for `emqx_telemetry'
-export([get_basic_usage_info/0]).

%%--------------------------------------------------------------------
%% Load/Unload
%%--------------------------------------------------------------------

enable() ->
    emqx_conf:add_handler([rewrite], ?MODULE),
    Rules = emqx_conf:get([rewrite], []),
    register_hook(Rules).

disable() ->
    emqx_conf:remove_handler([rewrite]),
    unregister_hook(),
    ok.

list() ->
    emqx_conf:get_raw([<<"rewrite">>], []).

update(Rules0) ->
    {ok, _} = emqx_conf:update([rewrite], Rules0, #{override_to => cluster}),
    ok.

post_config_update(_KeyPath, _Config, Rules, _OldConf, _AppEnvs) ->
    register_hook(Rules).

register_hook([]) ->
    unregister_hook();
register_hook(Rules) ->
    {PubRules, SubRules, ErrRules} = compile(Rules),
    emqx_hooks:put('client.subscribe', {?MODULE, rewrite_subscribe, [SubRules]}, ?HP_REWRITE),
    emqx_hooks:put('client.unsubscribe', {?MODULE, rewrite_unsubscribe, [SubRules]}, ?HP_REWRITE),
    emqx_hooks:put('message.publish', {?MODULE, rewrite_publish, [PubRules]}, ?HP_REWRITE),
    case ErrRules of
        [] ->
            ok;
        _ ->
            ?SLOG(error, #{rewrite_rule_re_complie_failed => ErrRules}),
            {error, ErrRules}
    end.

unregister_hook() ->
    emqx_hooks:del('client.subscribe', {?MODULE, rewrite_subscribe}),
    emqx_hooks:del('client.unsubscribe', {?MODULE, rewrite_unsubscribe}),
    emqx_hooks:del('message.publish', {?MODULE, rewrite_publish}).

rewrite_subscribe(ClientInfo, _Properties, TopicFilters, Rules) ->
    Binds = fill_client_binds(ClientInfo),
    {ok, [{match_and_rewrite(Topic, Rules, Binds), Opts} || {Topic, Opts} <- TopicFilters]}.

rewrite_unsubscribe(ClientInfo, _Properties, TopicFilters, Rules) ->
    Binds = fill_client_binds(ClientInfo),
    {ok, [{match_and_rewrite(Topic, Rules, Binds), Opts} || {Topic, Opts} <- TopicFilters]}.

rewrite_publish(Message = #message{topic = Topic}, Rules) ->
    Binds = fill_client_binds(Message),
    {ok, Message#message{topic = match_and_rewrite(Topic, Rules, Binds)}}.

%%--------------------------------------------------------------------
%% Telemetry
%%--------------------------------------------------------------------

-spec get_basic_usage_info() -> #{topic_rewrite_rule_count => non_neg_integer()}.
get_basic_usage_info() ->
    RewriteRules = list(),
    #{topic_rewrite_rule_count => length(RewriteRules)}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
compile(Rules) ->
    lists:foldl(
        fun(Rule, {Publish, Subscribe, Error}) ->
            #{source_topic := Topic, re := Re, dest_topic := Dest, action := Action} = Rule,
            case re:compile(Re) of
                {ok, MP} ->
                    case Action of
                        publish ->
                            {[{Topic, MP, Dest} | Publish], Subscribe, Error};
                        subscribe ->
                            {Publish, [{Topic, MP, Dest} | Subscribe], Error};
                        all ->
                            {[{Topic, MP, Dest} | Publish], [{Topic, MP, Dest} | Subscribe], Error}
                    end;
                {error, ErrSpec} ->
                    {Publish, Subscribe, [{Topic, Re, Dest, ErrSpec}]}
            end
        end,
        {[], [], []},
        Rules
    ).

match_and_rewrite(Topic, [], _) ->
    Topic;
match_and_rewrite(Topic, [{Filter, MP, Dest} | Rules], Binds) ->
    case emqx_topic:match(Topic, Filter) of
        true -> rewrite(Topic, MP, Dest, Binds);
        false -> match_and_rewrite(Topic, Rules, Binds)
    end.

rewrite(Topic, MP, Dest, Binds) ->
    case re:run(Topic, MP, [{capture, all_but_first, list}]) of
        {match, Captured} ->
            Vars = lists:zip(
                [
                    "\\$" ++ integer_to_list(I)
                 || I <- lists:seq(1, length(Captured))
                ],
                Captured
            ),
            iolist_to_binary(
                lists:foldl(
                    fun({Var, Val}, Acc) ->
                        re:replace(Acc, Var, Val, [global])
                    end,
                    Dest,
                    Binds ++ Vars
                )
            );
        nomatch ->
            Topic
    end.

fill_client_binds(#{clientid := ClientId, username := Username}) ->
    filter_client_binds([{"\\${clientid}", ClientId}, {"\\${username}", Username}]);
fill_client_binds(#message{from = ClientId, headers = Headers}) ->
    Username = maps:get(username, Headers, undefined),
    filter_client_binds([{"\\${clientid}", ClientId}, {"\\${username}", Username}]).

filter_client_binds(Binds) ->
    lists:filter(
        fun
            ({_, undefined}) -> false;
            ({_, <<"">>}) -> false;
            ({_, ""}) -> false;
            (_) -> true
        end,
        Binds
    ).
