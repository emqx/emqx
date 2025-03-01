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

%% @doc
%% This module limits the rate of retained messages publishing
%% (deletion is a special case of publishing) using a simple token bucket algorithm.
%%
%% Each second at most `max_publish_rate` tokens are refilled. Each publish
%% consumes one token. If there are no tokens available, the publish is dropped.

-module(emqx_retainer_publisher).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    store_retained/1,
    delete_message/1
]).

-elvis([{elvis_style, invalid_dynamic_call, disable}]).

%%--------------------------------------------------------------------
%% Constants
%%--------------------------------------------------------------------

-define(LIMITER_ID, {?RETAINER_LIMITER_GROUP, ?PUBLISHER_LIMITER_NAME}).
%% 1MB
-define(DEF_MAX_PAYLOAD_SIZE, (1024 * 1024)).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec store_retained(emqx_types:message()) -> ok.
store_retained(#message{topic = Topic, payload = Payload} = Msg) ->
    Size = iolist_size(Payload),
    case payload_size_limit() of
        PayloadSizeLimit when PayloadSizeLimit > 0 andalso PayloadSizeLimit < Size ->
            ?SLOG_THROTTLE(warning, #{
                msg => retain_failed_for_payload_size_exceeded_limit,
                topic => Topic,
                config => "retainer.max_payload_size",
                size => Size,
                limit => PayloadSizeLimit
            }),
            ok;
        _ ->
            WithinLimits = with_limiter(fun() ->
                _ = emqx_retainer:with_backend(
                    fun(Mod, State) -> Mod:store_retained(State, Msg) end
                )
            end),
            case WithinLimits of
                true ->
                    ?tp(retain_within_limit, #{topic => Topic});
                {false, Reason} ->
                    ?tp(retain_failed_for_rate_exceeded_limit, #{topic => Topic}),
                    ?SLOG_THROTTLE(warning, #{
                        msg => retain_failed_for_rate_exceeded_limit,
                        topic => Topic,
                        config => "retainer.max_publish_rate",
                        reason => Reason
                    })
            end
    end.

-spec delete_message(binary()) -> ok.
delete_message(Topic) ->
    WithinLimits = with_limiter(fun() ->
        _ = emqx_retainer:with_backend(
            fun(Mod, State) -> Mod:delete_message(State, Topic) end
        )
    end),
    case WithinLimits of
        true ->
            ok;
        {false, Reason} ->
            ?tp(retained_delete_failed_for_rate_exceeded_limit, #{topic => Topic}),
            ?SLOG_THROTTLE(info, #{
                msg => retained_delete_failed_for_rate_exceeded_limit,
                topic => Topic,
                config => "retainer.max_publish_rate",
                reason => Reason
            })
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

with_limiter(Fun) ->
    Client = emqx_limiter:connect(?LIMITER_ID),
    case emqx_limiter_client:try_consume(Client, 1) of
        {true, _Client} ->
            Fun(),
            true;
        {false, _Client, Reason} ->
            {false, Reason}
    end.

payload_size_limit() ->
    emqx_conf:get([retainer, max_payload_size], ?DEF_MAX_PAYLOAD_SIZE).
