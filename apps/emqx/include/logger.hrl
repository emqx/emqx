%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_LOGGER_HRL).
-define(EMQX_LOGGER_HRL, true).

%% structured logging
-define(SLOG(Level, Data),
    ?SLOG(Level, Data, #{})
).

%% structured logging, meta is for handler's filter.
-define(SLOG(Level, Data, Meta),
    %% check 'allow' here, only evaluate Data and Meta when necessary
    case logger:allow(Level, ?MODULE) of
        true ->
            logger:log(
                Level,
                (Data),
                ?MAPPEND(Meta, #{
                    mfa => {?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY},
                    line => ?LINE
                })
            );
        false ->
            ok
    end
).

%% NOTE: do not forget to use atom for msg and add every used msg to
%% the default value of `log.throttling.msgs` list.
-define(SLOG_THROTTLE(Level, Data),
    ?SLOG_THROTTLE(Level, Data, #{})
).

-define(SLOG_THROTTLE(Level, Data, Meta),
    ?SLOG_THROTTLE(Level, undefined, Data, Meta)
).

-define(SLOG_THROTTLE(Level, UniqueKey, Data, Meta),
    case logger:allow(Level, ?MODULE) of
        true ->
            (fun(#{msg := __Msg} = __Data) ->
                case emqx_log_throttler:allow(__Msg, UniqueKey) of
                    true ->
                        logger:log(Level, __Data, Meta);
                    false ->
                        ?_DO_TRACE(Level, __Msg, maps:merge(__Data, Meta))
                end
            end)(
                Data
            );
        false ->
            ok
    end
).

-define(AUDIT_HANDLER, emqx_audit).
-define(TRACE_FILTER, emqx_trace_filter).
-define(OWN_KEYS, [level, filters, filter_default, handlers]).

%% Internal macro
-define(_DO_TRACE(Tag, Msg, Meta),
    case persistent_term:get(?TRACE_FILTER, []) of
        [] ->
            ok;
        %% We can't bind filter list to a variable because we pollute the calling scope with it.
        %% We also don't want to wrap the macro body in a fun
        %% because this adds overhead to the happy path.
        %% So evaluate `persistent_term:get` twice.
        _ ->
            emqx_trace:log(
                persistent_term:get(?TRACE_FILTER, []),
                Msg,
                ?MAPPEND(Meta, #{trace_tag => Tag})
            )
    end
).

-define(TRACE(Tag, Msg, Meta), ?TRACE(debug, Tag, Msg, Meta)).

%% Only evaluate when necessary
-define(TRACE(Level, Tag, Msg, Meta), begin
    ?_DO_TRACE(Tag, Msg, Meta),
    ?SLOG(
        Level,
        ?MAPPEND(Meta, #{msg => Msg, tag => Tag}),
        #{is_trace => false}
    )
end).

-ifdef(EMQX_RELEASE_EDITION).

-if(?EMQX_RELEASE_EDITION == ee).

-define(AUDIT(_LevelFun_, _MetaFun_), begin
    case logger_config:get(logger, ?AUDIT_HANDLER) of
        {error, {not_found, _}} ->
            ok;
        {ok, Handler = #{level := _AllowLevel_}} ->
            _Level_ = _LevelFun_,
            case logger:compare_levels(_AllowLevel_, _Level_) of
                _R_ when _R_ == lt; _R_ == eq ->
                    emqx_audit:log(_Level_, _MetaFun_, Handler);
                _ ->
                    ok
            end
    end
end).

-else.
%% Only for compile pass, ce edition will not call it
-define(AUDIT(_L_, _M_), _ = {_L_, _M_}).
-endif.

-else.
%% Only for compile pass, ce edition will not call it
-define(AUDIT(_L_, _M_), _ = {_L_, _M_}).
-endif.

%% print to 'user' group leader
-define(ULOG(Fmt, Args), io:format(user, Fmt, Args)).
-define(ELOG(Fmt, Args), io:format(standard_error, Fmt, Args)).

%% macro utilities

%% Append literal associations to a (meta) map, avoiding compliler warnings.
-define(MAPPEND(META, EXTRA),
    (begin
        META
    end)
        EXTRA
).

-endif.
