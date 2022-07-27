%%--------------------------------------------------------------------
%% Copyright (c) 2018-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
                (Meta#{
                    mfa => {?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY},
                    line => ?LINE
                })
            );
        false ->
            ok
    end
).

-define(TRACE_FILTER, emqx_trace_filter).

-define(TRACE(Tag, Msg, Meta), ?TRACE(debug, Tag, Msg, Meta)).

%% Only evaluate when necessary
-define(TRACE(Level, Tag, Msg, Meta), begin
    case persistent_term:get(?TRACE_FILTER, []) of
        [] -> ok;
        %% We can't bind filter list to a variablebecause we pollute the calling scope with it.
        %% We also don't want to wrap the macro body in a fun
        %% beacause this adds overhead to the happy path.
        %% So evaluate `persistent_term:get` twice.
        _ -> emqx_trace:log(persistent_term:get(?TRACE_FILTER, []), Msg, (Meta)#{trace_tag => Tag})
    end,
    ?SLOG(
        Level,
        (emqx_trace_formatter:format_meta_map(Meta))#{msg => Msg, tag => Tag},
        #{is_trace => false}
    )
end).

%% print to 'user' group leader
-define(ULOG(Fmt, Args), io:format(user, Fmt, Args)).
-define(ELOG(Fmt, Args), io:format(standard_error, Fmt, Args)).

-endif.
