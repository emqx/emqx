%%--------------------------------------------------------------------
%% Copyright (c) 2018-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% debug | info | notice | warning | error | critical | alert | emergency
-define(DEBUG(Format), ?LOG(debug, Format, [])).
-define(DEBUG(Format, Args), ?LOG(debug, Format, Args)).

-define(INFO(Format), ?LOG(info, Format, [])).
-define(INFO(Format, Args), ?LOG(info, Format, Args)).

-define(NOTICE(Format), ?LOG(notice, Format, [])).
-define(NOTICE(Format, Args), ?LOG(notice, Format, Args)).

-define(WARN(Format), ?LOG(warning, Format, [])).
-define(WARN(Format, Args), ?LOG(warning, Format, Args)).

-define(ERROR(Format), ?LOG(error, Format, [])).
-define(ERROR(Format, Args), ?LOG(error, Format, Args)).

-define(CRITICAL(Format), ?LOG(critical, Format, [])).
-define(CRITICAL(Format, Args), ?LOG(critical, Format, Args)).

-define(ALERT(Format), ?LOG(alert, Format, [])).
-define(ALERT(Format, Args), ?LOG(alert, Format, Args)).

-define(LOG(Level, Format), ?LOG(Level, Format, [])).

%% deprecated
-define(LOG(Level, Format, Args, Meta),
        %% check 'allow' here so we do not have to pass an anonymous function
        %% down to logger which may cause `badfun` exception during upgrade
        case logger:allow(Level, ?MODULE) of
            true ->
                logger:log(Level, (Format), (Args),
                           (Meta)#{ mfa => {?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY}
                                  , line => ?LINE
                                  });
            false ->
                ok
        end).

-define(LOG(Level, Format, Args), ?LOG(Level, Format, Args, #{})).

%% structured logging
-define(SLOG(Level, Data),
        ?SLOG(Level, Data, #{})).

%% structured logging, meta is for handler's filter.
-define(SLOG(Level, Data, Meta),
%% check 'allow' here, only evaluate Data when necessary
    case logger:allow(Level, ?MODULE) of
        true ->
            logger:log(Level, (Data), Meta#{ mfa => {?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY}
                , line => ?LINE
            });
        false ->
            ok
    end).

-define(TRACE(Event, Msg, Meta), emqx_trace:log(Event, Msg, Meta)).

%% print to 'user' group leader
-define(ULOG(Fmt, Args), io:format(user, Fmt, Args)).
-define(ELOG(Fmt, Args), io:format(standard_error, Fmt, Args)).

-endif.
