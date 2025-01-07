%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_API_LIB_HRL).
-define(EMQX_API_LIB_HRL, true).

-define(ERROR_MSG(CODE, REASON), #{code => CODE, message => emqx_utils:readable_error_msg(REASON)}).

-define(OK(CONTENT), {200, CONTENT}).

-define(CREATED(CONTENT), {201, CONTENT}).

-define(NO_CONTENT, 204).

-define(BAD_REQUEST(CODE, REASON), {400, ?ERROR_MSG(CODE, REASON)}).
-define(BAD_REQUEST(REASON), ?BAD_REQUEST('BAD_REQUEST', REASON)).

-define(NOT_FOUND(REASON), {404, ?ERROR_MSG('NOT_FOUND', REASON)}).

-define(METHOD_NOT_ALLOWED, 405).

-define(INTERNAL_ERROR(REASON), {500, ?ERROR_MSG('INTERNAL_ERROR', REASON)}).

-define(NOT_IMPLEMENTED, 501).

-define(SERVICE_UNAVAILABLE(REASON), {503, ?ERROR_MSG('SERVICE_UNAVAILABLE', REASON)}).
-endif.
