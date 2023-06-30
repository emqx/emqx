%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_RELEASE_HRL).
-define(EMQX_RELEASE_HRL, true).

%% NOTE: this is the release version which is not always the same
%% as the emqx app version defined in emqx.app.src
%% App (plugin) versions are bumped independently.
%% e.g. EMQX_RELEASE being 4.3.1 does no always imply emqx app
%% should be 4.3.1, as it might be the case that only one of the
%% plugins had a bug to fix. So for a hot beam upgrade, only the app
%% with beam files changed needs an upgrade.

%% NOTE: This version number should be manually bumped for each release

-ifndef(EMQX_ENTERPRISE).

-define(EMQX_RELEASE, {opensource, "4.4.19"}).

-else.

-endif.

-endif.
