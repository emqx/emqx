%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% NOTE: this is the release version which is not always the same
%% as the emqx app version defined in emqx.app.src
%% App (plugin) versions are bumped independently.
%% e.g. EMQX_RELEASE_CE being 4.3.1 does no always imply emqx app
%% should be 4.3.1, as it might be the case that only one of the
%% plugins had a bug to fix. So for a hot beam upgrade, only the app
%% with beam files changed needs an upgrade.

%% NOTE: This version number should be manually bumped for each release

%% NOTE: This version number should have 3 numeric parts
%% (Major.Minor.Patch), and extra info can be added after a final
%% hyphen.

%% NOTE: ALso make sure to follow the instructions in end of
%% `apps/emqx/src/bpapi/README.md'

%% Community edition
-define(EMQX_RELEASE_CE, "5.0.12").

%% Enterprise edition
-define(EMQX_RELEASE_EE, "5.0.0-beta.6").

%% the HTTP API version
-define(EMQX_API_VERSION, "5.0").
