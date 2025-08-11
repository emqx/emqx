%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% NOTE: this is the release version which is not always the same
%% as the emqx app version defined in emqx.app.src
%% App (plugin) versions are bumped independently.
%% e.g. EMQX_RELEASE_EE being 5.9.0 does not always imply emqx app
%% should be 5.9.0, as it might be the case that only one of the
%% plugins had a bug to fix. So for a hot beam upgrade, only the app
%% with beam files changed needs an upgrade.

%% NOTE: This version number should be manually bumped for each release

%% NOTE: This version number should have 3 numeric parts
%% (Major.Minor.Patch), and extra info can be added after a final
%% hyphen.

%% NOTE: Also make sure to follow the instructions in end of
%% `apps/emqx/src/bpapi/README.md'

-define(EMQX_RELEASE_EE, "5.10.1-beta.1").
