%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%%
%% The Initial Developer of the Original Code is ery.lee@gmail.com
%% Copyright (c) 2012 Ery Lee.  All rights reserved.
%%

%% -------------------------------------------
%% banner
%% -------------------------------------------

-record(internal_user, {username, passwdhash}).

%name: <<"a/b/c">>
%node: node()
%words: [<<"a">>, <<"b">>, <<"c">>]
-record(topic, {name, node}).

-record(trie, {edge, node_id}).

-record(trie_node, {node_id, edge_count=0, topic}).

-record(trie_edge, {node_id, word}).

%topic: topic name

-record(subscriber, {topic, qos, client}).

