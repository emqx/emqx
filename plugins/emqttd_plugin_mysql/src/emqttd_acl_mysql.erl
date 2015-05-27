%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd demo acl module.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_acl_mysql).

-include("emqttd.hrl").

-behaviour(emqttd_acl_mod).

%% ACL callbacks
-export([init/1, check_acl/2, reload_acl/1, description/0]).
-record(state, {user_table, acl_table, acl_username_field, acl_topic_field, acl_rw_field, user_name_field, user_super_field}).

init(Opts) ->
  Mapper = proplists:get_value(field_mapper, Opts),
  State =
    #state{
      user_table = proplists:get_value(users_table, Opts, auth_user),
      user_super_field = proplists:get_value(is_super, Mapper, is_superuser),
      user_name_field = proplists:get_value(username, Mapper, username),
      acl_table = proplists:get_value(acls_table, Opts, auth_acl),
      acl_username_field = proplists:get_value(acl_username, Mapper, username),
      acl_rw_field = proplists:get_value(acl_rw, Mapper, rw),
      acl_topic_field = proplists:get_value(acl_topic, Mapper, topic)
    },
  {ok, State}.

check_acl({#mqtt_client{username = Username}, PubSub, Topic}, #state{user_table = UserTab, acl_table = AclTab, user_name_field = UsernameField, user_super_field = SuperField, acl_topic_field = TopicField, acl_username_field = AclUserField, acl_rw_field = AclRwField}) ->
  Flag = case PubSub of publish -> 2; subscribe -> 1; pubsub -> 2 end,
  Where = {'and', {'>=', AclRwField, Flag}, {TopicField, Topic}},
  Where1 = {'or', {AclUserField, Username}, {AclUserField, "*"}},
  Where2 = {'and', Where, Where1},
  case emysql:select(UserTab, {'and', {UsernameField, Username}, {SuperField, 1}}) of
    {ok, []} ->
      case emysql:select(UserTab, {UsernameField, Username}) of
        {ok, []} -> ignore;
        {ok, _} -> case emysql:select(AclTab, Where2) of
                     {ok, []} -> deny;
                     {ok, _Record} -> allow
                   end
      end;
    {ok, _} -> allow
  end.

reload_acl(_State) -> ok.

description() -> "ACL Module by Mysql".
