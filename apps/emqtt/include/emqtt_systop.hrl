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
%%% emqtt system topics.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-define(SYSTOP, <<"$SYS">>).

%%------------------------------------------------------------------------------
%% $SYS Topics of Broker
%%------------------------------------------------------------------------------
-define(SYSTOP_BROKERS, [
    version,        % Broker version
    uptime,         % Broker uptime
    timestamp,      % Broker timestamp
    description     % Broker description
]).

%%------------------------------------------------------------------------------
%% $SYS Topics of Clients
%%------------------------------------------------------------------------------
-define(SYSTOP_CLIENTS, [
    'clients/connected',    % ???
    'clients/disconnected', % ???
    'clients/total',        % total clients connected current
    'clients/max'           % max clients connected
]).

%%------------------------------------------------------------------------------
%% $SYS Topics of Subscribers
%%------------------------------------------------------------------------------
-define(SYSTOP_SUBSCRIBERS, [
    'subscribers/total',   % ...
    'subscribers/max'      % ...
]).  

%%------------------------------------------------------------------------------
%% Bytes sent and received of Broker
%%------------------------------------------------------------------------------
-define(SYSTOP_BYTES, [
    'bytes/received',   % Total bytes received
    'bytes/sent'        % Total bytes sent
]).

%%------------------------------------------------------------------------------
%% Packets sent and received of Broker
%%------------------------------------------------------------------------------
-define(SYSTOP_PACKETS, [
    'packets/received',         % All Packets received
    'packets/sent',             % All Packets sent
    'packets/connect',          % CONNECT Packets received
    'packets/connack',          % CONNACK Packets sent
    'packets/publish/received', % PUBLISH packets received
    'packets/publish/sent',     % PUBLISH packets sent
    'packets/subscribe',        % SUBSCRIBE Packets received 
    'packets/suback',           % SUBACK packets sent 
    'packets/unsubscribe',      % UNSUBSCRIBE Packets received
    'packets/unsuback',         % UNSUBACK Packets sent
    'packets/pingreq',          % PINGREQ packets received
    'packets/pingresp',         % PINGRESP Packets sent
    'packets/disconnect'        % DISCONNECT Packets received 
]).

%%------------------------------------------------------------------------------
%% Messages sent and received of broker
%%------------------------------------------------------------------------------
-define(SYSTOP_MESSAGES, [
    'messages/received',    % Messages received
    'messages/sent',        % Messages sent
    'messages/retained',    % Messagea retained
    'messages/stored',      % Messages stored
    'messages/dropped'      % Messages dropped
]).


