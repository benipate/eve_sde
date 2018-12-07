-module(eve_sde).

%% api for sde_server

-export([start/0, start/1]).
-export([stop/0, stop/1]).

-export([take_table/1, take_table/2, give_table/1, give_table/2, give_table/3]).
-export([return_table/1, return_table/2]).
-export([get/1, get/2]).

-define(DEFAULT_SDE_SERVER_NAME, test_eve_sde).
-define(SDE_SERVER_NAME, application:get_env(eve_sde, server_name ,?DEFAULT_SDE_SERVER_NAME)).


start()->
  start(?SDE_SERVER_NAME).
start(Name)->
  sde_server:start_link(Name).

stop()->
  stop(?SDE_SERVER_NAME).
stop(Name)->
  gen_server:stop(Name).

%% if you want to take table to custom proces u also can call give_table/2 and handle info message in your server

take_table(TableName)->
  take_table(?SDE_SERVER_NAME, TableName).
take_table(Name, TableName)->
  give_table(Name, TableName),
  receive
    {'ETS-TRANSFER',Tab,FromPid,GiftData}->
      GiftData
  after
    10000 ->
      timeout
  end.

give_table(TableName)->
  give_table(?SDE_SERVER_NAME, TableName).
give_table(Name, TableName) when is_atom(TableName)->
  give_table(Name, TableName, self());
give_table(TableName, PID)->
  give_table(?SDE_SERVER_NAME, TableName, PID).
give_table(Name, TableName, PID)->
  gen_server:cast(Name, {give, TableName, PID}).

return_table(TableName)->
  return_table(?SDE_SERVER_NAME, TableName).
return_table(Name, TableName)->
  PID = whereis(Name),
  true = ets:give_away(TableName, PID, #{from => self()}).

get(Req)->
  get(?SDE_SERVER_NAME, Req).
get(ServerName, Req)->
  gen_server:call(ServerName, {get, Req}).
