-module(sde_server).

-behaviour(gen_server).

-export([start_link/1]).
%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(Name) when is_atom(Name) ->
  gen_server:start_link({local, Name}, ?MODULE, Name, []).
%%====================================================================
%% gen_server callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init(Name) ->
  {ok, true} = application:get_env(Name, use_as_sde_server_config),
  {ok, Root} = application:get_env(Name, root),
  {ok, Options} = application:get_env(Name, options),
  {ok, _Tables} = application:get_env(Name, tables),
  Tables = lists:foldr(
    fun(_TableSpec, Acc)->
      #{ ets_file := TableFile, table := #{ name := TableName}=TableNameMap}=TableSpec=normalize_table_map(_TableSpec),
      TablePath = filename:join(Root,TableFile),
      case sde:load(TablePath) of
        {error, {read_error,  {file_error,_TableFile,  enoent}}}->
          sde:create_table(Root, Options, TableSpec),
          Acc#{TableName => TableSpec#{new => true}};
        {error, Reason}->
          Acc;
        Table -> %% loaded from file, noneed to parse yaml
          Acc#{Table => TableSpec#{table => TableNameMap#{ name => Table}}}
    end end, #{}, _Tables),
    maps:map(fun
      (TableName,#{final_fun := FinalFun, new := true,
            ets_file := TableFile})->
        sde:post_process(TableName, FinalFun),
        ets:tab2file(TableName, filename:join(Root,TableFile), [{sync, true}]);
      (TableName,_Map)->
        ok
       end, Tables),
  %%Files = filelib:wildcard(filename:join(Root, "*.parsed")),
  %%Tables = lists:map(fun(File)->sde:load(File) end, Files),
  {ok, #{tables => Tables, options => Options, root => Root}}.
%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({get, Key}, _From, State) ->
  Reply = maps:get(Key, State, undefined),
  {reply, Reply, State};
handle_call(_Request, _From, State) ->
  {reply, undefined, State}.
%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast({give, TableName, PID}, #{tables := Tables}=State) ->
  case maps:get(TableName, Tables, undefined) of
    undefined->  %% invalid table_name
      PID ! {'ETS-TRANSFER-ERROR', {invalid_tablename, TableName}},
      {noreply, State};
    #{table := #{name := TableName}=TableNameMap}=TableMap->
      case maps:get(owner, TableNameMap, undefined) of
        undefined-> %% this process is table owner
          true = ets:give_away(TableName, PID, #{from => self()}),
          {noreply, State#{TableName => TableMap#{table => TableNameMap#{ owner => PID}}}};
        CurrentPID->
          if
            CurrentPID =:= PID -> %% already taken by right pid
              PID ! {'ETS-TRANSFER-ERROR', {already_taken, TableName}};
            true->  %% already taken by another pid
              PID ! {'ETS-TRANSFER-ERROR', {already_taken, TableName, CurrentPID}}
          end,
          {noreply, State}
      end
  end;
handle_cast(_Msg, State) ->
  {noreply, State}.
%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({'ETS-TRANSFER',TableName,PID,GiftData}, #{tables := Tables}=State) ->
  case maps:get(TableName, Tables, undefined) of
    undefined->  %% invalid table_name
      error_logger:error_msg("Invalid table ~p from ~p", [TableName, PID]),
      PID ! {'ETS-TRANSFER-ERROR', {invalid_tablename, TableName}},
      {noreply, State};
    #{table := #{ name := TableName}=TableNameMap}=TableMap->
      error_logger:info_msg("Got ~p table back from ~p", [TableName, PID]),
      {noreply, State#{TableName => TableMap#{table => maps:remove(owner, TableNameMap)}}}
  end;
handle_info(_Info, State) ->
  {noreply, State}.
%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.
%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------



normalize_table_map(#{yaml_file := _FilePath,
      ets_file := _TableFile,
      table := #{ name := _TableName, options := _ETSOptions},
      type := _Type,
      post_fun := _Fun,
      spec := _Spec}=TableMap)->
        TableMap;
normalize_table_map(#{yaml_file := FilePath,
      type := Type,
      spec := Spec}=TableMap)->
  TableFile = maps:get(ets_file, TableMap, filename:basename(FilePath, ".yaml")),
  Fun = maps:get(post_fun, TableMap, undefined),
  FinalFun = maps:get(final_fun, TableMap, undefined),
  TableData = case maps:get(table, TableMap, undefined) of
    undefined->
      #{ name => list_to_atom(TableFile), options => []};
    _Map->
      TableName = maps:get(name, _Map, list_to_atom(TableFile)),
      ETSOptions = maps:get(options, _Map, []),
      #{ name => TableName, options => ETSOptions}
  end,
  TableMap#{ets_file => TableFile, table =>  TableData, post_fun => Fun, final_fun => FinalFun}.
