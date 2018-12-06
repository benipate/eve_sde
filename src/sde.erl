-module(sde).


-export([load/1, create_table/3]).

%% Converter from CCP SDE yaml files to ets table(s)

load(File)->
  Res = ets:file2tab(File),
  case Res of
    {ok, Tab}->
      error_logger:info_msg("~s loaded from ~p",[File, Tab]),
      Tab;
    {error, Reason}->
      error_logger:info_msg("Error loading ~p as new table(~p)",[File, Reason]),
      {error, Reason}
  end.


create_table(Root, DecodeOpt, #{yaml_file := FilePath,
      ets_file := TableFile,
      table := #{ name := TableName, options := ETSOptions},
      type := Type,
      post_fun := Fun,
      spec := Spec})->
  TableName = create_table(Root, TableFile, TableName, ETSOptions),
  {ok,[List=[Element|_]]} = fast_yaml:decode_from_file(Root++FilePath, DecodeOpt),
  ActualFile = get_type(Element),
  if
    Type == ActualFile ->
      error_logger:info_msg("Loading ~s as ~p",[Root++FilePath,Type]),
      fill_table(Type, TableName, List, Spec),
      post_process(TableName, Fun),
      ets:tab2file(TableName, filename:join(Root,TableFile), [{sync, true}]);
    true ->
      error_logger:error_msg("~s should be ~p not ~p ",[Root++FilePath, Type, ActualFile])
  end.

post_process(TableName, {Mod,Fun,ArgList})->
  erlang:apply(Mod,Fun,[TableName|ArgList]);
post_process(_, undefined)->
  ok.

get_type(Element)->
  if
    is_tuple(Element) -> %% Unique key elements in list
      map;
    is_list(Element)-> %% keys duplicated, req key position in spec
      list
  end.

create_table(Root, FilePath, TableName, ETSOptions)->
  case ets:info(TableName) of
    undefined ->
      Res = ets:file2tab(filename:join(Root,FilePath), []),
      case Res of
        {ok, Tab}->
          error_logger:info_msg("Updating existing table(loaded from file): ~p",[TableName]),
          TableName;
        {error, Reason}->
           error_logger:info_msg("Creating new table(~p): ~p",[Reason,TableName]),
           TableName = ets:new(TableName, ETSOptions++[named_table])
      end;
    _-> %% table exist, posible problemm with protection options, but for now parsing done only by one PID
      error_logger:info_msg("Updating existing table: ~p",[TableName]),
      TableName
  end.

fill_table(list, Table, Data, {[TopKey|KeysList], MatchSpec})->
  N = length(KeysList),
  MaxRuns = length(Data),
  R = lists:foldr(fun(PropList,Acc)->
    TopKeyValue = proplists:get_value(TopKey, PropList),
    NewValue = to_map(proplists:delete(TopKey,PropList), KeysList),
    case proplists:get_value(TopKeyValue, Acc) of
      undefined ->
        [{TopKeyValue, NewValue}|Acc];
      CurrentValue->
        Updated = update_map(CurrentValue,NewValue,N),
        lists:keyreplace(TopKeyValue, 1, Acc, {TopKeyValue, Updated})
    end
  end, [], Data),
  OldSize = ets:info(Table, size),
  OldMemory = ets:info(Table, memory),
  lists:foreach(fun(X)-> ets:insert(Table, X)  end, ets:match_spec_run( R, ets:match_spec_compile(MatchSpec))),
  NewSize = ets:info(Table, size),
  NewMemory = ets:info(Table, memory),
  error_logger:info_msg("  ~p table updated:~n  ~p records (~p new)~nMemory usage: ~p (added ~p)",[Table, NewSize, NewSize-OldSize, NewMemory, NewMemory-OldMemory]);
fill_table(map, Table, Data, {DropKeysList, Spec})->
  OldSize = ets:info(Table, size),
  OldMemory = ets:info(Table, memory),
  R = lists:map(fun({K,V})-> {K,to_map(V, DropKeysList)} end,Data),
  lists:foreach(fun(X)-> ets:insert(Table, X)  end, ets:match_spec_run( R, ets:match_spec_compile(Spec))),
  NewSize = ets:info(Table, size),
  NewMemory = ets:info(Table, memory),
  error_logger:info_msg("  ~p table updated:~n  ~p records (~p new)~nMemory usage: ~p (added ~p)",[Table, NewSize, NewSize-OldSize, NewMemory, NewMemory-OldMemory]).
  %%ets:match_spec_run( R, ets:match_spec_compile([{{'$1', '$2'}, [], [{{'$1', {map_get, <<"en">>, {map_get, <<"name">>, '$2'}}}}]}]))

update_map(_Map1, Map2, 0)->Map2;
update_map(Map1, Map2, N)->
  Keys1 = maps:keys(Map1),
  Keys2 = maps:keys(Map2),
  KeysToUpdate = lists:filter(fun(X)->lists:member(X, Keys1) end, Keys2),
  Res1 = maps:merge(Map1, Map2),
  lists:foldr(fun(Key,Acc)-> Value=update_map(maps:get(Key, Map1), maps:get(Key, Map2), N-1), Acc#{Key => Value} end, Res1, KeysToUpdate).

to_map(List, []) when is_list(List) ->
  to_map(List);
to_map(List, [{KeyID1,KeyID2}]) when is_list(List) ->
  Key1 = proplists:get_value(KeyID1, List),
  Key2 = proplists:get_value(KeyID2, List),
  #{Key1 => Key2};
to_map(List, [DropKeys|KeysList]) when is_list(List) and is_list(DropKeys) ->
  Res = lists:foldr(fun(X,Acc)-> proplists:delete(X,Acc) end,List, DropKeys),
  to_map(Res, KeysList);
to_map(List, [KeyID|KeysList]) when is_list(List) ->
  Key = proplists:get_value(KeyID, List),
  #{Key => to_map(proplists:delete(KeyID,List), KeysList)}.
to_map(List) when is_list(List) ->
  lists:foldr(fun({K,V},Acc)-> Acc#{K => to_map(V)}; (X,Acc)-> Acc end, #{},List);
to_map(List)->
  List.
