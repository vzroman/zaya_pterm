
-module(zaya_pterm).

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  create/1,
  open/1,
  close/1,
  remove/1
]).

%%=================================================================
%%	LOW_LEVEL API
%%=================================================================
-export([
  read/2,
  write/2,
  delete/2
]).

%%=================================================================
%%	ITERATOR API
%%=================================================================
-export([
  first/1,
  last/1,
  next/2,
  prev/2
]).

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
-export([
  find/2,
  foldl/4,
  foldr/4
]).

%%=================================================================
%%	COPY API
%%=================================================================
-export([
  copy/3,
  dump_batch/2
]).

%%=================================================================
%%	TRANSACTION API
%%=================================================================
-export([
  commit/3,
  prepare_rollback/3,
  is_persistent/0
]).

%%=================================================================
%%	POOL API
%%=================================================================
-export([
  pool_batch/2
]).

%%=================================================================
%%	INFO API
%%=================================================================
-export([
  get_size/1
]).

-record(ref,{ pterm, locks, pool }).
-record(data,{ dict, index }).
-define(ref(Ref),{?MODULE, Ref}).
-define(none, {?MODULE, undefined}).
-define(locks(Ref), binary_to_atom( unicode:characters_to_binary(io_lib:format("~p",[Ref])), utf8) ).

%%=================================================================
%%	SERVICE
%%=================================================================
create( Params )->
  open( Params ).

open( Params )->
  PTerm = ?ref(erlang:make_ref()),
  {ok, LocksPid} = elock:start_link( ?locks(PTerm) ),
  Ref = #ref{ pterm = PTerm, locks = LocksPid, pool = undefined },
  try
    persistent_term:put(PTerm, #data{
      dict = #{},
      index = gb_sets:new()
    }),
    open_pool(Ref, Params)
  catch
    Class:Reason:Stack->
      catch close(Ref),
      erlang:raise(Class, Reason, Stack)
  end.

close(#ref{ pterm = PTerm, locks = LocksPid, pool = Pool })->
  catch close_pool(Pool),
  catch unlink( LocksPid ),
  catch exit( LocksPid, shutdown ),
  catch persistent_term:erase( PTerm ),
  ok.

remove( _Params )->
  ok.

%%=================================================================
%%	LOW_LEVEL
%%=================================================================
read(#ref{ pterm = PTerm }, Keys )->
  #data{ dict = Dict } = persistent_term:get( PTerm ),
  do_read( Dict, Keys ).

do_read(Dict, [Key|Rest])->
  case maps:find(Key, Dict) of
    {ok,Value}->
      [{Key, Value} | do_read(Dict, Rest)];
    _->
      do_read(Dict, Rest)
  end;
do_read(_Dict,[])->
  [].

write(#ref{ pool = disabled } = Ref, KVs)->
  locked_update(Ref, fun(Data)-> do_write(Data, KVs) end);
write(#ref{ pool = Pool }, KVs)->
  zaya_pool:call(Pool, [{write, KVs}]).

do_write( #data{ dict = Dict, index = Index } = Data, [{K,V} | Rest])->
  do_write(Data#data{ dict = Dict#{ K => V }, index = gb_sets:add_element(K, Index) }, Rest);
do_write(Data, [])->
  Data.


delete(#ref{ pool = disabled } = Ref, Keys)->
  locked_update(Ref, fun(Data)-> do_delete(Data, Keys) end);
delete(#ref{ pool = Pool }, Keys)->
  zaya_pool:call(Pool, [{delete, Keys}]).

do_delete( #data{ dict = Dict, index = Index } = Data, [K | Rest])->
  do_delete(Data#data{ dict = maps:remove(K, Dict), index = gb_sets:del_element(K, Index) }, Rest);
do_delete(Data, [])->
  Data.

%%=================================================================
%%	ITERATOR
%%=================================================================
first( #ref{ pterm = PTerm } )->
  #data{ dict = Dict, index = Index } = persistent_term:get( PTerm ),
  try
    First = gb_sets:smallest( Index ),
    {First, maps:get(First, Dict)}
  catch
    _:_-> undefined
  end.

last( #ref{ pterm = PTerm } )->
  #data{ dict = Dict, index = Index } = persistent_term:get( PTerm ),
  try
    Last = gb_sets:largest( Index ),
    {Last, maps:get(Last, Dict)}
  catch
    _:_-> undefined
  end.

next( #ref{ pterm = PTerm }, Key )->
  #data{ dict = Dict, index = Index } = persistent_term:get( PTerm ),
  I = gb_sets:iterator_from(Key, Index),
  case gb_sets:next( I ) of
    {Key, I1} ->
      case gb_sets:next( I1 ) of
        {Next, _}-> {Next, maps:get(Next, Dict)};
        _-> undefined
      end;
    {Next,_}->
      {Next, maps:get(Next, Dict)};
    _->
      undefined
  end.

prev( #ref{ pterm = PTerm }, Key )->
  #data{ dict = Dict, index = Index } = persistent_term:get( PTerm ),
  case prev_key(Key, Index) of
    undefined ->
      undefined;
    PrevKey ->
      {PrevKey, maps:get(PrevKey, Dict)}
  end.

prev_key(K, {_, T})->
  prev_key(K, T, undefined).
prev_key(K, {TK, L, _R}, Prev) when K < TK->
  prev_key(K, L, Prev);
prev_key(K, {TK, _L, R}, _Prev) when K > TK ->
  prev_key(K, R, TK);
prev_key(K, {TK, L, _R}, Prev) when K =:= TK ->
  prev_key(K, L, Prev);
prev_key(_K, nil, Prev) ->
  Prev.

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
%----------------------FIND------------------------------------------
find(#ref{ pterm = PTerm }, Query)->
  #data{ dict = Dict, index = Index } = persistent_term:get( PTerm ),

  Itr =
    case Query of
      #{start := Start}-> gb_sets:iterator_from(Start, Index);
      _-> gb_sets:iterator( Index )
    end,

  case Query of
    #{ stop:=Stop, ms:= MS, limit:=Limit }->
      CompiledMS = ets:match_spec_compile(MS),
      iterate_query(gb_sets:next(Itr), Dict, Stop, CompiledMS, Limit );
    #{ stop:=Stop, ms:= MS}->
      CompiledMS = ets:match_spec_compile(MS),
      iterate_ms_stop(gb_sets:next(Itr), Dict, Stop, CompiledMS );
    #{ stop:= Stop, limit := Limit }->
      iterate_stop_limit(gb_sets:next(Itr), Dict, Stop, Limit );
    #{ stop:= Stop }->
      iterate_stop(gb_sets:next(Itr), Dict, Stop);
    #{ms:= MS, limit := Limit}->
      iterate_ms_limit(gb_sets:next(Itr), Dict, ets:match_spec_compile(MS), Limit );
    #{ms:= MS}->
      iterate_ms(gb_sets:next(Itr), Dict, ets:match_spec_compile(MS) );
    #{limit := Limit}->
      iterate_limit(gb_sets:next(Itr), Dict, Limit );
    _->
      iterate(gb_sets:next(Itr), Dict )
  end.

iterate_query({K, Itr}, Dict, StopKey, MS, Limit ) when K =< StopKey, Limit > 0->
  Rec = {K, maps:get(K, Dict) },
  case ets:match_spec_run([Rec], MS) of
    [Res]->
      [Res| iterate_query( gb_sets:next(Itr), Dict, StopKey, MS, Limit - 1 )];
    []->
      iterate_query(gb_sets:next(Itr), Dict, StopKey, MS, Limit )
  end;
iterate_query(_, _Dict, _StopKey, _MS, _Limit )->
  [].

iterate_ms_stop({K,Itr}, Dict, StopKey, MS) when K =< StopKey->
  Rec = {K, maps:get(K, Dict) },
  case ets:match_spec_run([Rec], MS) of
    [Res]->
      [Res| iterate_ms_stop( gb_sets:next(Itr), Dict, StopKey, MS )];
    []->
      iterate_ms_stop( gb_sets:next(Itr), Dict, StopKey, MS )
  end;
iterate_ms_stop(_, _Dict, _StopKey, _MS )->
  [].

iterate_stop_limit({K,Itr}, Dict, StopKey, Limit ) when K =< StopKey, Limit > 0->
  [{K,maps:get(K, Dict) }| iterate_stop_limit( gb_sets:next(Itr), Dict, StopKey, Limit -1 )];
iterate_stop_limit(_, _Dict, _StopKey, _Limit )->
  [].

iterate_stop({K, Itr}, Dict, StopKey ) when K =< StopKey->
  [{K, maps:get(K, Dict) }| iterate_stop( gb_sets:next(Itr), Dict, StopKey )];
iterate_stop(_, _Dict, _StopKey )->
  [].

iterate_ms_limit({K,Itr}, Dict, MS, Limit ) when Limit >0->
  Rec = {K, maps:get(K, Dict)},
  case ets:match_spec_run([Rec], MS) of
    [Res]->
      [Res| iterate_ms_limit( gb_sets:next(Itr), Dict, MS, Limit - 1 )];
    []->
      iterate_ms_limit( gb_sets:next(Itr), Dict, MS, Limit )
  end;
iterate_ms_limit(_, _Dict, _MS, _Limit )->
  [].

iterate_ms({K, Itr}, Dict, MS )->
  Rec = {K, maps:get(K, Dict)},
  case ets:match_spec_run([Rec], MS) of
    [Res]->
      [Res| iterate_ms( gb_sets:next(Itr), Dict, MS )];
    []->
      iterate_ms( gb_sets:next(Itr), Dict, MS )
  end;
iterate_ms(_, _Dict, _MS )->
  [].

iterate_limit({K, Itr}, Dict, Limit) when Limit >0->
  [{K, maps:get(K, Dict)} | iterate_limit( gb_sets:next(Itr), Dict, Limit-1 ) ];
iterate_limit(_, _Dict, _Limit )->
  [].

iterate({K,Itr}, Dict)->
  [{K, maps:get(K, Dict)} | iterate( gb_sets:next(Itr), Dict ) ];
iterate(_, _Dict )->
  [].

%----------------------FOLD LEFT------------------------------------------
foldl( #ref{ pterm = PTerm }, Query, UserFun, InAcc )->
  #data{ dict = Dict, index = Index } = persistent_term:get( PTerm ),

  Itr =
    case Query of
      #{start := Start}-> gb_sets:iterator_from(Start, Index);
      _-> gb_sets:iterator( Index )
    end,

  Fun =
    case Query of
      #{ms:=MS}->
        CompiledMS = ets:match_spec_compile(MS),
        fun(Rec,Acc)->
          case ets:match_spec_run([Rec], CompiledMS) of
            [Res]->
              UserFun(Res,Acc);
            []->
              Acc
          end
        end;
      _->
        UserFun
    end,

  try
    case Query of
      #{ stop:=Stop }->
        do_foldl_stop( gb_sets:next(Itr), Dict, Fun, InAcc, Stop);
      _->
        do_foldl( gb_sets:next(Itr), Dict, Fun, InAcc )
    end
  catch
    {stop,Acc}->Acc
  end.

do_foldl_stop( {Key, Itr}, Dict, Fun, InAcc, StopKey ) when Key =< StopKey->
  Rec = {Key, maps:get(Key, Dict)},
  Acc = Fun( Rec, InAcc ),
  do_foldl_stop( gb_sets:next(Itr), Dict, Fun, Acc, StopKey  );
do_foldl_stop(_, _Dict, _Fun, Acc, _StopKey)->
  Acc.


do_foldl({Key,Itr}, Dict, Fun, InAcc )->
  Rec = {Key, maps:get(Key, Dict)},
  Acc = Fun( Rec, InAcc ),
  do_foldl(gb_sets:next(Itr), Dict, Fun, Acc );
do_foldl(_, _Dict, _Fun, Acc )->
  Acc.

%----------------------FOLD RIGHT------------------------------------------
foldr( #ref{ pterm = PTerm }, Query, UserFun, InAcc )->
  #data{ dict = Dict} = persistent_term:get( PTerm ),

  Records0 = lists:reverse( lists:usort( maps:to_list( Dict ))),

  Records =
    case Query of
      #{start := Start}-> lists:dropwhile(fun({K,_})->K > Start end, Records0);
      _->Records0
    end,
  Fun =
    case Query of
      #{ms:=MS}->
        CompiledMS = ets:match_spec_compile(MS),
        fun(Rec,Acc)->
          case ets:match_spec_run([Rec], CompiledMS) of
            [Res]->
              UserFun(Res,Acc);
            []->
              Acc
          end
        end;
      _->
        UserFun
    end,

  try
    case Query of
      #{ stop:=Stop }->
        do_foldr_stop( Records, Fun, InAcc, Stop);
      _->
        do_foldr( Records, Fun, InAcc )
    end
  catch
    {stop,Acc}-> Acc
  end.

do_foldr_stop( [{Key,_}=Rec| Rest], Fun, InAcc, StopKey ) when Key >= StopKey->
  Acc = Fun( Rec, InAcc ),
  do_foldr_stop( Rest, Fun, Acc, StopKey  );
do_foldr_stop([], _Fun, Acc, _StopKey)->
  Acc.

do_foldr( [Rec| Rest], Fun, InAcc )->
  Acc = Fun( Rec, InAcc ),
  do_foldr( Rest, Fun, Acc  );
do_foldr([], _Fun, Acc )->
  Acc.

%%=================================================================
%%	COPY
%%=================================================================
copy(Ref, Fun, InAcc)->
  foldl(Ref, #{}, Fun, InAcc).

dump_batch(Ref, KVs)->
  locked_update(Ref, fun(Data)-> do_write(Data, KVs) end).

%%=================================================================
%%	TRANSACTION API
%%=================================================================
commit(#ref{ pool = disabled } = Ref, Write, Delete)->
  locked_update(
    Ref,
    fun(Data)->
      do_delete(do_write(Data, Write), Delete)
    end
  );
commit(#ref{ pool = Pool }, Write, Delete)->
  zaya_pool:call(Pool, [{write, Write}, {delete, Delete}]).

prepare_rollback(Ref, Write, Delete)->
  prepare_rollback_from_read(fun(Keys)-> read(Ref, Keys) end, Write, Delete).

is_persistent()->
  false.

prepare_rollback_from_read(ReadFun, Write, Delete)->
  WriteMap = maps:from_list(Write),
  WriteKeys = maps:keys(WriteMap),
  CurrentForWrites = maps:from_list(ReadFun(WriteKeys)),
  CurrentForDeletes = maps:from_list(ReadFun(Delete)),
  RestoreWrites =
    maps:fold(
      fun(Key, Existing, Acc)->
        case maps:get(Key, WriteMap) of
          Existing -> Acc;
          _ -> Acc#{Key => Existing}
        end
      end,
      CurrentForDeletes,
      CurrentForWrites
    ),
  DeleteBack = [Key || Key <- WriteKeys, not maps:is_key(Key, CurrentForWrites)],
  {maps:to_list(RestoreWrites), DeleteBack}.

%%=================================================================
%%	POOL API
%%=================================================================
pool_batch(Ref, Requests)->
  locked_update(Ref, fun(Data)-> pool_batch(Requests, Data, _Writes = []) end).

pool_batch([{write, KVs}|Rest], Data, Writes)->
  pool_batch(Rest, Data, [KVs|Writes]);
pool_batch(Requests, Data, [_|_]=Writes)->
  KVs = lists:append(lists:reverse(Writes)),
  pool_batch(Requests, do_write(Data, KVs), []);
pool_batch([{delete, Keys}|Rest], Data, Writes)->
  pool_batch(Rest, do_delete(Data, Keys), Writes);
pool_batch([], Data, [])->
  Data.

%%=================================================================
%%	INFO
%%=================================================================
get_size( #ref{ pterm = PTerm } )->
  Data = persistent_term:get( PTerm ),
  size( term_to_binary( Data ) ).

%%=================================================================
%%	INTERNAL UTILITIES
%%=================================================================
locked_update(#ref{ pterm = PTerm }, Update)->
  {ok, Unlock} = elock:lock(?locks( PTerm ), PTerm, _IsShared = false, _Timeout = infinity ),
  try
    Data0 = persistent_term:get( PTerm ),
    Data = Update(Data0),
    persistent_term:put( PTerm, Data ),
    ok
  after
    Unlock()
  end.

open_pool(Ref, #{pool := disabled})->
  Ref#ref{pool = disabled};
open_pool(Ref, Params) when is_map(Params)->
  {ok, Pool} = zaya_pool:start_link(pool_params(Ref#ref{pool = disabled}, Params)),
  Ref#ref{pool = Pool}.

close_pool(undefined)->
  ok;
close_pool(disabled)->
  ok;
close_pool(Pool)->
  zaya_pool:stop(Pool).

pool_params(Ref, Params) when is_map(Params)->
  maps:merge(
    maps:get(pool, Params, #{}),
    #{
      ref => Ref,
      module => ?MODULE
    }
  ).

