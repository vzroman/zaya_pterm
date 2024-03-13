
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
  transaction/1,
  t_write/3,
  t_delete/3,
  commit/2,
  commit1/2,
  commit2/2,
  rollback/2
]).

%%=================================================================
%%	INFO API
%%=================================================================
-export([
  get_size/1
]).

-record(data,{ dict, index }).
-define(ref(Ref),{?MODULE, Ref}).
-define(none, {?MODULE, undefined}).

%%=================================================================
%%	SERVICE
%%=================================================================
create( Params )->
  open( Params ).

open( _Params )->

  Ref = ?ref(erlang:make_ref()),
  persistent_term:put(Ref,#data{
    dict = #{},
    index = gb_sets:new()
  }),

  Ref.

close( Ref )->
  catch persistent_term:erase( Ref ),
  ok.

remove( _Params )->
  ok.

%%=================================================================
%%	LOW_LEVEL
%%=================================================================
read( Ref, Keys )->
  #data{ dict = Dict } = persistent_term:get( Ref ),
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

write(Ref, KVs)->
  Data0 = persistent_term:get( Ref ),
  Data = do_write( Data0, KVs ),
  persistent_term:put( Ref, Data ),
  ok.

do_write( #data{ dict = Dict, index = Index } = Data, [{K,V} | Rest])->
  do_write(Data#data{ dict = Dict#{ K => V }, index = gb_sets:add_element(K, Index) }, Rest);
do_write(Data, [])->
  Data.


delete(Ref,Keys)->
  Data0 = persistent_term:get( Ref ),
  Data = do_delete( Data0, Keys ),
  persistent_term:put( Ref, Data ),
  ok.

do_delete( #data{ dict = Dict, index = Index } = Data, [K | Rest])->
  do_delete(Data#data{ dict = maps:remove(K, Dict), index = gb_sets:del_element(K, Index) }, Rest);
do_delete(Data, [])->
  Data.

%%=================================================================
%%	ITERATOR
%%=================================================================
first( Ref )->
  #data{ dict = Dict, index = Index } = persistent_term:get( Ref ),
  try
    First = gb_sets:smallest( Index ),
    {First, maps:get(First, Dict)}
  catch
    _:_-> undefined
  end.

last( Ref )->
  #data{ dict = Dict, index = Index } = persistent_term:get( Ref ),
  try
    Last = gb_sets:largest( Index ),
    {Last, maps:get(Last, Dict)}
  catch
    _:_-> undefined
  end.

next( Ref, Key )->
  #data{ dict = Dict, index = Index } = persistent_term:get( Ref ),
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

prev( Ref, Key )->
  #data{ dict = Dict, index = Index } = persistent_term:get( Ref ),
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
find(Ref, Query)->
  #data{ dict = Dict, index = Index } = persistent_term:get( Ref ),

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
foldl( Ref, Query, UserFun, InAcc )->
  #data{ dict = Dict, index = Index } = persistent_term:get( Ref ),

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
foldr( Ref, Query, UserFun, InAcc )->
  #data{ dict = Dict} = persistent_term:get( Ref ),

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
  write(Ref, KVs).

%%=================================================================
%%	TRANSACTION API
%%=================================================================
transaction( _Ref )->
  ets:new(?MODULE,[
    private,
    ordered_set,
    {read_concurrency, true},
    {write_concurrency, auto}
  ]).

t_write( _Ref, TRef, KVs )->
  ets:insert( TRef, KVs ),
  ok.

t_delete( _Ref, TRef, Keys )->
  ets:insert( TRef, [{K, ?none} || K <- Keys] ),
  ok.

commit(Ref, TRef)->
  {Write, Delete} = write_delete( ets:tab2list( TRef ), {[],[]} ),
  try
    write( Ref, Write ),
    delete( Ref, Delete )
  after
    ets:delete( TRef )
  end.

commit1(_Ref, _TRef)->
  ok.

commit2(Ref, TRef)->
  commit( Ref, TRef ).

rollback( _Ref, TRef )->
  ets:delete( TRef ),
  ok.

write_delete([{K, ?none}|Rest], {Write, Delete})->
  write_delete( Rest, { Write, [K|Delete] } );
write_delete([E|Rest], {Write, Delete})->
  write_delete( Rest, {[E|Write], Delete} );
write_delete([], Acc)->
  Acc.

%%=================================================================
%%	INFO
%%=================================================================
get_size( Ref )->
  Data = persistent_term:get( Ref ),
  size( term_to_binary( Data ) ).



