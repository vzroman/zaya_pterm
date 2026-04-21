-module(zaya_pterm_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([prepare_rollback_roundtrip_test/1, is_persistent_test/1]).

all()->
  [prepare_rollback_roundtrip_test, is_persistent_test].

init_per_suite(Config)->
  {ok, _Apps} = application:ensure_all_started(zaya_pterm),
  Config.

end_per_suite(_Config)->
  ok = application:stop(zaya_pterm),
  ok.

prepare_rollback_roundtrip_test(_Config)->
  with_ref(
    fun(Ref)->
      ok = zaya_pterm:write(Ref, [{item, original}, {drop, old}]),
      {RollbackWrite, RollbackDelete} =
        zaya_pterm:prepare_rollback(Ref, [{item, updated}, {fresh, new}], [drop]),
      ok = zaya_pterm:commit(Ref, [{item, updated}, {fresh, new}], [drop]),
      ok = zaya_pterm:commit(Ref, RollbackWrite, RollbackDelete),
      ?assertEqual(
        #{item => original, drop => old},
        read_map(Ref, [item, drop])
      ),
      ?assertEqual([], zaya_pterm:read(Ref, [fresh]))
    end
  ).

is_persistent_test(_Config)->
  ?assertEqual(false, zaya_pterm:is_persistent()).

with_ref(Fun)->
  Ref = zaya_pterm:create(#{}),
  try
    Fun(Ref)
  after
    ok = zaya_pterm:close(Ref)
  end.

read_map(Ref, Keys)->
  maps:from_list(zaya_pterm:read(Ref, Keys)).
