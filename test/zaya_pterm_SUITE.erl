-module(zaya_pterm_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(POOL_PARAMS, #{
  pool => #{
    pool_size => 1,
    batch_size => 4
  }
}).

-export([
  all/0,
  groups/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_group/2,
  end_per_group/2,
  init_per_testcase/2,
  end_per_testcase/2
]).

-export([
  default_pool_created_test/1,
  service_api_and_info_test/1,
  low_level_api_test/1,
  iterator_navigation_test/1,
  find_query_variants_test/1,
  fold_and_copy_test/1,
  dump_batch_and_pool_batch_test/1,
  transaction_api_test/1,
  prepare_rollback_roundtrip_test/1,
  is_persistent_test/1,
  concurrent_write_callers_test/1
]).

all()->
  [
    default_pool_created_test,
    {group, pool_mode},
    {group, direct_mode}
  ].

groups()->
  Tests = mode_tests(),
  [
    {pool_mode, [sequence], Tests},
    {direct_mode, [sequence], Tests}
  ].

mode_tests()->
  [
    service_api_and_info_test,
    low_level_api_test,
    iterator_navigation_test,
    find_query_variants_test,
    fold_and_copy_test,
    dump_batch_and_pool_batch_test,
    transaction_api_test,
    prepare_rollback_roundtrip_test,
    is_persistent_test,
    concurrent_write_callers_test
  ].

init_per_suite(Config)->
  {ok, _Apps} = application:ensure_all_started(zaya_pterm),
  Config.

end_per_suite(_Config)->
  ok = application:stop(zaya_pterm),
  ok.

init_per_group(pool_mode, Config)->
  [{mode, pool}, {backend_params, ?POOL_PARAMS} | Config];
init_per_group(direct_mode, Config)->
  [{mode, direct}, {backend_params, #{pool => disabled}} | Config];
init_per_group(_Group, Config)->
  Config.

end_per_group(_Group, _Config)->
  ok.

init_per_testcase(_TestCase, Config)->
  Config.

end_per_testcase(_TestCase, _Config)->
  ok.

default_pool_created_test(_Config)->
  Ref1 = zaya_pterm:create(#{}),
  try
    assert_default_pool_ref(Ref1),
    ok = zaya_pterm:write(Ref1, [{default_create, ok}]),
    ?assertEqual([{default_create, ok}], zaya_pterm:read(Ref1, [default_create]))
  after
    ok = zaya_pterm:close(Ref1)
  end,

  Ref2 = zaya_pterm:open(#{}),
  try
    assert_default_pool_ref(Ref2),
    ?assertEqual([], zaya_pterm:read(Ref2, [default_open]))
  after
    ok = zaya_pterm:close(Ref2)
  end.

service_api_and_info_test(Config)->
  Params = backend_params(Config),
  ?assertEqual(ok, zaya_pterm:remove(Params)),

  Ref1 = zaya_pterm:create(Params),
  try
    assert_mode_ref(Config, Ref1),
    ?assertEqual(undefined, zaya_pterm:first(Ref1)),
    ?assertEqual(undefined, zaya_pterm:last(Ref1)),
    ok = zaya_pterm:write(Ref1, [{service_key, service_value}]),
    ?assert(zaya_pterm:get_size(Ref1) > 0)
  after
    ok = zaya_pterm:close(Ref1)
  end,

  Ref2 = zaya_pterm:open(Params),
  try
    assert_mode_ref(Config, Ref2),
    ?assertEqual([], zaya_pterm:read(Ref2, [service_key]))
  after
    ok = zaya_pterm:close(Ref2)
  end,

  ?assertEqual(ok, zaya_pterm:remove(Params)).

low_level_api_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_pterm:write(Ref, sample_records()),
      ?assertEqual(
        [{5, five}, {1, one}, {3, three}],
        zaya_pterm:read(Ref, [5, 1, 99, 3])
      ),

      ok = zaya_pterm:write(Ref, []),
      ok = zaya_pterm:delete(Ref, [3, 42]),
      ?assertEqual(
        #{1 => one, 5 => five, 7 => seven},
        read_map(Ref, [1, 5, 7])
      ),

      ok = zaya_pterm:delete(Ref, []),
      ?assertEqual([], zaya_pterm:read(Ref, [3, 42]))
    end
  ).

iterator_navigation_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      ?assertEqual(undefined, zaya_pterm:first(Ref)),
      ?assertEqual(undefined, zaya_pterm:last(Ref)),
      ?assertEqual(undefined, zaya_pterm:next(Ref, 1)),
      ?assertEqual(undefined, zaya_pterm:prev(Ref, 1)),

      ok = zaya_pterm:write(Ref, sample_records()),
      ?assertEqual({1, one}, zaya_pterm:first(Ref)),
      ?assertEqual({7, seven}, zaya_pterm:last(Ref)),
      ?assertEqual({3, three}, zaya_pterm:next(Ref, 1)),
      ?assertEqual({3, three}, zaya_pterm:next(Ref, 2)),
      ?assertEqual(undefined, zaya_pterm:next(Ref, 7)),
      ?assertEqual({5, five}, zaya_pterm:prev(Ref, 7)),
      ?assertEqual({5, five}, zaya_pterm:prev(Ref, 6)),
      ?assertEqual(undefined, zaya_pterm:prev(Ref, 1))
    end
  ).

find_query_variants_test(Config)->
  MSAtLeastThree = [{{'$1', '$2'}, [{'>=', '$1', 3}], ['$_']}],
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_pterm:write(Ref, sample_records()),
      ?assertEqual(sample_records(), zaya_pterm:find(Ref, #{})),
      ?assertEqual(
        [{3, three}, {5, five}, {7, seven}],
        zaya_pterm:find(Ref, #{start => 2})
      ),
      ?assertEqual(
        [{3, three}, {5, five}],
        zaya_pterm:find(Ref, #{start => 2, stop => 5, limit => 2})
      ),
      ?assertEqual(
        [{3, three}, {5, five}, {7, seven}],
        zaya_pterm:find(Ref, #{ms => MSAtLeastThree})
      ),
      ?assertEqual(
        [{3, three}, {5, five}],
        zaya_pterm:find(Ref, #{ms => MSAtLeastThree, limit => 2})
      ),
      ?assertEqual(
        [{3, three}, {5, five}],
        zaya_pterm:find(Ref, #{start => 2, stop => 5, ms => MSAtLeastThree})
      ),
      ?assertEqual(
        [{3, three}],
        zaya_pterm:find(Ref, #{start => 2, stop => 7, ms => MSAtLeastThree, limit => 1})
      )
    end
  ).

fold_and_copy_test(Config)->
  MSValuesAtLeastThree = [{{'$1', '$2'}, [{'>=', '$1', 3}], ['$2']}],
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_pterm:write(Ref, sample_records()),
      ?assertEqual(
        [seven, five, three],
        zaya_pterm:foldl(
          Ref,
          #{ms => MSValuesAtLeastThree},
          fun(Value, Acc)-> [Value | Acc] end,
          []
        )
      ),
      ?assertEqual(
        [1, 3, 5],
        zaya_pterm:foldr(
          Ref,
          #{start => 6},
          fun({Key, _Value}, Acc)-> [Key | Acc] end,
          []
        )
      ),
      ?assertEqual(
        [{5, five}, {3, three}, {1, one}],
        zaya_pterm:foldl(
          Ref,
          #{},
          fun({5, five} = Rec, Acc)-> throw({stop, [Rec | Acc]});
             (Rec, Acc)-> [Rec | Acc]
          end,
          []
        )
      ),
      ?assertEqual(
        [7, 5, 3, 1],
        zaya_pterm:copy(
          Ref,
          fun({Key, _Value}, Acc)-> [Key | Acc] end,
          []
        )
      )
    end
  ).

dump_batch_and_pool_batch_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_pterm:dump_batch(Ref, [{raw_a, 10}, {raw_b, 20}]),
      ?assertEqual(
        #{raw_a => 10, raw_b => 20},
        read_map(Ref, [raw_a, raw_b])
      ),

      ok =
        zaya_pterm:pool_batch(
          Ref,
          [
            {write, [{raw_c, 30}]},
            {write, [{raw_d, 40}]},
            {delete, [raw_a]},
            {write, [{raw_e, 50}]},
            {delete, []}
          ]
        ),
      ?assertEqual(
        #{raw_b => 20, raw_c => 30, raw_d => 40, raw_e => 50},
        read_map(Ref, [raw_b, raw_c, raw_d, raw_e])
      ),
      ?assertEqual([], zaya_pterm:read(Ref, [raw_a]))
    end
  ).

transaction_api_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_pterm:write(Ref, [{keep, 1}, {drop, 2}, {stay, 3}]),
      ok = zaya_pterm:commit(Ref, [{keep, 10}, {add, 11}], [drop]),
      ?assertEqual(
        #{keep => 10, add => 11, stay => 3},
        read_map(Ref, [keep, add, stay])
      ),
      ?assertEqual([], zaya_pterm:read(Ref, [drop])),

      ok = zaya_pterm:commit(Ref, [], []),
      ok = zaya_pterm:write(Ref, []),
      ok = zaya_pterm:delete(Ref, []),
      ?assertEqual(
        #{keep => 10, add => 11, stay => 3},
        read_map(Ref, [keep, add, stay])
      )
    end
  ).

prepare_rollback_roundtrip_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      ok = zaya_pterm:write(Ref, [{item, original}, {drop, old}]),
      {RollbackWrite, RollbackDelete} =
        zaya_pterm:prepare_rollback(Ref, [{item, updated}, {fresh, new}], [drop, missing]),
      ok = zaya_pterm:commit(Ref, [{item, updated}, {fresh, new}], [drop, missing]),
      ok = zaya_pterm:commit(Ref, RollbackWrite, RollbackDelete),
      ?assertEqual(
        #{item => original, drop => old},
        read_map(Ref, [item, drop])
      ),
      ?assertEqual([], zaya_pterm:read(Ref, [fresh, missing]))
    end
  ).

is_persistent_test(_Config)->
  ?assertEqual(false, zaya_pterm:is_persistent()).

concurrent_write_callers_test(Config)->
  with_ref(
    Config,
    fun(Ref)->
      Parent = self(),
      Keys = lists:seq(1, 8),
      [
        spawn(fun()->
          Parent ! {writer_result, Key, catch zaya_pterm:write(Ref, [{Key, Key * 10}])}
        end)
       || Key <- Keys
      ],
      Results = collect_results(length(Keys), []),
      ?assertEqual([], [Result || {_Key, Result} <- Results, Result =/= ok]),
      ?assertEqual(
        maps:from_list([{Key, Key * 10} || Key <- Keys]),
        read_map(Ref, Keys)
      )
    end
  ).

with_ref(Config, Fun)->
  Ref = zaya_pterm:create(backend_params(Config)),
  try
    assert_mode_ref(Config, Ref),
    Fun(Ref)
  after
    ok = zaya_pterm:close(Ref)
  end.

backend_params(Config)->
  ?config(backend_params, Config).

assert_default_pool_ref(Ref)->
  ?assert(is_pid(ref_pool(Ref))).

assert_mode_ref(Config, Ref)->
  case ?config(mode, Config) of
    pool->
      ?assert(is_pid(ref_pool(Ref)));
    direct->
      ?assertEqual(disabled, ref_pool(Ref))
  end.

ref_pool(Ref)->
  element(4, Ref).

sample_records()->
  [{1, one}, {3, three}, {5, five}, {7, seven}].

read_map(Ref, Keys)->
  maps:from_list(zaya_pterm:read(Ref, Keys)).

collect_results(0, Results)->
  Results;
collect_results(Count, Results)->
  receive
    {writer_result, Key, Result}->
      collect_results(Count - 1, [{Key, Result} | Results])
  after
    5000 ->
      ct:fail(timeout)
  end.
