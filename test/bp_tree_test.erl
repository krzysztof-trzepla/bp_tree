%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_test).
-author("krzysztof").

-include("bp_tree.hrl").
-include_lib("eunit/include/eunit.hrl").

new_should_use_defaults_test() ->
    ?assertMatch(#bp_tree{
        order = 50,
        store_module = bp_tree_map_store
    }, bp_tree:new()).

new_should_use_custom_options_test_() ->
    StoreModule = some_module,
    StoreArgs = [arg1, arg2, arg3],
    {setup,
        fun() ->
            meck:new(StoreModule, [non_strict]),
            meck:expect(StoreModule, init, fun(Args) when Args =:= StoreArgs ->
                state
            end)
        end,
        fun(_) ->
            ?assert(meck:validate(StoreModule)),
            meck:unload(StoreModule)
        end,
        ?_assertEqual(#bp_tree{
            order = 32,
            store_module = StoreModule,
            store_state = state
        }, bp_tree:new([
            {order, 32},
            {store_module, StoreModule},
            {store_args, StoreArgs}
        ]))
    }.