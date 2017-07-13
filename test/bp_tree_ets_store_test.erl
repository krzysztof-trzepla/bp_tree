%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This file contains bp_tree_ets_store module tests.
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_ets_store_test).
-author("Krzysztof Trzepla").

-include_lib("eunit/include/eunit.hrl").

bp_tree_ets_store_test_() ->
    {foreach,
        fun() -> {ok, State} = bp_tree_ets_store:init([]), State end,
        fun(State) -> bp_tree_ets_store:terminate(State) end,
        [
            fun set_root_should_succeed/1,
            fun unset_root_should_succeed/1,
            fun get_root_should_succeed/1,
            fun get_root_should_return_missing_error/1,
            fun create_root_should_succeed/1,
            fun get_node_should_succeed/1,
            fun get_node_should_return_missing_error/1,
            fun update_node_should_succeed/1,
            fun update_node_should_return_missing_error/1,
            fun delete_node_should_succeed/1,
            fun delete_node_should_return_missing_error/1
        ]
    }.

set_root_should_succeed(State) ->
    ?_assertMatch({ok, _}, bp_tree_ets_store:set_root_id(1, State)).

unset_root_should_succeed(State) ->
    ?_assertMatch({ok, _}, bp_tree_ets_store:unset_root_id(State)).

get_root_should_succeed(State) ->
    {ok, State2} = bp_tree_ets_store:set_root_id(1, State),
    ?_assertMatch({{ok, 1}, _}, bp_tree_ets_store:get_root_id(State2)).

get_root_should_return_missing_error(State) ->
    ?_assertMatch({{error, not_found}, _},
        bp_tree_ets_store:get_root_id(State)).

create_root_should_succeed(State) ->
    ?_assertMatch({{ok, _}, _}, bp_tree_ets_store:create_node(node, State)).

get_node_should_succeed(State) ->
    {{ok, NodeId}, State2} = bp_tree_ets_store:create_node(node, State),
    ?_assertMatch({{ok, node}, _}, bp_tree_ets_store:get_node(NodeId, State2)).

get_node_should_return_missing_error(State) ->
    ?_assertMatch({{error, not_found}, _},
        bp_tree_ets_store:get_node(1, State)).

update_node_should_succeed(State) ->
    {{ok, NodeId}, State2} = bp_tree_ets_store:create_node(node, State),
    ?_assertMatch({ok, _}, bp_tree_ets_store:update_node(NodeId, node, State2)).

update_node_should_return_missing_error(State) ->
    ?_assertMatch({{error, not_found}, _},
        bp_tree_ets_store:update_node(1, node, State)).

delete_node_should_succeed(State) ->
    {{ok, NodeId}, State2} = bp_tree_ets_store:create_node(node, State),
    ?_assertMatch({ok, _}, bp_tree_ets_store:delete_node(NodeId, State2)).

delete_node_should_return_missing_error(State) ->
    ?_assertMatch({{error, not_found}, _},
        bp_tree_ets_store:delete_node(1, State)).
