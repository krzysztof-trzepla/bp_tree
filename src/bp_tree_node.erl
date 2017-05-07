%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_node).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").

%% API exports
-export([new_leaf/1, find/2]).

-type id() :: any().
-type children() :: bp_tree_map:ordered_map().

-export_type([id/0, children/0]).

%%====================================================================
%% API functions
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec new_leaf(bp_tree:tree()) ->
    {{ok, id(), bp_tree:tree_node()} | {error, term()}, bp_tree:tree()}.
new_leaf(Tree = #bp_tree{
    order = Order,
    store_module = Module,
    store_state = State
}) ->
    Node = #bp_tree_node{
        leaf = true,
        last = undefined,
        children = bp_tree_map:new(2 * Order)
    },
    case Module:create_node(Node, State) of
        {{ok, NodeId}, State2} ->
            {{ok, NodeId, Node}, Tree#bp_tree{store_state = State2}};
        {{error, Reason}, State2} ->
            {{error, Reason}, Tree#bp_tree{store_state = State2}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec find(bp_tree:key(), bp_tree:tree_node()) ->
    {ok, bp_tree:value()} | {next, id()} | {error, term()}.
find(Key, #bp_tree_node{leaf = true, children = Children}) ->
    bp_tree_map:find(Key, Children);
find(Key, #bp_tree_node{leaf = false, last = Last, children = Children}) ->
    Pos = bp_tree_map:upper_bound(Key, Children),
    case bp_tree_map:at(Pos, Children) of
        {ok, {_, NodeId}} ->
            {next, NodeId};
        {error, out_of_range} ->
            {next, Last}
    end.

%%====================================================================
%% Internal functions
%%====================================================================
