%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").

%% API exports
-export([new/0, new/1]).
-export([find/2, insert/3, erase/2]).

-type key() :: term().
-type value() :: term().
-type tree() :: #bp_tree{}.
-type tree_node() :: #bp_tree_node{}.
-type option() :: {order, pos_integer()} |
                  {store_module, module()} |
                  {store_args, bp_tree_store:args()}.

-export_type([key/0, value/0, tree/0, tree_node/0, option/0]).

%%====================================================================
%% API functions
%%====================================================================

%%--------------------------------------------------------------------
%% @equiv new(undefined)
%% @end
%%--------------------------------------------------------------------
-spec new() -> tree().
new() ->
    new([]).

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec new([option()]) -> tree().
new(Opts) ->
    Order = proplists:get_value(order, Opts, 50),
    Module = proplists:get_value(store_module, Opts, bp_tree_map_store),
    Args = proplists:get_value(store_args, Opts, []),
    #bp_tree{
        order = Order,
        store_module = Module,
        store_state = Module:init(Args)
    }.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec find(key(), tree()) -> {{ok, value()} | {error, term()}, tree()}.
find(Key, Tree = #bp_tree{}) ->
    case get_root_id(Tree) of
        {{ok, RootId}, Tree2} -> find(Key, RootId, Tree2);
        {{error, Reason}, Tree2} -> {{error, Reason}, Tree2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec find(key(), bp_tree_node:id(), tree()) ->
    {{ok, value()} | {error, term()}, tree()}.
find(Key, RootId, Tree = #bp_tree{}) ->
    case get_node(RootId, Tree) of
        {{ok, Node}, Tree2} ->
            case bp_tree_node:find(Key, Node) of
                {ok, Value} -> {{ok, Value}, Tree2};
                {next, NodeId} -> find(Key, NodeId, Tree2);
                {error, Reason} -> {{error, Reason}, Tree2}
            end;
        {{error, Reason}, Tree2} ->
            {{error, Reason}, Tree2}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec insert(key(), value(), tree()) -> {ok | {error, term()}, tree()}.
insert(_Key, _Value, Tree = #bp_tree{}) ->
    {{error, not_implemented}, Tree}.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec erase(key(), tree()) -> {ok | {error, term()}, tree()}.
erase(_Key, Tree = #bp_tree{}) ->
    {{error, not_implemented}, Tree}.

%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec get_root_id(tree()) ->
    {{ok, bp_tree_node:id()} | {error, term()}, tree()}.
get_root_id(Tree = #bp_tree{store_module = Module, store_state = State}) ->
    case Module:get_root(State) of
        {{ok, RootId}, State2} ->
            {{ok, RootId}, Tree#bp_tree{store_state = State2}};
        {{error, Reason}, State2} ->
            {{error, Reason}, Tree#bp_tree{store_state = State2}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec get_node(bp_tree_node:id(), tree()) ->
    {{ok, tree_node()} | {error, term()}, tree()}.
get_node(NodeId, Tree = #bp_tree{store_module = Module, store_state = State}) ->
    case Module:get_node(NodeId, State) of
        {{ok, Node}, State2} ->
            {{ok, Node}, Tree#bp_tree{store_state = State2}};
        {{error, Reason}, State2} ->
            {{error, Reason}, Tree#bp_tree{store_state = State2}}
    end.