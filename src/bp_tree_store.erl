%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_store).
-author("Krzysztof Trzepla").

-type args() :: list().
-type state() :: any().

-export_type([args/0, state/0]).

%%====================================================================
%% Callbacks
%%====================================================================

-callback init(args()) -> state().

-callback set_root(bp_tree_node:id(), state()) ->
    {ok | {error, term()}, state()}.

-callback get_root(state()) ->
    {{ok, bp_tree_node:id()} | {error, term()}, state()}.

-callback create_node(bp_tree:tree_node(), state()) ->
    {{ok, bp_tree_node:id()} | {error, term()}, state()}.

-callback get_node(bp_tree_node:id(), state()) ->
    {{ok, bp_tree:tree_node()} | {error, term()}, state()}.

-callback update_node(bp_tree_node:id(), bp_tree:tree_node(), state()) ->
    {ok | {error, term()}, state()}.

-callback delete_node(bp_tree_node:id(), state()) ->
    {ok | {error, term()}, state()}.

-callback terminate(state()) -> any().