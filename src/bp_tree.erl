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
-export([new/0, new/1, new/2]).

-type key() :: term().
-type value() :: term().
-type tree() :: #bp_tree{}.
-type tree_node() :: #bp_tree_node{}.
-type option() :: {degree, pos_integer()} |
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
    new(undefined).

%%--------------------------------------------------------------------
%% @equiv new(RootId, [])
%% @end
%%--------------------------------------------------------------------
-spec new(undefined | bp_tree_node:id()) -> tree().
new(RootId) ->
    new(RootId, []).

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec new(undefined | bp_tree_node:id(), [option()]) -> tree().
new(RootId, Opts) ->
    Degree = proplists:get_value(degree, Opts, 100),
    Module = proplists:get_value(store_module, Opts, bp_tree_map_store),
    Args = proplists:get_value(store_args, Opts, []),
    #bp_tree{
        degree = Degree,
        root_id = RootId,
        store_module = Module,
        store_state = Module:init(Args)
    }.
