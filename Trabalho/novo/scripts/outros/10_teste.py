# -*- coding: utf-8 -*-

# ===================================================================================================================================

# TGT410062 - Tarefa 03
# Script para gerar uma camada de dados geograficos vetorias, com propriedados de grafo,
# a partir de dados do OpenStreetMap
# 2020-09-14

# ===================================================================================================================================

# CONFIGURAÇÕES

arquivo_graphml = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_03/Material/dados/dados_processados/mynetwork.graphml"
diretorio_shape_network = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_03/Material/dados/dados_processados/shapefile"

# ===================================================================================================================================

import time
import networkx as nx
import osmnx as ox
import requests
import matplotlib.cm as cm
import matplotlib.colors as colors
# %matplotlib inline
ox.config(use_cache=True, log_console=True)
ox.__version__

# ===================================================================================================================================

class PegaOSM():

    def __init__(self):

        start_time = time.time()

        # get a graph for some city
        G = ox.graph_from_place("Florianópolis, Brazil", network_type="drive")
        # fig, ax = ox.plot_graph(G)

        # convert projected graph to edges geodataframe
        gdf_edges = ox.graph_to_gdfs(ox.project_graph(G), nodes=False)
        print(gdf_edges)

        # list of lats and lngs
        lngs = gdf_edges.head().centroid#.map(lambda x: x.coords[0][0])
        lats = gdf_edges.head().centroid#.map(lambda x: x.coords[0][1])

        print(lngs)
        print(type(lngs))
        print(lats)
        print(type(lats))

        # the lat, lng at the spatial center of the graph
        lng, lat = gdf_edges.unary_union.centroid.coords[0]
        center_point = lat, lng

        # find the nearest node to some point
        center_node = ox.get_nearest_node(G, center_point)

        # find the nearest nodes to a set of points
        # optionally specify `method` use use a kdtree or balltree index
        nearest_nodes = ox.get_nearest_nodes(G, lngs, lats, method='kdtree')

        # find the nearest edge to some point
        nearest_edge = ox.get_nearest_edge(G, center_point)

        # find the nearest edges to some set of points
        # optionally specify `method` use use a kdtree or balltree index
        nearest_edges = ox.get_nearest_edges(G, lngs, lats)

        # find the shortest path (by distance) between these nodes then plot it
        orig = list(G)[0]
        dest = list(G)[120]
        route = ox.shortest_path(G, orig, dest, weight='length')
        # fig, ax = ox.plot_graph_route(G, route, route_color='y', route_linewidth=6, node_size=0)

        routes = ox.k_shortest_paths(G, orig, dest, k=30, weight='length')
        # fig, ax = ox.plot_graph_routes(G, list(routes), route_colors='y', route_linewidth=4, node_size=0)

        # same thing again, but this time pass in a few default speed values (km/hour)
        # to fill in edges with missing `maxspeed` from OSM
        hwy_speeds = {"trunk": 100,
                      "['trunk', 'trunk_link']": 30,
                      "secondary": 70,
                      "['primary_link', 'primary']": 30,
                      "dummy": 30,
                      "trunk_link": 80,
                      "tertiary": 60,
                      "secondary_link": 50,
                      "tertiary_link": 50,
                      "['residential', 'unclassified']": 30,
                      "living_street": 30,
                      "primary": 80,
                      "['residential', 'secondary']": 30,
                      "residential": 40,
                      "primary_link": 60,
                      "['secondary_link', 'secondary']": 30,
                      "['residential', 'primary']": 30,
                      "['primary', 'trunk_link']": 30,
                      "['residential', 'living_street']": 30,
                      "['tertiary', 'secondary']": 30,
                      "unclassified": 30,
                      "['primary', 'trunk']": 30}

        G = ox.add_edge_speeds(G, hwy_speeds)
        G = ox.add_edge_travel_times(G)

        # calculate two routes by minimizing travel distance vs travel time
        orig = list(G)[1]
        dest = list(G)[120]
        route1 = ox.shortest_path(G, orig, dest, weight='length')
        route2 = ox.shortest_path(G, orig, dest, weight='travel_time')

        # plot the routes
        #fig, ax = ox.plot_graph_routes(G, routes=[route1, route2], route_colors=['r', 'y'], route_linewidth=6, node_size=0)

        # compare the two routes
        route1_length = int(sum(ox.utils_graph.get_route_edge_attributes(G, route1, 'length')))
        route2_length = int(sum(ox.utils_graph.get_route_edge_attributes(G, route2, 'length')))
        route1_time = int(sum(ox.utils_graph.get_route_edge_attributes(G, route1, 'travel_time')))
        route2_time = int(sum(ox.utils_graph.get_route_edge_attributes(G, route2, 'travel_time')))
        print('Route 1 is', route1_length, 'meters and takes', route1_time, 'seconds.')
        print('Route 2 is', route2_length, 'meters and takes', route2_time, 'seconds.')


        # save graph to disk as geopackage (for GIS) or graphml file (for gephi etc)
        # ox.save_graph_geopackage(G, filepath='./data/mynetwork.gpkg')
        #ox.save_graphml(G, filepath=arquivo_graphml)
        #ox.save_graph_shapefile(G, filepath=diretorio_shape_network)

        end_time = time.time()
        print("Tempo de execução = %s segundos." % (end_time - start_time))

if __name__ == "__main__":

    pegaOSM = PegaOSM()


# ===================================================================================================================================

