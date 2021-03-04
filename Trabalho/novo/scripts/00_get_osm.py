# -*- coding: utf-8 -*-

# ===================================================================================================================================

# TGT410062 - Tarefa 03
# Script para gerar uma camada de dados geograficos vetorias, com propriedados de grafo,
# a partir de dados do OpenStreetMap
# 2020-09-14

# ===================================================================================================================================

# CONFIGURAÇÕES

arquivo_graphml = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Trabalho/novo/dados/processados/mynetwork.graphml"
diretorio_shape_network = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Trabalho/novo/dados/processados/shapefile"

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

        # save graph to disk as geopackage (for GIS) or graphml file (for gephi etc)
        # ox.save_graph_geopackage(G, filepath='./data/mynetwork.gpkg')
        ox.save_graphml(G, filepath=arquivo_graphml)
        ox.save_graph_shapefile(G, filepath=diretorio_shape_network)

        end_time = time.time()
        print("Tempo de execução = %s segundos." % (end_time - start_time))

if __name__ == "__main__":

    pegaOSM = PegaOSM()


# ===================================================================================================================================



