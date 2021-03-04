# -*- coding: utf-8 -*-

# ===================================================================================================================================

# TGT410062 - Tarefa 03
# Classe para o par OD
# 2020-09-15

# ===================================================================================================================================

import networkx as nx
import osmnx as ox

class ParOD():

    __nodes = []
    
    def __init__(self, G, origem_X, origem_Y, destino_X, destino_Y):

        # print("ParOD criado!!!!")

        self.G = G
        self.origem_X = origem_X
        self.origem_Y = origem_Y
        self.destino_X = destino_X
        self.destino_Y = destino_Y




    def processa_CM(self):

        no_origem = ox.get_nearest_node(self.G, (self.origem_Y, self.origem_X))
        no_destino = ox.get_nearest_node(self.G, (self.destino_Y, self.destino_X))

        self.__nodes = nx.dijkstra_path(self.G, no_origem, no_destino, weight="length")


    def get_CM(self):

        return self.__nodes



# ===================================================================================================================================


