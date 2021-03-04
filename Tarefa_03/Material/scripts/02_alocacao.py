# -*- coding: utf-8 -*-
# ===================================================================================================================================
# TGT410062 - Tarefa 03
# Script para fazer alocacao
# 2020-09-15
# ===================================================================================================================================
# CONFIGURACOES

db_string = "host=localhost dbname=programacao user=danilo password=dan"
arquivo_grafo = '/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_03/Material/dados/dados_processados/mynetwork.graphml'
arquivo_fig = '/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_03/Material/resultados/figura.png'
n_processos = 8
# ===================================================================================================================================
import psycopg2
import osmnx as ox
import networkx as nx
ox.config(use_cache=True, log_console=True)
ox.__version__
from entidades.ParOD import ParOD
from multiprocessing.pool import ThreadPool
import matplotlib.pyplot as plt
import os
import time
# ===================================================================================================================================

def highway(velocidade):

    global clausula_where

    if velocidade == "100":
        clausula_where = "highway = 'trunk'"
    elif velocidade == "80":
        clausula_where = "highway='trunk_link' OR highway='primary'"
    elif velocidade == "70":
        clausula_where = "highway='secondary'"
    elif velocidade == "60":
        clausula_where = "highway='primary_link' OR highway='tertiary'"
    elif velocidade == "50":
        clausula_where = "highway='secondary_link' OR highway='tertiary_link'"
    elif velocidade == "40":
        clausula_where = "highway='residential'"

    return clausula_where

# ===================================================================================================================================

class Alocacao():

    G = None

    def __init__(self):

        global G
        #global tempo_max
        #global caminho_max

        tempo_max = 0        

        start_time = time.time()

        con = psycopg2.connect(db_string)
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        with con:

            cur = con.cursor()

            G = ox.load_graphml(arquivo_grafo)

            cur.execute("UPDATE edges SET volume=0")

            cur.execute("UPDATE edges SET velocidade=30")

            velocidades = ["100", "80", "70", "60", "50", "40"]

            for velocidade in velocidades:
                update_string = "UPDATE edges SET velocidade=%s WHERE %s" % (velocidade, highway(velocidade))
                # print(update_string)
                cur.execute(update_string)            

            cur.execute("SELECT zona FROM centroides ORDER BY zona")
            zonas = cur.fetchall()

            pares_od = []

            for origem in zonas:

                for destino in zonas:

                    if destino != origem:

                        pares_od.append([origem[0], destino[0]])


            # print(pares_od)

            pool = ThreadPool(n_processos)
            pool.map(self.__processaCM, pares_od)
            pool.close()
            pool.join()

            #print(tempo_max)
            #print(caminho_max)

            #route = caminho_max

            #fig, ax = ox.plot_graph_route(G, route, node_size=0)
            # plt.savefig(arquivo_fig)

        end_time = time.time()
        print("Tempo de execução = %s segundos." % (end_time - start_time))



    def __processaCM(self, origem_destino):

        global G
        #global tempo_max
        #global caminho_max

        # print("Proccess id: ", os.getpid())

        con = psycopg2.connect(db_string)
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        with con:

            #global tempo_max
            #global caminho_max

            cur = con.cursor()

            origem = int(origem_destino[0])
            destino = int(origem_destino[1])

            select_string = "SELECT viagens FROM matriz_od WHERE origem=%d AND destino=%d" % (origem, destino)
            #print(select_string)
            cur.execute(select_string)
            viagens = cur.fetchall()[0][0]

            if viagens > 0:

                # print(viagens)
                # print("origem = %d" % origem)
                # print("destino = %d" % destino)

                cur.execute("SELECT ST_X(geom), ST_Y(geom) FROM centroides WHERE zona=%d" % origem)
                #cur.execute("SELECT ST_X(wkb_geometry), ST_Y(wkb_geometry) FROM centroides WHERE zona=%d" % origem)
                result = cur.fetchall()
                origem_X = result[0][0]
                origem_Y = result[0][1]

                cur.execute("SELECT ST_X(geom), ST_Y(geom) FROM centroides WHERE zona=%d" % destino)
                #cur.execute("SELECT ST_X(wkb_geometry), ST_Y(wkb_geometry) FROM centroides WHERE zona=%d" % destino)
                result = cur.fetchall()
                destino_X = result[0][0]
                destino_Y = result[0][1]

                # print(origem_X)

                parOD = ParOD(G, origem_X, origem_Y, destino_X, destino_Y)
                parOD.processa_CM()

                caminho = parOD.get_CM()

                #print(caminho) # o caminho é uma lista de nós

                #tempo = 0             

                for i in range(len(caminho) - 1):


                    cur.execute("SELECT COUNT(*) FROM edges WHERE \"from\"=%d AND \"to\"=%d" % (caminho[i], caminho[i + 1]))
                    quantidade = cur.fetchall()[0][0]

                    if quantidade > 0:
                        
                        update_string = "UPDATE edges SET volume=volume+%d WHERE \"from\"=%d AND \"to\"=%d" % (viagens, caminho[i], caminho[i + 1])
                        # print(update_string)
                        cur.execute(update_string)

                        #update_string = "SELECT length, velocidade from edges WHERE \"from\"=%d AND \"to\"=%d" % (caminho[i], caminho[i + 1])
                        # print(update_string)
                        #cur.execute(update_string)
                        #result = cur.fetchall()
                        #length = result[0][0]
                        #vel = result[0][1]
                        #print(length, vel)

                        #tempo += (length * 1000 * 60 / vel)
                        #print(tempo)

                    else:

                        update_string = "UPDATE edges SET volume=volume+%d WHERE \"from\"=%d AND \"to\"=%d" % (viagens, caminho[i + 1], caminho[i])
                        # print(update_string)
                        cur.execute(update_string)

                        #update_string = "SELECT length, velocidade from edges WHERE \"from\"=%d AND \"to\"=%d" % (caminho[i + 1], caminho[i])
                        # print(update_string)
                        #cur.execute(update_string)
                        #result = cur.fetchall()
                        #length = result[0][0]
                        #vel = result[0][1]
                        #print(length, vel)

                        #tempo += (length * 1000 * 60 / vel)
                        #print(tempo)

                #if tempo > tempo_max:
                    #tempo_max = tempo
                    #caminho_max = caminho 


if __name__ == "__main__":

    alocacao = Alocacao()
