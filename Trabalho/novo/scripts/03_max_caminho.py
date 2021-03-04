# -*- coding: utf-8 -*-
# ===================================================================================================================================
# TGT410062 - Tarefa 03
# Script para fazer alocacao
# 2020-09-15
# ===================================================================================================================================
# CONFIGURACOES

db_string = "host=localhost dbname=trabalho user=danilo password=dan"
arquivo_grafo = '/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Trabalho/novo/dados/processados/mynetwork.graphml'
arquivo_fig = '/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Trabalho/novo/resultados/figura.png'
n_processos = 8
# ===================================================================================================================================
import pandas as pd
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

class CaminhoMax():

    G = None

    def __init__(self):

        global G
        global tempo_max
        global caminho_max
        global origem_max
        global destino_max
        global nome_origem
        global nome_destino
        global length_max
        global cama_max
        global colunas
        global resultadao

        tempo_max = 0
        origem_max = 0
        destino_max = 0
        nome_origem = ''
        nome_destino = ''        
        length_max = 0
        cama_max = []
        colunas = ['Origem', 'Destino', 'Distancia (km)', 'Tempo (min)']
        resultadao = pd.DataFrame(columns=colunas)

        start_time = time.time()

        con = psycopg2.connect(db_string)
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        with con:

            cur = con.cursor()

            G = ox.load_graphml(arquivo_grafo)

            cur.execute("SELECT zona FROM centroides ORDER BY zona")
            zonas = cur.fetchall()

            pares_od = []
            destino = 999


            for origem in zonas:

                if origem[0] != destino:

                    pares_od.append([origem[0], destino])
            print(pares_od)


            # print(pares_od)

            pool = ThreadPool(n_processos)
            pool.map(self.__procuraCMax, pares_od)
            pool.close()
            pool.join()

            # print(tempo_max)
            # print(origem_max)
            # print(destino_max)
            # print(nome_origem)
            # print(nome_destino)
            # print(caminho_max)
            #print(length_max)

            #print(cama_max)

            # print('\nA rota de maior tempo de percurso na área de estudo é entre %s e %s com o tempo de percurso de aproximadamente %d minutos.\n' % (nome_origem, nome_destino, tempo_max))

            #print(result)

            route = caminho_max

            #fig, ax = ox.plot_graph_route(G, route, save=True, node_size=0, show=False, dpi=600, filepath=arquivo_fig)

        end_time = time.time()
        print("\nTempo de execução do OSMnx = %s segundos." % (end_time - start_time))



    def __procuraCMax(self, origem_destino):

        global G
        global tempo_max
        global caminho_max
        global origem_max
        global destino_max
        global nome_origem
        global nome_destino
        global length_max
        global cama_max
        global colunas
        global resultadao

        # print("Proccess id: ", os.getpid())

        con = psycopg2.connect(db_string)
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        with con:

            global tempo_max
            global caminho_max
            global origem_max
            global destino_max
            global nome_origem
            global nome_destino
            global length_max
            global cama_max
            global colunas
            global resultadao

            cur = con.cursor()

            origem = int(origem_destino[0])
            destino = int(origem_destino[1])

            select_string = "SELECT viagens FROM matriz_od WHERE origem=%d AND destino=%d" % (origem, destino)
            #print(select_string)
            cur.execute(select_string)
            viagens = cur.fetchall()[0][0]

            tempo = 0
            distancia = 0
            cama = []

            



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

                              

                for i in range(len(caminho) - 1):


                    cur.execute("SELECT COUNT(*) FROM edges WHERE \"from\"=%d AND \"to\"=%d" % (caminho[i], caminho[i + 1]))
                    quantidade = cur.fetchall()[0][0]

                    if quantidade > 0:

                        update_string = "SELECT length, velocidade from edges WHERE \"from\"=%d AND \"to\"=%d" % (caminho[i], caminho[i + 1])
                        # print(update_string)
                        cur.execute(update_string)
                        result = cur.fetchall()
                        length = result[0][0]
                        vel = result[0][1]
                        #print(length, vel)

                        tempo += (float(length) * 0.001 * 60 / float(vel))
                        distancia += float(length)
                        cama.append(float(length))
                        #print(tempo)

                    else:

                        update_string = "SELECT length, velocidade from edges WHERE \"from\"=%d AND \"to\"=%d" % (caminho[i + 1], caminho[i])
                        # print(update_string)
                        cur.execute(update_string)
                        result = cur.fetchall()
                        length = result[0][0]
                        vel = result[0][1]
                        #print(length, vel)

                        tempo += (float(length) * 0.001 * 60 / float(vel))
                        distancia += float(length)
                        cama.append(float(length))
                        #print(tempo)

                update_string = "SELECT nome_zona FROM centroides WHERE zona=%d" % (origem)
                cur.execute(update_string)
                l_origem = cur.fetchall()[0][0]

                update_string = "SELECT nome_zona FROM centroides WHERE zona=%d" % (destino)
                cur.execute(update_string)
                l_destino = cur.fetchall()[0][0]



                info_temp = [l_origem, l_destino, (distancia * 0.001), tempo]

                temp = pd.DataFrame([info_temp], columns=colunas)

                #print(type(temp))

                resultadao = resultadao.append(temp, ignore_index=True)

                #print(resultadao)



                if tempo > tempo_max:
                    tempo_max = tempo
                    caminho_max = caminho
                    origem_max = origem
                    destino_max = destino
                    length_max = distancia
                    cama_max = cama

                    update_string = "SELECT nome_zona FROM centroides WHERE zona=%d" % (origem_max)
                    cur.execute(update_string)

                    nome_origem = cur.fetchall()[0][0]

                    update_string = "SELECT nome_zona FROM centroides WHERE zona=%d" % (destino_max)
                    cur.execute(update_string)

                    nome_destino = cur.fetchall()[0][0]





if __name__ == "__main__":

    caminhomax = CaminhoMax()


import googlemaps
import pandas as pd
import matplotlib.pyplot as plt

arquivo_origem = '/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Trabalho/novo/dados/originais/origem.csv'
arquivo_destino = '/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Trabalho/novo/dados/originais/temp1_nodes2.csv'

start_time = time.time()

gmaps = googlemaps.Client(key='AIzaSyBXY9UJhPyOpphSNaWAxfJS6-jVeOyTuIY')


origem = pd.read_csv(arquivo_origem, sep=",")
destino = pd.read_csv(arquivo_destino, sep=",")

#print(origem)
#print(destino)

colunas = ['Origem', 'Destino', 'Distancia (km)', 'Tempo (min)']
matriz = pd.DataFrame(columns=colunas)


for index, l_origem in origem.iterrows():

    try:
    
        consulta = gmaps.distance_matrix((l_origem['y'], l_origem['x']), (destino['y'], destino['x']))
        #print(consulta)

        distancia = consulta['rows'][0]['elements'][0]['distance']['value']
        tempo = consulta['rows'][0]['elements'][0]['duration']['value']

        #print(distancia)
        #print(tempo)

    except:

        distancia = ""
        tempo = ""

    info_temp = [l_origem['nome_zona'], destino['nome_zona'][0], (distancia / 1000), (tempo / 60)]

    temp = pd.DataFrame([info_temp], columns=colunas)

    matriz = matriz.append(temp, ignore_index=True)

    #print(info_temp)

#print(matriz)

matriz.sort_values(by=['Tempo (min)'], inplace=True)

plt.scatter(matriz['Origem'], matriz['Tempo (min)'], marker='o')
plt.scatter(resultadao['Origem'], resultadao['Tempo (min)'], marker='x')
plt.xticks(matriz['Origem'], rotation='vertical')
plt.legend(('API Google Maps', 'OSMnx'))
plt.ylabel('Tempo de deslocamento (min)')

end_time = time.time()
print("\nTempo de execução do Distance Matrix API Google Maps = %s segundos." % (end_time - start_time))

plt.show()
