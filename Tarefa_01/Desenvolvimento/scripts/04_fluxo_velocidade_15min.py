# -*- coding: utf-8 -*-

# ===================================================================================================================================

# TGT410062 - Tarefa 01
# Script para gerar um arquivo de imagem contendo um gráfico de pontos com a relação entre fluxo e velocidade com agregação temporal de 15 minutos para todos os postos.
# 2020-08-31

# ===================================================================================================================================

# CONFIGURAÇÕES

arquivo_sqlite = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_01/Aula_assicrona/resultados/database.sqlite"
arquivo_resultado_fluxo_velocidade_15min = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_01/Aula_assicrona/resultados/fluxo_velocidade_15min.png"

# ===================================================================================================================================

import time
import sqlite3
import datetime
import matplotlib.pyplot as plt

# ===================================================================================================================================

start_time = time.time()

con = sqlite3.connect(arquivo_sqlite)

with con:

    cur = con.cursor()

    cur.execute("SELECT date(timestamp) FROM dados")
    data = cur.fetchall()[0][0]

    cur.execute("SELECT DISTINCT posto FROM dados ORDER BY posto")
    postos = cur.fetchall()

    for posto in postos:

        print(posto[0])

        fluxos = []
        velocidades = []

        for hora in range(24):

            inicio = datetime.datetime.strptime(data, "%Y-%m-%d") + datetime.timedelta(hours=hora)

            for quarter in range(4):

                inicio_periodo = inicio + datetime.timedelta(minutes=quarter * 15)
                fim_periodo = inicio + datetime.timedelta(minutes= (quarter + 1) * 15)

                select_string = "SELECT COUNT(*), AVG(velocidade) FROM dados WHERE posto=%d AND datetime(timestamp) >= '%s' AND datetime(timestamp) < '%s'" % (posto[0], inicio_periodo, fim_periodo)
                cur.execute(select_string)

                resultado = cur.fetchall()
                volume = resultado[0][0]
                velocidade = resultado[0][1]

                fluxos.append(volume)
                velocidades.append(velocidade)

        plt.plot(fluxos, velocidades, "o", label=posto[0])

    plt.xlabel("Fluxo (veh/15 min)")
    plt.ylabel("Velocidade (km/h)")
    plt.ylim(bottom = 0, top = 100)
    plt.grid(True)
    plt.legend(loc="center left", bbox_to_anchor=(1, 0.5), numpoints=1)
    # plt.title("Fluxo x Velocidade")

    plt.savefig(arquivo_resultado_fluxo_velocidade_15min, bbox_inches="tight", dpi=200)
    
con.close()


end_time = time.time()
print("Tempo de execução = %s segundos." % (end_time - start_time))
