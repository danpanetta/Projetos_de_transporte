# -*- coding: utf-8 -*-

# ===================================================================================================================================

# TGT410062 - Tarefa 01
# Script para criar graficos de variacao horaria, com uma curva por posto.
# 2020-08-31

# ===================================================================================================================================

# CONFIGURAÇÕES

arquivo_sqlite = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_01/Aula_assicrona/resultados/database.sqlite"
arquivo_resultado_variacao_horaria = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_01/Aula_assicrona/resultados/variacao_horaria.png"

# ===================================================================================================================================

import time
import sqlite3
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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

        horas = []

        volumes = []

        for hora in range(24):

            inicio = datetime.datetime.strptime(data, "%Y-%m-%d") + datetime.timedelta(hours=hora)
            fim = datetime.datetime.strptime(data, "%Y-%m-%d") + datetime.timedelta(hours=hora + 1)

            select_string = "SELECT COUNT(*) FROM dados WHERE posto=%d AND datetime(timestamp) >= '%s' AND datetime(timestamp) < '%s'" % (posto[0], inicio, fim)
            # print(select_string)
            cur.execute(select_string)

            volume = cur.fetchall()[0][0]

            horas.append(inicio)
            volumes.append(volume)

            print(horas)
            print(volumes)

        plt.plot_date(horas, volumes, "-o", label=posto[0])

    plt.xlabel("Tempo (h)")
    plt.ylabel("Volume (veh/h)")
    plt.ylim(bottom = 0)
    #plt.xlim([data, (data + datetime.timedelta(hours=24))])
    plt.grid(True)
    plt.legend(loc="center left", bbox_to_anchor=(1, 0.5), numpoints=1)

    formato_h_m = mdates.DateFormatter("%H:%M")
    plt.gca().xaxis.set_major_formatter(formato_h_m)


    plt.savefig(arquivo_resultado_variacao_horaria, bbox_inches="tight", dpi=200)

con.close()


end_time = time.time()
print("Tempo de execução = %s segundos." % (end_time - start_time))
