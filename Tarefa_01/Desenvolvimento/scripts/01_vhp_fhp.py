# -*- coding: utf-8 -*-

# ===================================================================================================================================

# TGT410062 - Tarefa 01
# Script para determinar o volume de hora de pico e o fator de hora de pico para cada posto.
# 2020-08-25

# ===================================================================================================================================

# CONFIGURAÇÕES

arquivo_sqlite = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_01/Aula_assicrona/resultados/database.sqlite"
arquivo_resultado_vhp_fhp = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_01/Aula_assicrona/resultados/vhp_fhp.csv"

# ===================================================================================================================================

import time
import sqlite3
import datetime


# ===================================================================================================================================

start_time = time.time()

con = sqlite3.connect(arquivo_sqlite)

with con:
    cur = con.cursor()

    arquivo_saida = open(arquivo_resultado_vhp_fhp, "w")
    arquivo_saida.write("posto;hora_pico;vhp;fhp\n")

    cur.execute("SELECT DISTINCT posto FROM dados ORDER BY posto")
    postos = cur.fetchall()

    for posto in postos:
        print(posto[0])

        vhp = -1
        hora_pico = ""
        fhp = -1.0

        cur.execute("SELECT MIN(date(timestamp)), MAX(date(timestamp)) FROM dados WHERE posto=%d" % posto[0])
        resultado = cur.fetchall()
        data_minima = resultado[0][0]
        data_maxima = resultado[0][1]

        # print(data_minima)
        # print(data_maxima)

        datetime_minimo = datetime.datetime.strptime(data_minima, "%Y-%m-%d")
        datetime_maximo = datetime.datetime.strptime(data_maxima, "%Y-%m-%d") + datetime.timedelta(hours=23)

        inicio = datetime_minimo
        fim = datetime_minimo

        maior_volume = 0

        while fim <= datetime_maximo:

            fim = inicio + datetime.timedelta(hours=1)


            select_string = "SELECT COUNT(*) FROM dados WHERE posto=%d AND datetime(timestamp) >= '%s' AND datetime(timestamp) < '%s'" % (posto[0], inicio, fim)
            # print(select_string)
            cur.execute(select_string)

            volume = cur.fetchall()[0][0]

            if volume > maior_volume:

                vhp = volume
                hora_pico = inicio
                maior_volume = volume

                max_15_min = 0

                for periodo in range(4):

                    inicio_periodo = inicio + datetime.timedelta(minutes=periodo * 15)
                    fim_periodo = inicio + datetime.timedelta(minutes= (periodo + 1) * 15)

                    #print("  ---  %s" % inicio_periodo)
                    #print("  ---  %s" % fim_periodo)

                    select_string = "SELECT COUNT(*) FROM dados WHERE posto=%d AND datetime(timestamp) >= '%s' AND datetime(timestamp) < '%s'" % (posto[0], inicio_periodo, fim_periodo)
                    # print(select_string)
                    cur.execute(select_string)

                    vol_15_min = cur.fetchall()[0][0]

                    if vol_15_min > max_15_min:

                        max_15_min = vol_15_min
                        fhp = vhp / (4 * max_15_min)


            # print("volume = %d" % volume)

            inicio = fim

        print("vhp = %d, encontrado em %s" % (vhp, hora_pico))

        arquivo_saida.write("%s;%s;%d;%.2f\n" % (posto[0], hora_pico, vhp, fhp))







arquivo_saida.close()
con.close()


end_time = time.time()
print("Tempo de execução = %s segundos." % (end_time - start_time))

































































