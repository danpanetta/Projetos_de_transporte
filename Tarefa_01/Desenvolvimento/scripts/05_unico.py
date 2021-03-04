# -*- coding: utf-8 -*-

# ===================================================================================================================================

# TGT410062 - Tarefa 01
# Script para criar o banco de dados e preencher com o conteúdo dos arquivos CSV
# 2020-08-30

# ===================================================================================================================================

# CONFIGURAÇÕES
diretorio_dados = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_01/Aula_assicrona/dados"
arquivo_sqlite = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_01/Aula_assicrona/resultados/database.sqlite"
arquivo_resultado_vhp_fhp = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_01/Aula_assicrona/resultados/vhp_fhp.csv"
arquivo_resultado_v_classificado = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_01/Aula_assicrona/resultados/v_classificado.csv"
arquivo_resultado_variacao_horaria = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_01/Aula_assicrona/resultados/variacao_horaria.png"
arquivo_resultado_fluxo_velocidade_15min = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_01/Aula_assicrona/resultados/fluxo_velocidade_15min.png"

# ===================================================================================================================================

import sqlite3
import glob
import time
import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ===================================================================================================================================

def pega_clausula_where(classe):

    if classe == "moto":
        clausula_where = "comprimento<3.0"
    elif classe == "carro":
        clausula_where = "comprimento>=3.0 AND comprimento<7.0"
    elif classe == "cam_leve":
        clausula_where = "comprimento>=7.0 AND comprimento<15.0"
    elif classe == "cam_pesado":
        clausula_where = "comprimento>=15.0 AND comprimento<20.0"
    elif classe == "especial":
        clausula_where = "comprimento>=20.0"
    else:
        classe == "ERRO"

    return clausula_where


# ===================================================================================================================================

start_time = time.time()

con = sqlite3.connect(arquivo_sqlite) # para conectar no bd

classes = ["moto", "carro", "cam_leve", "cam_pesado", "especial"]

with con: # testa a conexao, se tiver conectado, prossegue, senao, pula (fecha automaticamente a conexao)

# ===================================================================================================================================

# Script 00

    start_time_00 = time.time()

    print("\nExecução do script para carregar o db e preencher com os dados das planilhas (00)...")


    conta_registros = 0

    cur = con.cursor() # cria o cursor para 'mexer' dentro do banco de dados

    cur.execute("DROP TABLE IF EXISTS dados")
    cur.execute("CREATE TABLE dados (posto integer, faixa integer, timestamp text, velocidade real, comprimento real)")

    lista_de_arquivos = glob.glob("%s/*.csv" % diretorio_dados)

    for arquivo in lista_de_arquivos:

        posto = int(arquivo.split("/")[-1].split(".")[0].split("_")[-1])
        # print("O número do posto é: %d" % posto)

        conta_linha = 0

        for linha in open(arquivo, "r").readlines():

            if conta_linha > 0:
                partes = linha.split(";")
        
                faixa = int(partes[0])
                timestamp = partes[1]
                velocidade = float(partes[2])
                comprimento = float(partes[3])

                insert_string = "INSERT INTO dados (posto, faixa, timestamp, velocidade, comprimento) VALUES (%d, %d, '%s', %f, %f)" % (posto, faixa, timestamp, velocidade, comprimento)


                cur.execute(insert_string)
                conta_registros += 1
        
            conta_linha += 1

    end_time_00 = time.time()
    print("Tempo de execução do script 00 = %.2f segundos.\n" % (end_time_00 - start_time_00))

    #cur.execute("CREATE INDEX idx_ts ON dados(timestamp")

# ===================================================================================================================================

# Script 01

    start_time_01 = time.time()
    print("Execução do script para determinar o volume de hora de pico e o fator de hora de pico (01)...")

    arquivo_saida = open(arquivo_resultado_vhp_fhp, "w")
    arquivo_saida.write("posto;hora_pico;vhp;fhp\n")

    cur.execute("SELECT DISTINCT posto FROM dados ORDER BY posto")
    postos = cur.fetchall()

    for posto in postos:
        #print(posto[0])

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

        # print("vhp = %d, encontrado em %s" % (vhp, hora_pico))

        arquivo_saida.write("%s;%s;%d;%.2f\n" % (posto[0], hora_pico, vhp, fhp))

    end_time_01 = time.time()
    print("Tempo de execução do script 01 = %.2f segundos.\n" % (end_time_01 - start_time_01))

# ===================================================================================================================================

# Script 02

    start_time_02 = time.time()
    print("Execução do script para determinar o volume médio diário classificado (02)...")

    arquivo_saida = open(arquivo_resultado_v_classificado, "w")
    arquivo_saida.write("posto;%s\n" % ";".join(classes))

    cur.execute("SELECT DISTINCT posto FROM dados ORDER BY posto")
    postos = cur.fetchall()

    for posto in postos:

        # print(posto[0])
        arquivo_saida.write("%d;" % posto[0])

        volumes_classes = ""

        for classe in classes:

            cur.execute("SELECT COUNT(*) FROM dados WHERE posto=%d AND %s" % (posto[0], pega_clausula_where(classe)))
            volume_na_classe = cur.fetchall()[0][0]
            volumes_classes += "%d;" % volume_na_classe

        volumes_classes = volumes_classes[0:-1]
        arquivo_saida.write("%s\n" % volumes_classes)


    end_time_02 = time.time()
    print("Tempo de execução do script 02 = %.2f segundos.\n" % (end_time_02 - start_time_02))


# ===================================================================================================================================

# Script 03

    
    start_time_03 = time.time()
    print("Execução do script para criar gráficos de variação horária, com uma curva por posto (03)...")

    cur.execute("SELECT date(timestamp) FROM dados")
    data = cur.fetchall()[0][0]

    cur.execute("SELECT DISTINCT posto FROM dados ORDER BY posto")
    postos = cur.fetchall()

    for posto in postos:

        # print(posto[0])

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

            # print(horas)
            # print(volumes)

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

    plt.clf()

    end_time_03 = time.time()
    print("Tempo de execução do script 03 = %.2f segundos.\n" % (end_time_03 - start_time_03))

# ===================================================================================================================================

# Script 04

    start_time_04 = time.time()
    print("Execução do script para gerar um arquivo de imagem contendo um gráfico de pontos com a relação\nentre fluxo e velocidade com agregação temporal de 15 minutoso (04)...")

    cur.execute("SELECT date(timestamp) FROM dados")
    data = cur.fetchall()[0][0]

    cur.execute("SELECT DISTINCT posto FROM dados ORDER BY posto")
    postos = cur.fetchall()

    for posto in postos:

        # print(posto[0])

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

    end_time_04 = time.time()
    print("Tempo de execução do script 04 = %.2f segundos.\n" % (end_time_04 - start_time_04))


arquivo_saida.close()
con.close()


end_time = time.time()
print("Tempo de execução total = %.4f segundos, para trabalhar com %d registros.\n" % (end_time - start_time, conta_registros))
