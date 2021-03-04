# -*- coding: utf-8 -*-

# ===================================================================================================================================

# TGT410062 - Tarefa 01
# Script para criar o banco de dados e preencher com o conteúdo dos arquivos CSV
# 2020-08-30

# ===================================================================================================================================

# CONFIGURAÇÕES
diretorio_dados = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_01/Aula_assicrona/dados"
arquivo_sqlite = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_01/Aula_assicrona/resultados/database.sqlite"

# ===================================================================================================================================

import sqlite3
import glob
import time

# ===================================================================================================================================

start_time = time.time()

con = sqlite3.connect(arquivo_sqlite) # para conectar no bd

with con: # testa a conexao, se tiver conectado, prossegue, senao, pula (fecha automaticamente a conexao)

    conta_registros = 0

    cur = con.cursor() # cria o cursor para 'mexer' dentro do banco de dados

    cur.execute("DROP TABLE IF EXISTS dados")
    cur.execute("CREATE TABLE dados (posto integer, faixa integer, timestamp text, velocidade real, comprimento real)")

    lista_de_arquivos = glob.glob("%s/*.csv" % diretorio_dados)

    for arquivo in lista_de_arquivos:

        posto = int(arquivo.split("/")[-1].split(".")[0].split("_")[-1])
        print("O número do posto é: %d" % posto)

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

    #cur.execute("CREATE INDEX idx_ts ON dados(timestamp")

end_time = time.time()
print("Tempo de execução = %s segundos, para inserir %d registros." % (end_time - start_time, conta_registros))