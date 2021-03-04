# -*- coding: utf-8 -*-

# ===================================================================================================================================

# TGT410062 - Tarefa 01
# Script para determinar o volume médio diário classificado para cada posto
# 2020-08-31

# ===================================================================================================================================

# CONFIGURAÇÕES

arquivo_sqlite = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_01/Aula_assicrona/resultados/database.sqlite"
arquivo_resultado_v_classificado = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_01/Aula_assicrona/resultados/v_classificado.csv"

# ===================================================================================================================================

import sqlite3
import time

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

classes = ["moto", "carro", "cam_leve", "cam_pesado", "especial"]

con = sqlite3.connect(arquivo_sqlite) # para conectar no bd

with con: # testa a conexao, se tiver conectado, prossegue, senao, pula (fecha automaticamente a conexao)

    cur = con.cursor() # cria o cursor para 'mexer' dentro do banco de dados

    arquivo_saida = open(arquivo_resultado_v_classificado, "w")
    arquivo_saida.write("posto;%s\n" % ";".join(classes))

    cur.execute("SELECT DISTINCT posto FROM dados ORDER BY posto")
    postos = cur.fetchall()

    for posto in postos:

        print(posto[0])
        arquivo_saida.write("%d;" % posto[0])

        volumes_classes = ""

        for classe in classes:

            cur.execute("SELECT COUNT(*) FROM dados WHERE posto=%d AND %s" % (posto[0], pega_clausula_where(classe)))
            volume_na_classe = cur.fetchall()[0][0]
            volumes_classes += "%d;" % volume_na_classe

        volumes_classes = volumes_classes[0:-1]
        arquivo_saida.write("%s\n" % volumes_classes)

       










arquivo_saida.close()
con.close()


end_time = time.time()
print("Tempo de execução = %s segundos." % (end_time - start_time))












