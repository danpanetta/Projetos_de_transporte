#!/usr/bin/env python
# coding: utf-8

# In[59]:


# Script para passar os arquivos CSV para o SQLite

# Configuracoes =====================================

dir_arquivos_dados = "/Users/danilo/Documentos/Projects/TGT410062/Tarefa_01/Material/01_Dados_de_entrada/dados_0902"
dir_resultado = "/Users/danilo/Documentos/Projects/TGT410062/Tarefa_01/Material/04_Dados_saida"


# Imports ===========================================

import glob
import sqlite3
import time

# ===================================================

start_time = time.time()

print( "Processando..." )

con = sqlite3.connect( "%s/banco_de_dados.sqlite" % dir_resultado )

with con:

    cur = con.cursor()
    
    cur.execute( "DROP TABLE IF EXISTS medicoes" )
    cur.execute( "CREATE TABLE medicoes (posto integer, faixa integer, timestamp text, velocidade float, comprimento float)" )

    lista_de_arquivos = glob.glob( "%s/*.csv" % dir_arquivos_dados )

    for arquivo in lista_de_arquivos:

        print( arquivo )
        
        posto = int( arquivo.split( "." )[0].split( "_" )[-1] )
        
        print( posto )
        
        input_file = open( arquivo, "r" )
        
        contador = 100
        for line in input_file.readlines():
            
            if contador > 100:
                #print( line )
            
                partes = line.split( ";" )
                faixa = int( partes[0] )
                timestamp = partes[1]
                velocidade = float( partes[2] )
                comprimento = float( partes[3] )
        
                cur.execute( "INSERT INTO medicoes (posto,faixa,timestamp,velocidade,comprimento) VALUES (%d,%d,'%s',%f,%f)" % ( posto, faixa, timestamp, velocidade, comprimento ) )
    
            contador = contador + 1

end_time = time.time()
print( "Tempo de processamento = %s segundos" % ( end_time - start_time ) )


# In[2]:


# Script para determinar VHP e FHP

# Configuracoes =====================================
arquivo_bd = "/Users/danilo/Documentos/Projects/TGT410062/Tarefa_01/Material/04_Dados_saida/banco_de_dados.sqlite"
arquivo_resultado = "/Users/danilo/Documentos/Projects/TGT410062/Tarefa_01/Material/04_Dados_saida/vhp_fhp.csv"
# Imports ===========================================

import sqlite3
import time
import datetime

# ===================================================

start_time = time.time()

saida = open( arquivo_resultado, "w" )
saida.write( "posto;inicio;vhp;fhp\n" )

con = sqlite3.connect( arquivo_bd )


with con:

    cur = con.cursor()
    
    cur.execute( "SELECT DISTINCT posto FROM medicoes ORDER BY posto" )
    postos = cur.fetchall()
    print(postos)
 
    for posto in postos:
        vol_max = 0
        inicio_max = 0
        fim_max = 0
        vol_max_quarter = 0
        
        print( "posto = %s" % posto )
    
        cur.execute( "SELECT MIN( date( timestamp ) ) FROM medicoes WHERE posto=%s" % posto[0] )
        min_date = cur.fetchall()[0][0]
        # print( min_date )
        
        inicio_posto = datetime.datetime.strptime( min_date, "%Y-%m-%d" )
        
        for hora in range( 24 ):
        
            inicio = inicio_posto + datetime.timedelta( hours=( hora ) )
            fim = inicio_posto + datetime.timedelta( hours=( hora + 1 ) )
            
            select_string = "SELECT COUNT(*) FROM medicoes WHERE posto=%s AND datetime( timestamp )>='%s' AND datetime( timestamp )<'%s'" % ( posto[0], inicio, fim )
            # print( select_string )
            cur.execute( select_string )
            volume = cur.fetchall()[0][0]
        
            # print( "inicio = %s" % inicio )
            # print( "fim = %s" % fim )
            # print( "volume = %s" % volume )
            
            if volume > vol_max:
                vol_max = volume
                inicio_max = inicio
                fim_max = fim

        for quarter in [0, 15, 30, 45]:
            inicio = inicio_max + datetime.timedelta(minutes=(quarter))
            fim = inicio_max + datetime.timedelta(minutes=(quarter + 15))

            select_string = "SELECT COUNT(*) FROM medicoes WHERE posto=%s AND datetime( timestamp )>='%s' AND datetime( timestamp )<'%s'" % (
            posto[0], inicio, fim)
            # print( select_string )
            cur.execute(select_string)
            volume_quarter = cur.fetchall()[0][0]

            if volume_quarter > vol_max_quarter:
                vol_max_quarter = volume_quarter
            
        resultado = [posto, inicio_max.strftime("%Y-%m-%d %H"), vol_max, vol_max_quarter]
        saida = open( arquivo_resultado, "a" )
        saida.write( '%s;%s;%d;%.3f\n' % (posto[0], inicio_max, vol_max, vol_max/(4 * vol_max_quarter)) )
        print(resultado)

end_time = time.time()
print( "Tempo de processamento = %s segundos" % ( end_time - start_time ) )


