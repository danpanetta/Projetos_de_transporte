# -*- coding: utf-8 -*-

# ===============================================
# TGT410062 - Tarefa 03
# Script para passar os dados de matriz OD do arquivo CSV para o banco de dados (PostgreSQL).
# 2020-09-11
# ===============================================
# CONFIGURACOES

arquivo_od = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_03/Material/dados/dados_entrada/matriz_OD_07h.csv"
arquivo_temporario = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_03/Material/dados/dados_processados/temp.csv"
arquivo_shape_centroides = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_03/Material/dados/dados_entrada/centroides/centroides.shp"
arquivo_shape_edges = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_03/Material/dados/dados_processados/shapefiles/edges.shp"
db_string = "host=localhost dbname=programacao user=danilo password=dan"

# ===============================================

import time
import psycopg2
import subprocess

# ===============================================

def executa( input ):

	print( input )
	subprocess.run( input, shell=True )
			
# ===============================================

class ProcessaOD():

	def __init__( self ):

		start_time = time.time()

		con = psycopg2.connect( db_string )
		con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

		with con:
		
			temp_file = open( arquivo_temporario, "w" )

			cur = con.cursor()
			
			# Objetivo específico 2
			
			cur.execute( "DROP TABLE IF EXISTS matriz_od" )
			cur.execute( "CREATE TABLE matriz_od ( origem integer, destino integer, viagens integer )" )
			
			lines = open( arquivo_od, "r" ).readlines()
			
			centroides = lines[0].split( "," )[1:]
			
			for line in lines[1:]:
			
				partes = line.split( "," )
				
				origem = partes[0]
				
				for i, destino in enumerate( centroides ):
				
					#cur.execute( "INSERT INTO matriz_OD (origem, destino, viagens) VALUES (%d, %d, %d)" % ( int( origem ), int( destino ), int( partes[i+1] )  ) )
					temp_file.write( "%d,%d,%d\n" % ( int( origem ), int( destino ), int( partes[i+1] ) ) )
		
			temp_file.close()
		
			#cur.execute( "COPY matriz_od FROM '%s'" % arquivo_temporario )
			#executa( "psql -U alexandre -d programacao -c \"\\COPY matriz_OD FROM '%s' WITH DELIMITER ','\"" % arquivo_temporario )
			
			temp_file = open( arquivo_temporario, "r" )
			
			cur.copy_from( temp_file, "matriz_od", sep="," )
			
			
			# Objetivo específico 3
			
			cur.execute( "DROP TABLE IF EXISTS centroides" )
			#executa( "shp2pgsql -S \"%s\" centroides | psql -d programacao" % arquivo_shape_centroides )
			
			executa( "ogr2ogr -f \"PostgreSQL\" PG:\"%s\" \"%s\"" % ( db_string, arquivo_shape_centroides ) )
			
				
			cur.execute( "DROP TABLE IF EXISTS edges" )
			executa( "ogr2ogr -f \"PostgreSQL\" PG:\"%s\" \"%s\"" % ( db_string, arquivo_shape_edges ) )
			cur.execute( "ALTER TABLE edges ADD COLUMN volume integer" )
			
			
		
		
		end_time = time.time()
		print( "Tempo de execucao = %s segundos." % ( end_time - start_time ) )

		
		
if __name__ == "__main__":

	processaOD = ProcessaOD()
		
		
		
		