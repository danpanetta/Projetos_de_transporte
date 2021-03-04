# -*- coding: utf-8 -*-
# ===============================================
# TGT410062 - Tarefa 03
# Script para fazer a alocação.
# 2020-09-12
# ===============================================
# CONFIGURACOES

db_string = "host=localhost dbname=programacao user=danilo password=dan"
arquivo_grafo = "/Users/danilopanettadefaria/Documentos/Projects/TGT410062/Tarefa_03/Material/dados/dados_processados/mynetwork.graphml"
n_processos = 8
# ===============================================
import psycopg2
import networkx as nx
import osmnx as ox
ox.config(use_cache=True, log_console=True)
ox.__version__
from entidades.ParOD import ParOD
from multiprocessing.pool import ThreadPool
import os
import time
# ===============================================


class Alocacao():

	G = None
	
	def __init__( self ):
	
		global G
		
		start_time = time.time()
	
		con = psycopg2.connect( db_string )
		con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

		with con:
		
			cur = con.cursor()
			
			G = ox.load_graphml( arquivo_grafo )
			
			cur.execute( "SELECT zona FROM centroides ORDER BY zona" )
			zonas = cur.fetchall()
			
			cur.execute( "UPDATE edges SET volume=0" )
			
			pares_od = []
			
			for origem in zonas:
				
				for destino in zonas:
					
					if destino != origem:
								
						pares_od.append( [ origem[0], destino[0] ] )
						
						
			#print( pares_od )
						
			pool = ThreadPool( n_processos )
			pool.map( self.__processaCM, pares_od )
			pool.close()
			pool.join()
			
		end_time = time.time()
		print( "Tempo de execucao = %s segundos." % ( end_time - start_time ) )
				
				
				
	def __processaCM( self, origem_destino ):
	
		global G
	
		#print("Proccess id: ", os.getpid())
	
		con = psycopg2.connect( db_string )
		con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

		with con:
		
			cur = con.cursor()
			
			origem = int( origem_destino[0] )
			destino = int( origem_destino[1] )
			
			select_string = "SELECT viagens FROM matriz_od WHERE origem=%d AND destino=%d" % ( origem, destino )
			#print( select_string )
			cur.execute( select_string )
			viagens = cur.fetchall()[0][0]
			
			if viagens > 0:
				
				#print( viagens )
				#print( "origem = %d" % origem )
				#print( "destino = %d" % destino )
				

				cur.execute( "SELECT ST_X(wkb_geometry), ST_Y(wkb_geometry) FROM centroides WHERE zona=%d" % origem )
				result = cur.fetchall()
				origem_X = result[0][0] 
				origem_Y = result[0][1]
				

				cur.execute( "SELECT ST_X(wkb_geometry), ST_Y(wkb_geometry) FROM centroides WHERE zona=%d" % destino )
				result = cur.fetchall()
				destino_X = result[0][0] 
				destino_Y = result[0][1]

				#print( origem_X )

				parOD = ParOD( G, origem_X, origem_Y, destino_X, destino_Y )
				parOD.processa_CM()

				caminho = parOD.get_CM()
				
				#print( caminho )

				for i in range( len( caminho ) - 1 ):
				
				
					cur.execute( "SELECT COUNT(*) FROM edges WHERE \"from\"=%d AND \"to\"=%d" % ( caminho[i], caminho[i+1] ) )
					quantidade = cur.fetchall()[0][0]
					
					if quantidade > 0:
				
						update_string = "UPDATE edges SET volume=volume+%d WHERE \"from\"=%d AND \"to\"=%d" % ( viagens, caminho[i], caminho[i+1] )
						#print( update_string )
						cur.execute( update_string )
					
					else:
						
						update_string = "UPDATE edges SET volume=volume+%d WHERE \"from\"=%d AND \"to\"=%d" % ( viagens, caminho[i+1], caminho[i] )
						#print( update_string )
						cur.execute( update_string )
		
	
	
	
	
		
		
if __name__ == "__main__":

	alocacao = Alocacao()
