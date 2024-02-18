import geopandas as gpd
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine
from shapely.geometry import MultiPolygon

datetime1=datetime.now()
print(datetime1)


#------------------------------------------------------Ler pontos-----------------------------------------
shp="path/shapefile.shp"
pts=gpd.read_file(shp)
pts=pts.to_crs('epsg:4326')
#----------------------------------------------------- Buffers----------------------------------------
buffer1=pts.buffer(100000)
buffer1=buffer1.to_crs('epsg:4326')
buffer1=gpd.GeoDataFrame({'geometry': buffer1, 'id_polygon':pts["id_table"]})
buffer1['peso_buffer']=3

buffer2=pts.buffer(200000)
buffer2=buffer2.to_crs('epsg:4326')
buffer2=buffer2.difference(buffer1, align=True)
buffer2=gpd.GeoDataFrame({'geometry': buffer2, 'id_polygon':pts["id_table"]})
buffer2['peso_buffer']=2

buffer3=pts.buffer(300000)
buffer3=buffer3.to_crs('epsg:4326')
buffer3=buffer3.difference(buffer2.union(buffer1, align=True), align=True)
buffer3=gpd.GeoDataFrame({'geometry': buffer3, 'id_polygon':pts["id_table"]})
buffer3['peso_buffer']=1

buffer=pd.concat([pd.concat([buffer1, buffer2]), buffer3])
buffer.to_file('path/shape.shp')
#buffer.plot()
#buffer
datetime2=datetime.now()
print("lerpontos e buffer:", datetime2)
#-------------------------------------------------Critérios para recorte-------------------------------------
#--------------------------------------------Conexão ao bd ------------------------------------
db_connection_url = "postgresql://user:password@host:port/bd"
conn = create_engine(db_connection_url)
#------------------------------------------------TI---------------------------------------------
dados_ti=pd.DataFrame()
sql = "SELECT * FROM public.bd"
df = gpd.read_postgis(sql, conn)
dados_ti = pd.concat([dados_ti, df])
b_ti=buffer.overlay(dados_ti, how='intersection')
b_ti=b_ti.to_crs('epsg:4326')
#------------------------------------------------UC-----------------------------------------------
dados_uc=pd.DataFrame()
sql = "SELECT * FROM public.bd WHERE grupo ='xx' AND sit='ok'"
df = gpd.read_postgis(sql, conn)
dados_uc = pd.concat([dados_uc, df])
b_uc=buffer.overlay(dados_uc, how='intersection')

buffer=buffer.overlay(b_ti, how='difference')
buffer=buffer.overlay(b_uc, how='difference')
datetime3=datetime.now()
print("ti e uc: ",datetime3)
#----------------------------------------------- Buscar CARs---------------------------------------------------
#db_connection_url2 = "postgresql://user:password@host:port/bd"
db_connection_url2 = "postgresql://user:password@host:port/bd"
conn2 = create_engine(db_connection_url2)
dados_car=pd.DataFrame()

#iterating over rows and getting CAR information (geom too)
sql = "SELECT * FROM public.table WHERE condicao_i NOT LIKE 'C%%' AND (uuf = 'AC' OR (uuf = 'AM' OR (uuf = 'RO' OR (uuf = 'RR'OR (uuf = 'PA'OR (uuf = 'MA'OR (uuf = 'AP'OR (uuf = 'TO'OR (uuf = 'MT')))))))))"
#sql = "SELECT * FROM public.area_imovel WHERE condicao_i NOT LIKE 'C%%' AND (uuf = 'PA' )" #OR (uuf = 'RO'))"
df = gpd.read_postgis(sql, conn2)
dados_car = pd.concat([dados_car, df])

#dados_car.to_csv('path/dados.csv', index=False)
test1=dados_car.overlay(buffer, how='intersection')
dados_car["imovel"]=dados_car["cd_im"]
test2=test1["cd_im"]
test2=list(dict.fromkeys(test2))
dict_car=dados_car.set_index('cd_im').T.to_dict('list')
a=[dict_car[k] for k in test2]
b=pd.DataFrame.from_dict(a)
farms_buffer=gpd.GeoDataFrame(b, geometry=8)
farms_buffer["cd_im"]=farms_buffer[10]
farms_buffer["area_ha"]=farms_buffer[1]
farms_buffer["uf"]=farms_buffer[2]
farms_buffer["mun"]=farms_buffer[3]
farms_buffer["mod"]=farms_buffer[4]
farms_buffer["situacao"]=farms_buffer[7]
farms_buffer["geometry"]=farms_buffer[8]
farms_buffer.drop(columns=[0,1,2,3,4,5,6,7,8,9,10])
farms_buffer=farms_buffer.set_geometry("geometry")
farms_buffer=farms_buffer.set_crs('epsg:4326')

#------------------------------------------------- esvaziando as variáveis---------------------
df=pd.DataFrame()
a=dict()
dict_car=dict()
b=pd.DataFrame()
test1=pd.DataFrame()
dados_farms=pd.DataFrame()

datetime4=datetime.now()
print("busca CAR:", datetime4)
#-------------------------------------- Municipios de interesse --------------------------------- 
shp="path/BR_Municipios_2020.shp"
mun=gpd.read_file(shp)

#----------------------------------------------- Selecionando os Municipios de interesse-------------------
test1=mun.overlay(buffer, how='intersection')
mun["CD_MUN2"]=mun["CD_MUN"]
test2=test1["CD_MUN"]
test2=list(dict.fromkeys(test2))
dict_mun=mun.set_index('CD_MUN').T.to_dict('list')
a=[dict_mun[k] for k in test2]
b=pd.DataFrame.from_dict(a)
mun_buffer=gpd.GeoDataFrame(b, geometry=4)
mun_buffer["nm_mun"]=mun_buffer[0]
mun_buffer["uf"]=mun_buffer[1]
mun_buffer["area_km2"]=mun_buffer[2]
mun_buffer["cd_mun"]=mun_buffer[5]
mun_buffer["geometry"]=mun_buffer[4]
mun_buffer.drop(columns=[0,1,2,3,4,5])
mun_buffer=mun_buffer.set_geometry("geometry")
mun_buffer=mun_buffer.set_crs('epsg:4326')

#------------------------------------------------- esvaziando as variáveis---------------------
df=pd.DataFrame()
a=dict()
dict_mun=dict()
b=pd.DataFrame()
test1=pd.DataFrame()

datetime5=datetime.now()
print("municipios de interesse:", datetime5)
#------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------
#------------------ESTA ETAPA DE E BUSCAR A ÁREA DE PASTAGEM DO BANCO-----------------
#------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------

#---------------------------------------- Busca área de pastagem----------------
db_connection_url3 =""postgresql://user:password@host:port/bd""
conn3 = create_engine(db_connection_url3)
dados_pastagem=pd.DataFrame()
b19_pastagem=pd.DataFrame()
m19_pastagem=pd.DataFrame()
b20_pastagem=pd.DataFrame()
m20_pastagem=pd.DataFrame()
b21_pastagem=pd.DataFrame()
m21_pastagem=pd.DataFrame()

#iterating over rows on sheets and getting CAR information (geom too)
estados=['am','ac','ro','rr','pa','ma','ap','to','mt']

#geometry=farms_buffer["geometry"]
for j in estados:
    db_connection_url3 ="postgresql://user:password@host:port/bd"
    conn3 = create_engine(db_connection_url3)
    #for i in geometry:
        #---------------------------------------------2021-------------------------------
    sql1 = "SELECT * FROM bd.br_"+j+"_uso19_a WHERE class=15" # AND ST_Intersects(br_"+j+"_uso19_a.geom,"+"'"+str(i)+"')"
    df = gpd.read_postgis(sql1, conn3)
    dados_pastagem = pd.concat([dados_pastagem, df], ignore_index=True)
    #------------------------- pastagem nos CARS--------------------------
    i_pastagem=farms_buffer.overlay(dados_pastagem, how='intersection')
    b19_pastagem = pd.concat([b19_pastagem, i_pastagem], ignore_index=True)
    #--------------------------pastagem nos Municipios--------------------
    mun_pastagem=mun_buffer.overlay(dados_pastagem, how='intersection')
    m19_pastagem= pd.concat([m19_pastagem, mun_pastagem], ignore_index=True)
    #------------------------- Limpando variáveis------------------------
    dados_pastagem=pd.DataFrame()
    i_pastagem=pd.DataFrame()
    mun_pastagem=pd.DataFrame()
    df=pd.DataFrame()
        
    #---------------------------------------------2021-------------------------------    
    sql2 = "SELECT * FROM bd.br_"+j+"_uso20_a WHERE class=15" # AND ST_Intersects(br_"+j+"_uso19_a.geom,"+"'"+str(i)+"')"
    df = gpd.read_postgis(sql2, conn3)
    dados_pastagem = pd.concat([dados_pastagem, df], ignore_index=True)
    #------------------------- pastagem nos CARS--------------------------
    i_pastagem=farms_buffer.overlay(dados_pastagem, how='intersection')
    b20_pastagem = pd.concat([b20_pastagem, i_pastagem], ignore_index=True)
    #--------------------------pastagem nos Municipios--------------------
    mun_pastagem=mun_buffer.overlay(dados_pastagem, how='intersection')
    m20_pastagem= pd.concat([m20_pastagem, mun_pastagem], ignore_index=True)
    #------------------------- Limpando variáveis------------------------
    dados_pastagem=pd.DataFrame()
    i_pastagem=pd.DataFrame()
    mun_pastagem=pd.DataFrame()
    df=pd.DataFrame()
        
    #---------------------------------------------2021-------------------------------
    sql3 = "SELECT * FROM bd.br_"+j+"_uso21_a WHERE class=15" # AND ST_Intersects(br_"+j+"_uso19_a.geom,"+"'"+str(i)+"')"
    df = gpd.read_postgis(sql3, conn3)
    dados_pastagem = pd.concat([dados_pastagem, df], ignore_index=True)
    #------------------------- pastagem nos CARS--------------------------
    i_pastagem=farms_buffer.overlay(dados_pastagem, how='intersection')
    b21_pastagem = pd.concat([b21_pastagem, i_pastagem], ignore_index=True)
    #--------------------------pastagem nos Municipios--------------------
    mun_pastagem=mun_buffer.overlay(dados_pastagem, how='intersection')
    m21_pastagem= pd.concat([m21_pastagem, mun_pastagem], ignore_index=True)
    #------------------------- Limpando variáveis------------------------
    dados_pastagem=pd.DataFrame()
    i_pastagem=pd.DataFrame()
    mun_pastagem=pd.DataFrame()
    df=pd.DataFrame()        
        
#------------------------- calculando área de pastagem para cada CAR 2019-----------------        
#b19_pastagem=gpd.GeoDataFrame(b19_pastagem, geometry='geometry')
b19_pastagem= b19_pastagem.to_crs('epsg:9822')       
b19_pastagem['pastaFarm19_ha']=((b19_pastagem['geometry'].area)/10000)
b19_pastagem= b19_pastagem.to_crs('epsg:4326')
b19_pastagem =b19_pastagem[["cd_im", "pastaFarm19_ha"]].groupby("cd_im").sum()
#------------------------- calculando área de pastagem para cada Municipio 2019-----------------    
#m19_pastagem=gpd.GeoDataFrame(m19_pastagem, geometry='geometry')
m19_pastagem=gpd.GeoDataFrame(m19_pastagem, geometry='geometry')
m19_pastagem= m19_pastagem.to_crs('epsg:9822')       
m19_pastagem['pastaMun19_ha']=((m19_pastagem['geometry'].area)/10000)
m19_pastagem= m19_pastagem.to_crs('epsg:4326')
m19_pastagem =m19_pastagem[["cd_mun", "pastaMun19_ha"]].groupby("cd_mun").sum()


#------------------------- calculando área de pastagem para cada CAR 2020 -----------------        
#b20_pastagem=gpd.GeoDataFrame(b20_pastagem, geometry='geometry')
b20_pastagem= b20_pastagem.to_crs('epsg:9822')       
b20_pastagem['pastaFarm20_ha']=((b20_pastagem['geometry'].area)/10000)
b20_pastagem= b20_pastagem.to_crs('epsg:4326')
b20_pastagem =b20_pastagem[["cd_im", "pastaFarm20_ha"]].groupby("cd_im").sum()

#------------------------- calculando área de pastagem para cada Municipio 2020-----------------    
#m20_pastagem=gpd.GeoDataFrame(m20_pastagem, geometry='geometry')
m20_pastagem= m20_pastagem.to_crs('epsg:9822')       
m20_pastagem['pastaMun20_ha']=((m20_pastagem['geometry'].area)/10000)
m20_pastagem= m20_pastagem.to_crs('epsg:4326')
m20_pastagem =m20_pastagem[["cd_mun", "pastaMun20_ha"]].groupby("cd_mun").sum()

#------------------------- calculando área de pastagem para cada CAR 2021 -----------------        
#b21_pastagem=gpd.GeoDataFrame(b21_pastagem, geometry='geometry')
b21_pastagem= b21_pastagem.to_crs('epsg:9822')       
b21_pastagem['pastaFarm21_ha']=((b21_pastagem['geometry'].area)/10000)
b21_pastagem= b21_pastagem.to_crs('epsg:4326')
b21_pastagem =b21_pastagem[["cd_im", "pastaFarm21_ha"]].groupby("cd_im").sum()

#------------------------- calculando área de pastagem para cada Municipio 2021-----------------    
#m21_pastagem=gpd.GeoDataFrame(m21_pastagem, geometry='geometry')
m21_pastagem= m21_pastagem.to_crs('epsg:9822')       
m21_pastagem['pastaMun21_ha']=((m21_pastagem['geometry'].area)/10000)
m21_pastagem= m21_pastagem.to_crs('epsg:4326')
m21_pastagem =m21_pastagem[["cd_mun", "pastaMun21_ha"]].groupby("cd_mun").sum()

datetime6=datetime.now()
print("get pastagem:", datetime6)

#------------------------------------------------------------------------------------
#-------------------------------------------------Critérios-------------------------------------
#--------------------------------------------Conexão ao bd -------------------------------------
db_connection_url = ""postgresql://user:password@host:port/bd""
conn = create_engine(db_connection_url)
#-----------------------------Buscando Desmatamento - Paramentros---------------------------------
desmatamento=pd.DataFrame()
b_desmatamento=pd.DataFrame()
years=[2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022]
#------------------------------------------- Buscando no banco ----------------------
for i in years:
    #sql = "SELECT * FROM public.bd"+str(i)+"_a WHERE area >= 6.25"
    sql = "SELECT * FROM public.bd"+str(i)+"_a"
    df = gpd.read_postgis(sql, conn)
    desmatamento = pd.concat([desmatamento, df], ignore_index=True)
#----------------------------------------intersecção com os CArs----------------------
    #i_desmatamento=buffer.overlay(desmatamento, how='intersection')
    #i_desmatamento=i_desmatamento.overlay(farms_buffer, how='intersection')
    i_desmatamento=desmatamento.overlay(farms_buffer, how='intersection')
    b_desmatamento = pd.concat([b_desmatamento, i_desmatamento], ignore_index=True)
#--------------------------------esvaziando variáveis----------------------
    desmatamento=pd.DataFrame()
    i_desmatamento=pd.DataFrame()
    df=pd.DataFrame()
#-------------------------------calculando área----------------------
b_desmatamento= b_desmatamento.to_crs('epsg:9822')
b_desmatamento['area_ha']=((b_desmatamento['geometry'].area)/10000)
b_desmatamento= b_desmatamento.to_crs('epsg:4326')
b_desmatamento['areadesmatamento_ha']=0.0 #variável de área ponderada
#----------calculando área ponderada pelo ano (>2019 = 2 ) e pelo buffer----------------------
for j in range(len(b_desmatamento)):
    if b_desmatamento.ano[j] >2019:
        b_desmatamento.areadesmatamento_ha[j]=b_desmatamento.area_ha[j]*2#*b_desmatamento.peso_buffer[j]
    if b_desmatamento.ano[j] <=2019:
        b_desmatamento.areadesmatamento_ha[j]=b_desmatamento.area_ha[j]#*b_desmatamento.peso_buffer[j]
        
#--------------------------------somando área pelo CAR---------------------
d_desmatamento=b_desmatamento[["cd_im", "areadesmatamento_ha"]].groupby("cd_im").sum()
#------------------------- Limpando variáveis------------------------
b_desmatamento=pd.DataFrame()


#---------------------------------------ICMBIO----------------------------------------------    
dados_icmbio=pd.DataFrame()
sql = "SELECT * FROM public.embargos"
df = gpd.read_postgis(sql, conn)
dados_icmbio= pd.concat([dados_icmbio, df])
b_icmbio=farms_buffer.overlay(dados_icmbio, how='intersection')
#b_icmbio=buffer.overlay(dados_icmbio, how='intersection')
#b_icmbio=b_icmbio.overlay(farms_buffer, how='intersection')
#--------------------------------calculando área pelo car---------------------
b_icmbio= b_icmbio.to_crs('epsg:9822')
b_icmbio['area_icmbio']=((b_icmbio['geometry'].area)/10000)#*b_icmbio['peso_buffer']
b_icmbio= b_icmbio.to_crs('epsg:4326')
#--------------------------------somando área pelo CAR---------------------
d_icmbio=b_icmbio[["cd_im", "area_icmbio"]].groupby("cd_im").sum()  
#------------------------- Limpando variáveis------------------------
b_icmbio=pd.DataFrame()
i_icmbio=pd.DataFrame()
df=pd.DataFrame()
dados_icmbio=pd.DataFrame()
#------------------------------------Embargos IBAMA--------------------------------------------------   
dados_ibama=pd.DataFrame()
sql = "SELECT * FROM public.ibama_amb_csa_area_embagada_a"
df = gpd.read_postgis(sql, conn)
dados_ibama= pd.concat([dados_ibama, df]) 
b_ibama=farms_buffer.overlay(dados_ibama, how='intersection')
#b_ibama=buffer.overlay(dados_ibama, how='intersection')
#b_ibama=b_ibama.overlay(farms_buffer, how='intersection')
#-------------------------------calculando área--------------------
b_ibama= b_ibama.to_crs('epsg:9822')
b_ibama['area_ibama']=((b_ibama['geometry'].area)/10000)#*b_ibama['peso_buffer']
b_ibama= b_ibama.to_crs('epsg:4326')
#--------------------------------somando área pelo CAR---------------------
d_ibama=b_ibama[["cd_im", "area_ibama"]].groupby("cd_im").sum()  
#------------------------- Limpando variáveis------------------------
b_ibama=pd.DataFrame()
i_ibama=pd.DataFrame()
df=pd.DataFrame()
dados_ibama=pd.DataFrame()
#-------------------------------------SEMA-------------------------------------------------   
dados_sema=pd.DataFrame()
sql = "SELECT * FROM public.sema"
df = gpd.read_postgis(sql, conn)
dados_sema= pd.concat([dados_sema, df])
b_sema=farms_buffer.overlay(dados_sema, how='intersection')
#b_sema=buffer.overlay(dados_sema, how='intersection')
#b_sema=b_sema.overlay(farms_buffer, how='intersection')
#--------------------------------calculand área--------------------
b_sema= b_sema.to_crs('epsg:9822')
b_sema['area_sema']=((b_sema['geometry'].area)/10000)#*b_sema['peso_buffer']
b_sema= b_sema.to_crs('epsg:4326')
#--------------------------------somando área pelo CAR---------------------
d_sema=b_sema[["cd_im", "area_sema"]].groupby("cd_im").sum()
#------------------------- Limpando variáveis------------------------
b_sema=pd.DataFrame()
i_sema=pd.DataFrame()
df=pd.DataFrame()
dados_sema = pd.DataFrame()
#-------------------------- Concatenando os critérios em um mesmo df -----------------------------------
#buffer_final=pd.merge(buffer, d_desmatamento, how="left", on=["id_polygon"])
#buffer_final=pd.merge(buffer_final, d_icmbio, how="left", on=["id_polygon"])
#buffer_final=pd.merge(buffer_final, d_ibama,how="left", on=["id_polygon"])
#buffer_final=pd.merge(buffer_final, d_sema,how="left", on=["id_polygon"])

datetime7=datetime.now()
print("criterios socioambientais:", datetime7)
#----------------------------organizando os dados-----------------------------------------------
#--------------------------merge entre os farms e as informações---------------------------------- 
final_car=pd.merge(farms_buffer, b19_pastagem, how="left", on=["cd_im"])
final_car=pd.merge(final_car, b20_pastagem, how="left", on=["cd_im"])
final_car=pd.merge(final_car, b21_pastagem, how="left", on=["cd_im"])
final_car=pd.merge(final_car, d_sema, how="left", on=["cd_im"])
final_car=pd.merge(final_car, d_icmbio, how="left", on=["cd_im"])
final_car=pd.merge(final_car, d_desmatamento, how="left", on=["cd_im"])
final_car=pd.merge(final_car, d_ibama, how="left", on=["cd_im"])
test1=dados_car.overlay(buffer, how='intersection')
test1=test1.drop(columns=["num_area","uuf","nom_munici","num_modulo","tipo_imove","situacao","condicao_i","log","imovel","geometry"])
final_car=final_car.drop(columns=[0,1,2,3,4,5,6,7,8,9,10])
farms_final=pd.merge(test1,final_car, how="left", on=["cd_im"])
farms_final
farms_final["areap_sema"]=farms_final["area_sema"]*farms_final["peso_buffer"]
farms_final["areap_icmbio"]=farms_final["area_icmbio"]*farms_final["peso_buffer"]
farms_final["areap_desmatamento"]=farms_final["areadesmatamento_ha"]*farms_final["peso_buffer"]
farms_final["areap_ibama"]=farms_final["area_ibama"]*farms_final["peso_buffer"]

datetime8=datetime.now()
print("merge dos dataframes",datetime8)