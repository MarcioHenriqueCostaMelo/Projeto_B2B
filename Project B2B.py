import requests
import pandas as pd
import time
import json


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

base_url= "https://brasilapi.com.br/api/"

def get_cnpj(cnpj):
  url=f"{base_url}cnpj/v1/{cnpj}"
  response =requests.get(url)
  if response.status_code==200: # If the 200 , It 's meaning that request is okay
    info=response.json()
    return info
  else:
    error= f" Failed to recive this response {response.status_code}"
    raise Exception(error)
  

lista_cnpj=pd.read_csv("dados_brutos/list_cnpj.csv",dtype={"cnpj":str})

def enriquecimento(lista_cnpj):
   lista_cnpj_to_data=[]
   error_count=0
   
   for cnpj in lista_cnpj['cnpj']:
      try:
          print("")
          info_cnpj_api=get_cnpj(cnpj)
      except Exception:
          error_count+=1
          print(f"Erro ao buscar o {cnpj}")
          pass
      else:
        if info_cnpj_api is not None:
          lista_cnpj_to_data.append(info_cnpj_api)
          print(f"Foi adicionado com sucesso a empresa {info_cnpj_api["nome_fantasia"]} com cnpj : {info_cnpj_api["cnpj"]}")
          time.sleep(1)
          print(f"Numero atual de empresas enriquecidas :{len(lista_cnpj_to_data)}")
          if len(lista_cnpj_to_data)%30 == 0  and len(lista_cnpj_to_data)>0:# isso foi porque sempre quando chegava em mais ou menos 30 requisições seguidas, ele não conseguiar achar alguns cnpj , então add isso para ele parar uns 15 seg a cada 30 requisições
             time.sleep(15) 
             
        if len(lista_cnpj_to_data)%100==0 and len(lista_cnpj_to_data)>0:# aqui a gente ta salvando a cada 100 , para o caso de se um falhar a gente tenha o backup
          backup=pd.DataFrame(lista_cnpj_to_data)
          backup.to_csv('backup.csv',index=False)
          print("Backup Realizado!!!")
   return lista_cnpj_to_data,error_count

#data_cnpj,error_count=enriquecimento(lista_cnpj)
#print(f" quantidade de cnpjs não lidos {error_count}")
df_cnpj=pd.DataFrame(data_cnpj)
df_cnpj.to_csv('data.csv')
# criar uma def para salr tb , pq eu fiz tudo e n transformei para CSV KKKKKKKKKKKKKKKKKKKKKKK

###PRÓXIMOS PASSOS:
#Pensar em escalar isso para uns 10 mil cnoj , que devem ser filtrados com uf=sp e que são matrizes
