# extract_datasus_to_bigquery_with_mongodb_pyspark
## Contexto Inicial
Extração de dados por meio de endpoint e envio dos dados para o Mongo Atlas para armazenamento dos dados com uso de Spark e criação de dataframe usando Pandas, seguido de envio ao datawarehouse no Big Query e visualização no Power BI.

## Tecnologias Utilizada
Mongo Atlas, Google Cloud Storage, Big Query, Power BI, Spark, PySpark, Pandas.

## Bibliotecas Utilizadas
Para extração do json da API publica e inserção no Mongo Atlas:
```python
import os
import requests
from requests.auth import HTTPBasicAuth
from pymongo import MongoClient, errors
```

Para transformação do dataframe extraindo do Mongo Atlas e inserindo no BigQuery:
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date
from google.cloud.exceptions import NotFound
from pymongo import MongoClient
import os
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
```

## Modelagem de dados
Os dados estão no seguinte endpoin acessado apenas por senha disponibilizada na documentação do projeto.
Foi utilizado a lib requests para acessar o endpoint e começar a extração dos dados, depois foi configurado o ambiente do Mongo Atlas para receber o valor dos json existentes na página e armazenados lá.
O segundo pipeline é responsável por puxar os dados do Mongo Atlas e utilizando spark/pyspark realizado a captura dos campos e renomeados, depois executamos duas queries usando nosso df para criar as colunas: estado_nome que consiste no nome completo dos estados em que os pacientes tomaram as vacinas e a coluna faixa_etaria que enquadra os pacientes de acordo com sua faixa etaria, convertemos no df para um df unico e logo após definimos os schemas de inserção no big query e iniciamos os clientes, optei por primeiro criar tabelas parquet com o nome das dimensões da minha modelagem e depois a partir do parquet inserido no storage enviar ao Big Query.

Estrutura das Tabelas
**Tabela `dim_paciente`**:

| Coluna           | Descrição                             |
|------------------|---------------------------------------|
| `paciente_id`    | Id do paciente                        |
| `paciente_idade` | Idade do Paciente                     |
| `paciente_cidade`| Cidade do paciente                    |
| `paciente_sexo`  | Sexo Biologico com Paciente           |
| `faixa_etaria`   | Faixa etária correspondente a idade   |
| `nacionalidade`  | Nacionalidade do paciente             |
| `paciente_raca`  | Raça do paciente                      |

**Tabela `dim_localizacao`**:
| Coluna                          | Descrição                     |
|---------------------------------|-------------------------------|
| `paciente_id`                   | Id do paciente                |
| `estabelecimento_municipio_nome`| Nome do municipio da UBS      |
| `estabelecimento_uf`            | UF da UBS                     |
| `razaoSocial`                   | Nome do responsável pela UBS  |
| `estalecimento`                 | Nome da UBS                   |
| `estado_nome`                   | Estado da UBS                 |


**Tabela `fato_atendimento`**:
| Coluna                  | Descrição                             |
|-------------------------|---------------------------------------|
| `paciente_id`           | Id do paciente                        |
| `vacina_descricao_dose` | Dose aplicada                         |
| `vacina_fabricante_nome`| Fabricante da Vacina                  |
| `vacina_codigo`         | Codigo da Vacina                      |
| `v_numDose`             | Quantidade já ministradas por paciente|
| `vacina_categ`          | Categoria da dose                     |
| `v_dataAplicacao`       | Data do atendimento                   |
| `vacina_nome`           | Nome da Vacina                        |

#
### Agregação de Dados
Após a utilização de queries, editamos e unimos em um unico dataframe a partir do paciente_id.
#
### Visualização dos Dados
[Bigquery](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1smywebscrap-423316!2sestudos_gcp)

[PowerBI](https://app.powerbi.com/groups/me/reports/6665096e-92e1-4515-85f5-e05794abb6c5?ctid=6c60811c-739e-4044-a1d3-8c56a8a50c87&pbi_source=linkShare)
### Credenciais de Acesso
Para funcionamento dos notebooks é importante o uso de credenciais de acesso do Mongo Atlas e GCP Cloud, as dados da API publica são:

"AUTH_URL": "https://imunizacao-es.saude.gov.br/desc-imunizacao/_search",

"DATA_URL": "https://imunizacao-es.saude.gov.br/_search?scroll=1m",

"API_USERNAME": "imunizacao_public",

"API_PASSWORD": "qlto5t&7r_@+#Tlstigi"
