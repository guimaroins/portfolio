O intuito do código é analisar dados por grupo vulnerável de acordo com as categorias do ISP-RJ. Para isso, pivotei 5 colunas com valores 0 e 1 com 0 para o caso que não se encaixe no grupo e 1 para os que se encaixam. O objetivo de pivotar as colunas é evitar repetições de linhas, visto que um caso pode fazer parte de mais de um grupo.
###### Atenção: As condições para classificação de cada grupo estão disponíveis no site do ISP-RJ.

# Explicação do código 
### 1ª célula:
Importação das bibliotecas necessárias.
```python
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import psycopg2 as pg
import json
import locale
import requests
from datetime import date
from sqlalchemy import create_engine
```

### 2ª célula:
Conexão ao banco e extração dos dados lendo a query de um arquivo sql.

```python
# Parâmetros de conexão
conn_parameters = {
        'host': 'host',
        'database': 'banco',
        'user': 'user',
        'password': 'senha'
    }

try:
    engine = create_engine(f"postgresql+psycopg2://{conn_parameters['user']}:{conn_parameters['password']}@{conn_parameters['host']}/{conn_parameters['database']}")
    print("Conexão estabelecida")

    # Leitura do arquivo sql
    with open('consulta.sql', 'r') as file:
        query_sql = file.read()

    # Extração dos dados em um dataframe
    df = pd.read_sql_query(query_sql, engine)
    display(df.head(3))

    print("Extração de dados concluída.")
    print("Conexão fechada.")

except Exception as error:
    print(f"Erro ao fazer a extração de dados do banco: {error}")
```

### 3ª célula:
Aqui eu filtro o dataframe criando outro a partir do principal com os dados necessários para criar o gráfico geográfico e faço a modelagem dos dados. As colunas de soma são adicionadas apenas para retirar o grupo com maior registro de casos por município.

```python
# Colunas que vão ser filtradas
columns = ['municipio_fato', 'Crianças e Adolescentes', 'mulheres', 'jovens', 'idosos', 'População Negra']

# Filtrando apenas as colunas de interesse do DataFrame
df_filter = df[columns]

# Agrupando por 'municipio_fato' e calculando a soma para cada grupo
df_map = df_filter.groupby('municipio_fato').sum().reset_index()

# Como quero uma coluna com a quantidade de casos por municipio, é necessário criar ela a parte e depois fazer o join como dataframe que será utilizado
casos = df.groupby('municipio_fato').size().reset_index(name = 'Casos')
display(casos)

# Join
df_map = df_map.merge(casos, on = 'municipio_fato', how = 'left')

# Removendo '(Capital)' de 'Rio de Janeiro' para ficar igual ao registro do IBGE e evitar erros
df_map['municipio_fato'] = df_map['municipio_fato'].replace('Rio de Janeiro (Capital)', 'Rio de Janeiro')

# Usando .idxmax(axis=1) para encontrar o nome da coluna com o valor máximo em cada linha
grupos = ['Crianças e Adolescentes', 'mulheres', 'jovens', 'idosos', 'População Negra']
df_map['Grupo'] = df_map[grupos].idxmax(axis=1)

display(df_map)
```

### 4ª célula:
Coleta do geojson (necessário nesse tipo de gráfico) com os dados da localização de cada município através de uma API do IBGE. Aqui encontrei um obstáculo: a API das malhas, onde se encontra a divisão de cada município, não contém o nome dos municípios e o dataframe não contém o código deles. Assim, chamo uma segunda API para extrair em um json os dados contendo o nome do município e seu respectivo ID.

```python
url_malhas = 'https://servicodados.ibge.gov.br/api/v3/malhas/estados/33?formato=application/vnd.geo+json&qualidade=intermediaria&intrarregiao=municipio'
malhas_response = requests.get(url_malhas)
geojson_malhas = malhas_response.json()

url_municipios = 'https://servicodados.ibge.gov.br/api/v1/localidades/estados/33/municipios?view=nivelado'
municipios_response = requests.get(url_municipios)
dados_municipios = municipios_response.json()
```

### 5ª e 6ª células:
A partir daqui tenho 2 caminhos:
- adicionar o código IBGE ao dataframe
- adicionar o nome das cidades ao geojson
##### Por mais institivo que pareça adicionar o código IBGE ao dataframe por se tratar de um ID e naturalmente serem a opção mais segura, a verdade é que neste caso não faz diferença, pois de qualquer forma precisaremos que o nome dos municípios estejam exatamente iguais aos da API, se não a adição do código IBGE não será realizada ao município e retornará um valor NaN na tupla. Então, optei por mostrar as duas formas começando pela primeira.

```python
# Criar um dicionário para mapear nomes de municípios para seus códigos
municipios_codigos = {
    municipio['municipio-nome']: municipio['municipio-id'] for municipio in dados_municipios
    }
print(municipios_codigos)

# Mapear os códigos do município no DataFrame usando o nome do município como chave
df_map['codigo_ibge'] = df_map['municipio_fato'].map(municipios_codigos)
display(df_map)
```

```python
codigo_para_nome = {
    str(municipio['municipio-id']): municipio['municipio-nome'] for municipio in dados_municipios
                }
print(codigo_para_nome)

# Adicionar os nomes dos municípios ao GeoJSON
for feature in geojson_malhas['features']:
    codigo_municipio = feature['properties']['codarea']  # onde 'codarea'é o código do ibge na api das malhas
    feature['properties']['name'] = codigo_para_nome.get(codigo_municipio)
```

### 7ª célula:
Agora, finalmente, temos nosso geograph mostrando a quantidade de casos por município. Nesse primeiro vou utilizar os nomes como chave. Quanto mais roxo, menor o número e quanto mais amarelado, maior o número.

```python
map1 = px.choropleth_mapbox(
    df_map, # df com os dados
    geojson = geojson_malhas, # geojson com os dados das malhas
    color = "Casos", # coluna do df com os valores para colorir o gráfico
    locations = "municipio_fato", # coluna do df utilizada como chave
    featureidkey = "properties.name", # valor do geojson utilizado como chave
    title = "Casos por município (Jan/2020 a Jun/2023)",
    mapbox_style = "white-bg", # estilo do mapa (não terá, optei por fundo branco)
    center={"lat": -22.1, "lon": -43}, # posição do mapa
    zoom = 7, # zoom inicial
    opacity = 0.7, # opacidade para que apareça o fundo (nesse caso como o fundo é branco, utilizei apenas para clarear um pouco as cores)
    height = 700 # comprimento do mapa
)
map1.show()
```
![Captura de Tela 2024-02-21 às 05 19 56](https://github.com/guimaroins/portfolio/assets/108079970/92856c3e-4ac7-4a3e-bade-56ba2f1ce996)

### 8ª célula:
Sei que não é legal repetir gráficos, mas achei interessante os dados desse. Nesse é mostrado qual o grupo mais vulnerável por município com base no grupo que teve a maior quantidade de casos. Os grupos "Crianças e adolescentes" e "Jovens" não tiveram destaque em nenhum município. Foi utilizado o código IBGE como chave.

```python
map2 = px.choropleth_mapbox(
    df_map,
    geojson = geojson_malhas,
    color = "Grupo",
    locations = "codigo_ibge", # coluna do df utilizada como chave
    featureidkey = "properties.codarea", # valor do geojson utilizado como chave
    title = "Grupo vulnerável por municipio (Jan/2020 a Jun/2023)",
    mapbox_style = "white-bg",
    center={"lat": -22.1, "lon": -43},
    zoom = 7,
    opacity = 0.7,
    height = 700
)
map2.show()
```
![Captura de Tela 2024-02-21 às 05 21 26](https://github.com/guimaroins/portfolio/assets/108079970/531c9e32-f33a-4265-86c0-7a4720fe67d1)

### 9ª célula:
Agora criarei um gráfico de barras simples reutillizando o df usado anteriormente como filtro e removendo apenas a coluna com os municípios, pois não será utilizada, e realizo as transformações necessárias. Este gráfico mostra apenas quantidade de casos por grupo vulnerável no total.

```python
# Criando um novo dataframe transformando colunas em linhas e ordenando por casos em ordem decrescente.
df_grupo = df_filter.drop('municipio_fato', axis=1).sum().reset_index().rename(columns = {'index': 'Grupo', 0: 'Casos'}).sort_values(by = 'Casos', ascending = False)
display(df_grupo)
```

### 10ª célula:
E o gráfico ficou assim:

```python
fig = px.bar(
    df_grupo, # df a ser utilizado
    x = 'Grupo', # valores do eixo x
    y = 'Casos', # valores do eixo y
    title = "Quantidade de casos por grupo vulnerável (Jan/2020 a Jun/2023)", 
    height = 600, # altura da figura
    color_discrete_sequence = ['#6A5ACD'] # escolhendo a cor roxo
    )
fig.update_layout(width = 800) # largura da imagem

fig.show()
```
![Captura de Tela 2024-02-21 às 05 22 45](https://github.com/guimaroins/portfolio/assets/108079970/b14ef19f-88a0-4be0-bafb-41cfd1bea1c4)

### 11ª célula:
Aqui quero criar um gráfico múltiplas linhas (uma para cada grupo) com a quantidade de registros de ocorrências por mês no ano de 2020 (será explicador posteriormente a razão). Para isso, faço um processimento parecido com o feito anteriormente de filtrar as colunas com o adicional de ter que transformar os meses para sua numeração correspondente para que mantenha-se na ordem correta dos meses e não na ordem alfabética (o que faria começar por abril). Ao final, retorno para a nomenclatura do mês.

```python
import locale
locale.setlocale(locale.LC_TIME, 'pt_BR') # isso é para o pandas entender os meses em português certo, já que o padrão é em inglês.

columns = ['ano', 'mes', 'Crianças e Adolescentes', 'mulheres', 'jovens', 'idosos', 'População Negra']

# Filtrando o ano de 2020
df_filter2 = df[columns].loc[df['ano'] == 2020]
# Transformando os meses para numeral para ficar na ordem correta
df_filter2.loc[:, 'mes'] = pd.to_datetime(df_filter2['mes'], format='%B').dt.month
display(df_filter2)

# Agrupando por 'ano' e 'mes' e calculando a soma para cada grupo
df_line = df_filter2.groupby(['ano', 'mes']).sum().reset_index()
# Transformando os meses para a nomenclatura
df_line['mes'] = pd.to_datetime(df_line['mes'], format = '%m').dt.strftime('%B')

display(df_line)
```

### 12ª célula:
Aqui crio o gráfico de linhas.

```python
# Criando a figura
fig_line = go.Figure()

# Adicionando os traços de cada coluna
fig_line.add_trace(go.Scatter(x = df_line['mes'], y = df_line['Crianças e Adolescentes'], mode = 'lines', name = 'Crianças e Adolescentes'))
fig_line.add_trace(go.Scatter(x = df_line['mes'], y = df_line['mulheres'], mode = 'lines', name = 'Mulheres'))
fig_line.add_trace(go.Scatter(x = df_line['mes'], y = df_line['jovens'], mode = 'lines', name = 'Jovens'))
fig_line.add_trace(go.Scatter(x = df_line['mes'], y = df_line['idosos'], mode = 'lines', name = 'Idosos'))
fig_line.add_trace(go.Scatter(x = df_line['mes'], y = df_line['População Negra'], mode = 'lines', name = 'População Negra'))

# Ajustando o layout
fig_line.update_layout(
    title = 'Quantidade de registros por mês (2020)',
    xaxis_title = 'Casos',
    yaxis_title = 'Mês',
    plot_bgcolor = 'white'
    )

fig_line.update_xaxes(showline = True, linewidth = 1, linecolor = 'black')
fig_line.update_yaxes(showline = True, linewidth = 1, linecolor = 'black')

fig_line.show()
```
![Captura de Tela 2024-02-21 às 05 23 18](https://github.com/guimaroins/portfolio/assets/108079970/4dfc2c73-8a6a-46f9-b94f-dec0325a8a55)

### 13ª célula:
Bom, eu gostaria de fazer uma uma observação um tanto quanto triste sobre esse gráfico. Fazendo uma breve análise dele, nota-se que ente os meses de Fevereiro a Maio (Abril para os idosos) houve uma queda nos registros das ocorrências. Quero deixar claro que escolhi propositalmente as coluna 'ano' e 'mes' ao invés de 'ano_fato' e 'mes_fato', pois retratam quando foram feitos os registrosnas delegacias, e não quando ocorreram os fatos. O ano de 2020 e 2021, especialmente 2020, foram o ápice da pandemia no Brasil. Durante esses meses, foi quando começaram as medidas protetivas contra o COVID-19 e o lockdown em abril (onde nota-se o "fim" da queda brusca no gráfico). O que acontece é que infelizmente muitos desses casos ocorrem em ambientes domésticos e a vítima convive com o agressor. Com o lockdown, muitos desses casos não foram registrados. Até houve uma diminuição nos casos em que o agressor era alguém que não residia com a vítima, mas parte do motivo da diminuuição brusca dos casos se deve à impossibilidade de registro deles. Vou utilizar como exemplo outro gráfico de linhas similar, mas utilizando apenas o grupo de idosos onde uma linha representa a quantidade de casos registrados no mês e a outra a quantidade de casos que de fato aconteceram. Aqui separo os dados que vão ser usados.

```python
columns2 = ['ano_fato', 'mes_fato', 'Crianças e Adolescentes', 'mulheres', 'jovens', 'idosos', 'População Negra']

df_filter3 = df[columns2].loc[df['ano_fato'] == '2020']
df_filter3.loc[:, 'mes_fato'] = pd.to_datetime(df_filter3['mes_fato'], format='%B').dt.month

df_line2 = df_filter3.groupby(['ano_fato', 'mes_fato']).sum().reset_index()
df_line2['mes_fato'] = pd.to_datetime(df_line2['mes_fato'], format = '%m').dt.strftime('%B')
```

### 14ª célula:
O gráfico:

```python
fig_line2 = go.Figure()

fig_line2.add_trace(go.Scatter(x = df_line['mes'], y = df_line['idosos'], mode = 'lines', name = 'Idosos (registro)')) # Linha da quantidade de registros por mês
fig_line2.add_trace(go.Scatter(x = df_line2['mes_fato'], y = df_line2['idosos'], mode = 'lines', name = 'Idosos (caso)')) # Linha da quantidade de ocorrência do fato por mês

fig_line2.update_layout(
    title = 'Quantidade de casos e registros por mês (2020)',
    xaxis_title = 'Casos',
    yaxis_title = 'Mês'
    )

fig_line2.show()
```
![Captura de Tela 2024-02-21 às 05 23 52](https://github.com/guimaroins/portfolio/assets/108079970/b2d9a52f-e63b-4f9c-8b6b-4b3d3b78449e)

Como observado entre Junho e Julho, era esperado que quando fosse voltando ao normal o funcionamento de registros dos casos (seja podendo sair de casa ou através de maios alternativos durante a pandemia), aqueles casos ocorridos quando era impossibilitado o registro começassem a ser registrados. Deve levar em consideração que filtrei 'ano_fato' por 2020 (o que automaticamente já filtra pelos que tem informações do ano de mês), mas têm casos onde não há informações do ano da ocorrência e nem do mês.

# Considerações finais
Para visualizar os displays, acesse o arquivo ipynb. Infelizmente nele não há as imagens dos gráficos ao fazer o commit no github, então elas só se encontram aqui no readme. 
Recomendo a leitura do livro "Storytelling with data", pois contém uma leitura esclarecedora sobre visualização de dados. Utilizei alguns conceitos aqui.
Obrigada pela leitura, espero ter sido clara :) Qualquer dúvida, chama no linkedin ;)
