# ETL GRUPOS VULNERÁVEIS - PYTHON

Como os projetos deste repositório utilizarão os dados que solicitei por e-mail do ISP-RJ (Instituto de Segurança Pública do Rio de Janeiro), fiz uma pequena ETL em python para subir em um banco de dados postgre local. Fiz isso com o intuito de ser maais prático visto que os dados me foram encaminhados em dois arquivos csv e também para manter a prática em SQL. Os projetos vão ser focados em grupos vulneráveis e os dados estão disponíveis no site do ISP-RJ. Esses dados são de boletins de ocorrência registrados. Os passos são simples:

- O código irá ler os dois arquivos csv e unir em um único dataframe;
- Adicionará algumas colunas, substituirá alguns dados e converterá o tipo de algumas colunas;
- Irá carregar o dataframe resultante no banco de dados.
- Irá carregar o dataframe resultante no banco de dados.

Para rodar localmente, será necessário instalar as seguintes bibliotecas:

```
pip install pandas
pip install numpy
pip install SQLAlchemy
pip install psycopg2
```

Optei por modularizar o código para manter o costume, então ele foi separado por funções e cada função descrita em inglês dentro do código.

#### Observação: por uma questão de segurança, não será utilizado valores reais de dados sensíveis (como caminho de arquivos e dados de conexão do banco de dados) em nenhuma parte do código. Para testar localmente, deverá introduzir as respectivas informações de acordo com sua máquina.

## RESULTADO DO CÓDIGO: (gif)

## Explicando o código

### 1º: Bibliotecas

Na primeira célula apenas importei as bibliotecas necessárias para a construção da ETL.

```python
import pandas as pd
import numpy as np
import psycopg2 as pg
import locale
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema
```

### 2º: Variáveis

Nesta célula, configuro locale como Brasil para a data e idioma virem corretos e crio duas variáveis com o local do arquivo na minha máquina para lê-los porteriormente.

###### Obs.: é sempre bom procurar utilizar variáveis com os valores dos parâmetros para obter-se um melhor reaproveitamento futuro das funções.

```python
locale.setlocale(locale.LC_TIME, 'pt_BR')
csv_1 = 'caminho_do_arquivo_1.csv'
csv_2 = 'caminho_do_arquivo_2.csv'
schema_Name = 'grupos_vulneraveis'
table_Name = 'todos'
```

### 3º: Função principal

Nesta célula crio a função principal que conterá o fluxo do código. Direta como o significado de ETL (extrair, transformar e carregar), ela:
- Chama a função de extrair os dados (extract()) e atribui o resultado à uma variável que vai ser utilizada como parâmetro na póxima função. 
- Depois, chama a função de transformar os dados (transform()) com o dataframe salvo anteriormente como parâmetro para fazer as alterações necessárias no dataframe e salva o resultado em uma nova variável para ser utilizada na próxima função. 
- Por fim, chama a função load() para carregar os dados no banco de dados utilizando o dataframe salvo na função anterior e as variáveis com o nome do esquema e da tabela como parâmetros.

```python
def main() -> None:

    '''Main function that handles the pipeline flow. Steps:
        1. Calls the extract function to extract data from the CSV file and save it in a dataframe that will be used in the transform function.
        2. Calls the transform function to perform data modeling in the extracted dataframe and save the results in another dataframe that will be used in the lead function.
        3. Calls the load function to save the transformed dataframe in the database.

        Parameters:
        - None
    
        Returns:
        - None'''
    
    df_extracted = extract(csv_1, csv_2)
    df_transformed = transform(df_extracted)
    load(df_transformed, schema_Name, table_Name)
```

### 4º: Função de extração de dados

A partir desta parte do código, é criada as funções que de fato vão construir uma ETL. A primeira é a extract():
- Ela lê dois arquivos CSV com o pandas utilizando a funçao pd.read_csv() utilizando as varáveis com os caminhos dos arquivos csv a serem lidos como parâmetros e os salva em dataframes distintos. Também foram usados como parâmetros encoding=latin1 para ler os caracteres especiais e o separador sendo ";" para o dataframe ser lido corretamente;
- Concatena os dataframes utilizando a função pd.concat() do pandas utilizando ambos os dataframes para os unir em um dataframe.
- Imprimir uma amostra de cinco linhas do dataframe gerado.
- Retornar o dataframe gerado.

Não foi utilizado nenhum outro método de join, pois são dois dataframes com exatamente as mesmas colunas de mesmo tipo, mudando apenas os dados contidos nelas. Assim, foi usado como parâmetro axis=0 para indicar que a concatenação deveria ser feita verticalmente. O uso de index=true serve para ignorar os índices nos dataframes de origem e realizar outro, pois, se a última linha do segundo dataframe for, por exemplo, 10.000, então após a união com o dataframe anterior de, por exemplo, 60.000 linhas, ainda apareceria que a última linha é a 10.000 ao invés de 70.000, já que por padrão index=false. No entanto, indicando index=true, ele refaz essa contagem de linhas e apresenta um novo índice correto com a última linha passando a ser 70.000.

```python
def extract(file_path_1: str, file_path_2: str) -> pd.DataFrame:

    '''Extract the data from two CSV files and save in one dataframe. Steps:
        1. Read the CSV files separately using pandas and save them in saparete dataframes;
        2. Join the two dataframes using pandas.

        Parameters:
        - String containing the first CSV file path;
        - String containing the second CSV file path.

        Return:
        - Joined dataframe.'''
    
    df1 = pd.read_csv(file_path_1, encoding = 'latin1', sep = ";")
    df2 = pd.read_csv(file_path_2, encoding = 'latin1', sep = ";")

    df = pd.concat([df1, df2], axis = 0, ignore_index = True)
    print("DataFrame criado com sucesso.")
    
    display(df.head(5))
    
    return df
```

### 5º: Função de adicionar colunas

Não só para fins de reaproveitamento de código, mas de organização também, a parte de adicionar colunas ao dataframe ficará numa função separada que será utilizada dentro da função de transformar. Acho mais agradável separar uma função em duas se inicialmente ela fizer mais de um procedimento relativamente distintos. Assim, a função de adicionar colunas fica a cargo da função add_columns() e a função de transformar os dados fica a cargo da transform().
Algumas columas do dataframe têm valores nulos, o que torn a coluna de data_fato, por exemplo, do tipo text para o banco de dados ao substituir esses valores por qualquer outra coisa. Por algum motivo que não sei informar, essa coluna informa a data do fato que não é uma informação obrigatória ao prestar uma queixa. Mas deixando essa coluna como text, fica mais difícil o trabalho de filtrar dados de fatos que ocorreram em um determinado mês ou ano, ou até mesmo de utilizar uma função de agregação (como count). Desta forma, a função add_columns fica responsável por:
- Receber como parâmetro o dataframe a ser adicionadas as colunas.
- Adicionar a coluna de atualizado_em para manter-se um controle de quando aqueles dados foram atualizados no banco que criei utilizando a função date.today().
- Extrair o ano do fato a partir da coluna de data do fato utilizando o pandas da seguinte maneira:
    - utiliza a função pd.to_datetime() com a coluna data_fato como parâmetro para converter para o tipo datetime[64] e poder ser manipulada por outras funções de data;
    - utiliza a função dt.year para extrair o ano;
    - como alguns valores da coluna são nulos e o tipo inteiro não aceita valores nulos, a coluna foi transformada no tipo float que aceita esses valores, então também se utiliza a função astype() com str como parâmetro para converter a coluna para string para que seja manipulado a remoção dos caracteres '.0';
    - utiliza a função de string replace() com '.0' e '' como parâmetros, removendo, assim, a casa decimal que foi adicionada ao ser atribuido o tipo float a coluna.
- Extrair o mês do fato a partir da coluna de data do fato utilizando pd.dataframe() do pandas para transformar a coluna datetime[64] e poder ser manipulada por outras funções de data, depois usa dt.dtrftime() com o formato '%B' como parâmetro indicando para retornar o nome do mês. Como locale foi configurado para pt-BR, então retornará os nomes em português.
- Retornar o dataframe com as colunas adicionadas.

Agora, sim, pode-se utilizar filtros ou funções de agrupamento para contar a quantidade de ocorrências de um determinado ano e/ou mês.

```python
def add_columns(df: pd.DataFrame) -> pd.DataFrame:

    '''Add new columns for the dataframe. Steps:
        1. Creates a new column and assigns the dataframe's update date to it.
        2. Creates a new column and assigns the extracted year from a specific date column to it using pandas.
        3. Creates a new column and assigns the extracted month from the same date column used to extract the year to it using pandas.

        Parameters:
        - Dataframe for which the columns will be added.

        Returns:
        - Dataframe with the added columns.'''

    
    df['atualizado_em'] = date.today()
    print("Coluna 'atualizado_em' adicionada ao DataFrame.")

    df['ano_fato']= pd.to_datetime(df['data_fato']).dt.year.astype(str).str.replace('.0', '')
    print("Coluna 'ano_fato' adicionada ao DataFrame.")

    df['mes_fato']= pd.to_datetime(df['data_fato']).dt.strftime('%B')
    print("Coluna 'mes_fato' adicionada ao DataFrame.")

    return df
```

##### Curiosidade:
Você deve estar se perguntando o motivo pelo qual não uilizei o mesmo método para extrair os dados em ambas as colunas:

```python
df['ano_fato']= pd.to_datetime(df['data_fato']).dt.year.astype(str).str.replace('.0', '')
df['mes_fato']= pd.to_datetime(df['data_fato']).dt.strftime('%B')
```

Seria só substituir ```.dt.year.astype(str).str.replace('.0', '')``` por ```.dtstrftime('%Y)```, certo?
Mas a verdade é que eu tentei! Buscando o melhor método para extrair os dados, eu sempre acabo testanto tudo, e após testar diversas vezes curiosamente percebi que o primeiro método levava exatamente a metade do tempo do segundo. 
###(print)

Bom, não fui muito a fundo pesquisar o porquê, mas provavelmente deve ser pq ```strftime()``` faz um rolê muito maior para chegar no mesmo resultado e acaba ficando para trás. Não vale dizer que foi a máquina, viu? Testei até em dias diferentes, hahaha! E quanto ao uso da função ```rstrip()``````, bom, ela remove os caracteres do final da string independente da ordem. Então, 2020.0, por exemplo, acabava virando 202 D:

### 6º: Função de transformar dados
Aqui é feita a mudança ou conversão de dados originalmente no dataframe. Não tem muito mistério:
- Como adicionar colunar é uma forma de transformar o dataframe original, então a função add_columns() é chamada dentro da função transform() até para manter o padrão da pipeline.
- Apesar da coluna data_com não ter valores nulos, ao subir ela direto no banco de dados, ele ainda a recebe como texto. Então, é utilizada a função do pandas pd.to_datetime() com a coluna como parâmetro para converter para o tipo datetime[64] e conseguir utilizar a função dt.date, pois, apesar do pyhon imprimir a coluna somente com a data, ao subir no banco como datetime, você terá uma surpresinha ao ver a data acompanhada de 00:00:00+00.
    - Aqui dá pra ser utilizada a função lambda ```df['data_com'] = df['data_com'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").date()```, mas ela demorou 8.04 segundos para rodar enquanto o método que escolhi rodou em 0.04 segundos.
- Utiliza o mesmo esquema anterior para a coluna hora_com com a diferença de um format no parâmetro para indicar o formato das horas e time no lugar de date.
- Utiliza replace() com np.nan e 'Não informado'como parâmetros para repor todos os dados nulos por "Não informado" e evitar trabalho futuro de fazer o mesmo em cada projeto.
- Como dito anteriormente, o tipo inteiro não aceita dados nulos, então a coluna idade foi convertida para float. Assim, foi feito os passos:
    - utiliza a função astype() com str como parâmetro para converter a coluna para string;
    - utiliza a função de string replace() com '.0' e '' como parâmetros, realizando a remoção da casa decimal que foi adicionada ao ser atribuido o tipo float à coluna.
- Retorna o dataframe transformado.

A substituição dos valores nulos foi realizada antes da remoção do decimal da coluna de idade, pois do contrário, o nulo da coluna idade se transformaria em uma string nan atrapalhando a substituição dos valores nulos detectados por np.nan.

```python
def transform(df: pd.DataFrame) -> pd.DataFrame:
    '''Transform the dataframe by calling the add columns function and changing some columns types using pandas. Steps:
        1. Calls the add columns function and save in a dataframe;
        2. Uses pandas to convert the type of a date column from the dataframe from text to datetime and then to date;
        3. Uses pandas to convert the type of a hour column from the dataframe from text to datetime and then to time;
        4. Uses replace function to replace NULL values to a text;
        5. Uses pandas to convert the type of a age column from the dataframe from float to text and then remove unnecessaries character.
        
        Parameters:
        - Dataframe to be transformed.
        
        Return:
        - Dataframe transformed.'''

    df = add_columns(df)

    df['data_com'] = pd.to_datetime(df['data_com']).dt.date
    print("Coluna 'data_com' convertida para DATE.")

    df['hora_com'] = pd.to_datetime(df['hora_com'], format='%H:%M:%S').dt.time
    print("Coluna 'hora_com' convertida para TIME.")

    df = df.replace(np.nan, 'Não informado')
    print("Valores NULL do dataframe substituidos por 'Não informado'.")

    df['idade'] = df['idade'].astype(str).str.replace('.0', '')
    print("Coluna 'idade' convertida para STRING.")

    display(df.head(5))
    df.info()

    return df
```

### 7º: Função de conexão ao banco de dados

Seguindo a lógica de organização e reaproveitamento do código, ao invés da função de carregamento dos dados ficar responsável por toda a criação da conexão e a ingestão dos dados, criei uma função separada que cria os parâmetros para que assim a função load() apenas receba os parâmetros e fique mais responsável por fazer a ingestão dos dados. Assim, além de ser mais seguro uma função separada com os dados da função (geralmente é utilizada para se ler os dados de outro lugar e não ficar salvo no código como eu fiz), a função gets_postgre_connection_parameters() realiza:
- A criação de um dicionário contendo os as informações necessárias para serem utilizadas como parâmetros de conexão.
- Retorna o dicionário.
- Se não, imprime error e gera uma mensagem contendo o problema.

A função não há parâmetro algum.

```python
def get_postgre_connection() -> pg.extensions.connection:

    '''Connect to postgre database. Steps:
        1. Creates a dict containing the connection parameters info;
        2. Tries to conect to the database;
        3. Prints a successfully message if the connection is established;
        4. Prints the error if it's not.
        
        Parameters:
        - None
        
        Return:
        - The database connection or the error message.'''
        
    conn_parameters = {
        'host': 'host_do_banco',
        'database': 'nome_do_banco',
        'user': 'usuário',
        'password': 'senha'
    }

    return conn_parameters
```

### 8º: Função de carregar os dados

Aqui é onde a magia acontece, pois é a função responsável por fazer a ingestão dos dados no banco. A função load() faz:
- Chama a funçao get_postgree_connetion() para receber os parâmetros da conexão.
- Tenta criar um objeto da biblioteca SQLAlchemy para estabelecer uma conexão com o banco de dados utilizando o dicionário de parâmetros de conexão.
- Caso a conexão seja bem estabelecida, ele configura o schema no qual será inserida a tabela utilizando a variável contendo o nome do schema como parâmetro. Também usa if_not_exist=True como parâmetro para indicar que deve criar o schema caso ele não exista.
- Realiza a ingestão de dados utilizando to_sql() do pandas com a variável contendo o nome que a tabela se chamará como parâmetro, a engine atribuída ao parâmetro de conexão, index=false como parâmetro para indicar que não deve adicionar a coluna de índice ao banco em por fim, if_exists='replace' para indicar que é pra repor todos os dados do banco pelo que está sendo ingerido caso exista. Como essa tabela não será alimentada, não será utilizado o método append.
- Caso tudo dê certo, ele imprime uma mensagem de sucesso, do contrário, imprime o erro.

```python
def load(df: pd.DataFrame, schema_name: str, table_name: str) -> None:

    '''Load the transformed dataframe into the postgre database. Steps:
        1. Calls the postgre connection function to establishe a connection with the database;
        2. Creates a SQLAlchemy object for the conection using the parameters connection in the postgre connection function;
        3. Sets the schema and creates it if doesn't exist;
        4. Loads the transformed dataframe into the database;
        5. Closes the connection;
        6. Prints a successfully message if the ingestion is complete;
        7. Prints the error message if it's not.
        
        Parameters:
        - Dataframe to be load;
        - String with the schema name where the table will be load;
        - String containing the name the table will be called.
        
        Return:
        - None.'''

    conn_params = get_postgre_connection_parameters()

    try:
        engine = create_engine(f"postgresql+psycopg2://{conn_params['user']}:{conn_params['password']}@{conn_params['host']}/{conn_params['database']}")
        
        with engine.connect() as connection:
            connection.execute(CreateSchema(schema_name, if_not_exists=True))
            connection.commit()

        df.to_sql(table_name, con=engine, index=False, if_exists='replace', schema=schema_name)
        print(">> Ingestão de dados concluída com sucesso <<")

        print("Conexão fechada.")

    except Exception as error:
        print(f"Erro ao fazer a ingestão de dados no banco de dados: {error}")
```

### 9º: Para não perder o costume

Finalmente, temos aqui a chamada da função main(). Eu sei que nem sempre esse trecho do código é necessário, mas gosto de usar sempre que sei que não altera o resultado final para manter a prática. Ele basicamente faz uma checagem de escopo de execução e verifica se o programa está sendo executado diretamente. E o resto é história.

```python
if __name__ == "__main__":

    main()
```

---
Bom, chegamos ao final. Aprecio você, guerreirx, que leu até aqui. Espero que não tenha dúvidas quanto ao código, ele não foi feito para ser um grande projeto, apenas para subir dados a um banco por eu achar que vai ser mais prático de utilizá-los (e com certeza vai). E, principalmente, para manter o bom padrão de boas práticas, pois algumas coisas que fiz eu sei que não eram necessárias, mas é bom manter o costume. Qualquer dúvida, só chamar no linkedin (: