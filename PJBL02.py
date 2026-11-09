# PjBL 02 - Spark | Big Data - BCC - PUCPR

import os  # Permite definir variáveis de ambiente
import sys  # Permite descobrir qual Python está executando o script
from pyspark.sql import SparkSession  # Ponto de entrada para usar o Spark
from pyspark.sql import functions as F  # Funções do Spark SQL (col, max, etc.)

# sys.executable é o caminho do Python atual. Essas variáveis garantem que
# o Spark use esse mesmo Python para executar o código das tarefas
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# builder: inicia a configuração da sessão
# appName: define o nome da aplicação
# master("local[*]"): roda na máquina local, usando todos os núcleos
# getOrCreate: cria a sessão ou reaproveita uma que já exista
spark = SparkSession.builder.appName("PjBL02").master("local[*]").getOrCreate()

# O SparkContext é o objeto usado para criar e manipular RDDs
sc = spark.sparkContext

# Mostra no log apenas as mensagens de erro
sc.setLogLevel("ERROR")

# textFile: cria um RDD em que cada elemento é uma linha do arquivo
# É uma operação lazy: o arquivo só é processado quando uma ação é chamada
linhas = sc.textFile("operacoes_comerciais_inteira.csv")

# RETIRADA DO CABEÇALHO
# filter: mantém apenas os elementos em que a função retorna True
# lambda: cria uma função curta, recebendo cada linha na variável "linha"
# startswith: retorna True se o texto começa com o valor informado
# not: inverte o resultado, mantendo as linhas que não são o cabeçalho
# Não usei first() porque ele usa take() e causou erro neste ambiente Windows
dados = linhas.filter(lambda linha: not linha.startswith("country_or_area"))

# map: aplica a função em cada elemento e gera um novo elemento para cada um
# split(";"): corta o texto em cada ";" e devolve uma lista com as colunas
# Posições: 0 Country, 1 Year, 2 Code, 3 Commodity, 4 Flow, 5 Price,
#           6 Weight, 7 Unit, 8 Amount, 9 Category
colunas = dados.map(lambda linha: linha.split(";"))

# TRATAMENTO DE DADOS FALTANTES
# Cada questão descarta somente as linhas em que falta uma coluna usada nela.
# Campos vazios aparecem como "" e são retirados nos filtros com != "".


# Q1 - Número de transações envolvendo o Brasil (RDD)
# filter: mantém as linhas cujo país (c[0]) é "Brazil"
# País vazio é descartado, pois "" é diferente de "Brazil"
brasil = colunas.filter(lambda c: c[0] == "Brazil")
# count: ação que retorna a quantidade de elementos do RDD
print("Q1:", brasil.count())


# Q2 - Número de transações envolvendo o Brasil (PairRDD)
q2 = (
    # map: transforma cada transação em um par (chave, valor) = (país, 1)
    brasil.map(lambda c: (c[0], 1))
    # reduceByKey: combina os valores de cada chave; a + b soma as ocorrências
    .reduceByKey(lambda a, b: a + b)
    # collect: ação que traz o resultado para o programa em uma lista
    .collect()
)
print("Q2:", q2)


# Q3 - Número de transações envolvendo o Brasil em 2016 (RDD)
# filter: das transações do Brasil, mantém as do ano (c[1]) 2016
brasil_2016 = brasil.filter(lambda c: c[1] == "2016")
# count: ação que conta as transações filtradas
print("Q3:", brasil_2016.count())


# Q4 - Número de transações envolvendo o Brasil em 2016 (PairRDD)
q4 = (
    # map: gera ((país, ano), 1); a chave é uma tupla com as duas informações
    brasil_2016.map(lambda c: ((c[0], c[1]), 1))
    # reduceByKey: soma as ocorrências da chave ("Brazil", "2016")
    .reduceByKey(lambda a, b: a + b)
    # collect: traz o resultado para o programa
    .collect()
)
print("Q4:", q4)


# Q5 - Número de transações por ano e flow, a partir de 2010 (PairRDD)
q5 = (
    # filter: exige ano e flow preenchidos e mantém anos a partir de 2010
    # int: converte o ano para número; o and impede a conversão de ano vazio
    colunas.filter(lambda c: c[1] != "" and c[4] != "" and int(c[1]) >= 2010)
    # map: gera ((ano, flow), 1); upper coloca o flow em letras maiúsculas
    .map(lambda c: ((c[1], c[4].upper()), 1))
    # reduceByKey: soma as ocorrências de cada par (ano, flow)
    .reduceByKey(lambda a, b: a + b)
    # sortByKey: ordena pela chave, ou seja, por ano e depois por flow
    .sortByKey()
    # collect: traz o resultado para o programa
    .collect()
)
print("Q5:", q5)


# Q6 - Média de Price em 2016 (RDD)
precos_2016 = (
    # filter: mantém 2016 e descarta as linhas com preço vazio
    colunas.filter(lambda c: c[1] == "2016" and c[5] != "")
    # map: mantém somente o preço; float converte de texto para número
    .map(lambda c: float(c[5]))
)
# reduce: ação que combina todos os elementos; aqui, soma os preços
soma = precos_2016.reduce(lambda a, b: a + b)
# count: ação que conta quantos preços existem
quantidade = precos_2016.count()
# A média é a soma dividida pela quantidade
print("Q6:", soma / quantidade)


# Q7 - Média de Price em 2016 (PairRDD)
q7 = (
    # filter: mantém 2016 e descarta as linhas com preço vazio
    colunas.filter(lambda c: c[1] == "2016" and c[5] != "")
    # map: gera (ano, (preço, 1)), guardando valor e contador juntos
    .map(lambda c: (c[1], (float(c[5]), 1)))
    # reduceByKey: soma os preços (posição 0) e as quantidades (posição 1)
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    # mapValues: altera só o valor, mantendo a chave; calcula soma / quantidade
    .mapValues(lambda v: v[0] / v[1])
    # collect: traz o resultado para o programa
    .collect()
)
print("Q7:", q7)


# Q8 - Preço máximo e mínimo por ano e categoria (PairRDD)
# A chave pedida não tem país, então a saída foi ordenada por (ano, categoria)
q8 = (
    # filter: exige ano, categoria e preço preenchidos
    colunas.filter(lambda c: c[1] != "" and c[9] != "" and c[5] != "")
    # map: gera ((ano, CATEGORIA), (preço, preço))
    # A primeira posição do valor guarda o máximo e a segunda guarda o mínimo
    .map(lambda c: ((c[1], c[9].upper()), (float(c[5]), float(c[5]))))
    # reduceByKey: max mantém o maior preço e min mantém o menor preço
    .reduceByKey(lambda a, b: (max(a[0], b[0]), min(a[1], b[1])))
    # sortByKey: ordena por ano e depois por categoria
    .sortByKey()
    # collect: traz o resultado para o programa
    .collect()
)
print("Q8:", q8)


# Q9 - País com a maior exportação (PairRDD)
# Maior exportação considerada como a maior soma de Price por país
q9 = (
    # filter: exige país e preço preenchidos e mantém apenas Flow = Export
    colunas.filter(lambda c: c[0] != "" and c[4] == "Export" and c[5] != "")
    # map: gera ((país, flow), preço)
    .map(lambda c: ((c[0], c[4]), float(c[5])))
    # reduceByKey: soma os preços de exportação de cada país
    .reduceByKey(lambda a, b: a + b)
    # sortBy: ordena pelo valor (par[1]); ascending=False vai do maior ao menor
    .sortBy(lambda par: par[1], ascending=False)
    # collect traz a lista ordenada e [0] pega o primeiro, o maior valor
    .collect()[0]
)
print("Q9:", q9)


# Q10 - Transação Export com o maior preço por kg (funções do Spark SQL)
# spark.read.csv: lê o arquivo como DataFrame, uma tabela com colunas nomeadas
# sep=";": define o separador; header=True usa a primeira linha como cabeçalho
df = spark.read.csv("operacoes_comerciais_inteira.csv", sep=";", header=True)

exportacoes = (
    # filter: mantém as linhas com flow igual a Export
    # F.col: faz referência a uma coluna do DataFrame pelo nome
    df.filter(F.col("flow") == "Export")
    # withColumn: cria uma coluna; cast("double") converte texto para número
    .withColumn("preco", F.col("trade_usd").cast("double"))
    .withColumn("peso", F.col("weight_kg").cast("double"))
    # dropna: remove as linhas com valor nulo nas colunas indicadas
    .dropna(subset=["preco", "peso"])
    # filter: mantém peso maior que zero para evitar divisão por zero
    .filter(F.col("peso") > 0)
    # withColumn: cria a coluna com o preço dividido pelo peso
    .withColumn("preco_por_kg", F.col("preco") / F.col("peso"))
)

# agg: calcula uma agregação sobre o DataFrame
# F.max: encontra o maior valor da coluna
# collect()[0][0]: pega o valor da primeira linha e da primeira coluna
maior = exportacoes.agg(F.max("preco_por_kg")).collect()[0][0]

q10 = (
    # filter: busca a transação que tem o maior preço por kg
    exportacoes.filter(F.col("preco_por_kg") == maior)
    # select: escolhe as colunas exibidas: ano, país, categoria e preço por kg
    .select("year", "country_or_area", "category", "preco_por_kg")
    # collect: traz o resultado para o programa
    .collect()
)
print("Q10:", q10)