# Censosup Dados

Esse projeto foi criado visando automatizar o processo de download e ingestão dos dados disponibilizados pelo inep, de forma simples utilizando pandas.

## O que este projeto faz?

Baseado em uma lista de URL`s para os arquivos do INEP, este script faz download destes dados, desempacota os arquivos e carrega em um banco de dados postgres.

## Os dados são tratados?

Não, são feitos alguns ajustes necessários para não ter erro de encoding na hora de inserir no banco de dados e nada mais, a ideia é ter o dado identico ao disponibilizado pelo inep e tratamentos podem ser feitos posteriormente por quem quiser consumir estes dados.


# Como funciona
1. Clone o projeto
   ```
   git clone git@github.com:alandrebraga/dados-censo.git
   ```
2. Instale as dependências necessárias para o projeto com o comando abaixo:

    ```
    pip install -r requirements.txt
    ```
3. Após isto, coloque as credencias do seu banco de dados postgres no arquivo (`database.ini`)
4. Estando no source do projeto basta rodar o arquivo (`app.py`) e ele vai baixar todos os zips das urls no arquivo (`urls.txt`), por padrão deixei os ultimos cinco anos.
5. Para melhor proveito das consultas em sql, é recomendado que baixe pelo menos 1 arquivo zip e pegue a legenda de dados do INEP, o nome das colunas foram mantidos os mesmos, o arquivo pode ser encontrado [clicando aqui](https://www.gov.br/inep/pt-br/areas-de-atuacao/pesquisas-estatisticas-e-indicadores/censo-da-educacao-superior/resultados).

# Tabela no banco de dados
Modelagem:
Como o ER ficou bem grande por conta da quantidade de colunas vou descrever a modelagem. Temos tabelas de instituições de ensino e de cursos, na tabela de instituições temos a chave primaria pelo código da universidade, essa chave primeira é usada de chave estrangeira na tabela de cursos.

## Exemplo de consulta dos dados
![sql example](/img/sql_example.png)
# To-Do

- Adicionar testes para os dados (preciso estudar sobre como validar os dados com testes)
- Extender o projeto a outros banco de dados como mySQL, SQLite e posteriormente a bancos noSQL.


# Tempo de execução
O tempo de execucão vai variar de acordo com a conexão da internet já que ele vai baixar os arquivos.
Na parte do código o processo que mais demora é uma operacão de concatenacão de dataframes que pode levar de 50s a 1m30s.
Testei utilizar spark para testar a velocidade e caiu de 50s a 30s para esta operacão, todavia, não acho que valha o tradeoff principalmente na parte de rodar este projeto em outros ambientes, já que a ideia é mante-lo simples e como é um processo que roda-se a cada ano praticamente, não tem vejo necessidade nesta alteracão.


# Como contrbuir
1. Clone o repositório e cri auma branch nova
2. Faça a mudança e teste
3. Envie um PR com a descricão das mudanças

contato: https://www.linkedin.com/in/andrelbmallmann/
