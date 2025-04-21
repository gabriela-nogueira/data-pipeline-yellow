# Projeto ETL Yellow Taxi Data 

Este repositório contém o pipeline de ingestão de dados fornecidos no site de Nova York onde que possui informações sobre os registros de viagens de táxi na cidade.

## Objetivo do Projeto

Criar um pipeline que ingeste as informações de janeiro-maio de 2023 e após isso realizar algumas consultas para responder algumas perguntas em questão.

## Pré-Requisitos

- Python 3.10
- Ambiente virtual
- Linux
- Bibliotecas (requirements.txt)
- Usuário IAM AWS com acesso ao S3 e Athena

## Estrutura do Projeto

- **dags**: contém os scripts para a criação do pipeline no Airflow
- **notebooks**: os notebooks com as queries de resposta para as perguntas do projeto
- **scripts**: os scripts modularizados separados por funcionalidade.
- **temp**: pasta temporária para armazenamento temporário dos arquivos gerados no pipeline.

## Arquitetura Proposta

## Como executar o projeto localmente

- Crie um ambiente virtual python
- Instale as dependências do projeto
- Crie um arquivo .env dentro da pasta ./scripts com as variáveis de ambiente: 
- Instale o airflow
- Configure o usuário do airflow
- Execute o airflow

## Features Implementadas

- Download de arquivos por requests
- Upload de arquivos nos buckets S3 padronizado
- Transformação do dado para salvar apenas o que é necessário
- Criação da tabela para consumo no Athena
- Criação da tabela/partição via HQL

## Pontos de Melhorias Futuras
