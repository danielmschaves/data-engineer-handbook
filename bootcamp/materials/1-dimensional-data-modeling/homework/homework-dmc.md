
# Análise Histórica de Carreira de Atores - Sistema de Dados Cumulativo e Versionado

## Contexto do Projeto
O projeto tem como objetivo criar um sistema de análise da carreira de atores ao longo do tempo, baseado em dados de filmes, avaliações e períodos de atividade. O sistema foi desenvolvido para processar dados históricos e manter tanto uma visão cumulativa quanto um histórico detalhado de mudanças.

## Base de Dados Original
- **Tabela**: `actor_films`
- **Campos**:
  - `actor`: Nome do ator
  - `actorid`: ID único do ator (formato 'nm######')
  - `film`: Nome do filme
  - `year`: Ano de lançamento
  - `votes`: Número de votos recebidos
  - `rating`: Avaliação média
  - `filmid`: ID único do filme (formato 'tt######')

## Estruturas de Dados Implementadas

### 1. Tipos Customizados
```sql
-- Estrutura para filmes no array cumulativo
CREATE TYPE film_details AS (
    film text,
    votes integer,
    rating real,
    filmid text
);

-- Classificação de qualidade do ator
CREATE TYPE quality_class AS ENUM (
    'star',    -- rating > 8
    'good',    -- rating > 7 e ≤ 8
    'average', -- rating > 6 e ≤ 7
    'bad'      -- rating ≤ 6
);

-- Estrutura para versionamento histórico
CREATE TYPE actor_scd_type AS (
    quality_class quality_class,
    is_active boolean,
    start_season INTEGER,
    end_season INTEGER
);
```

### 2. Tabelas Principais
```sql
-- Tabela cumulativa
CREATE TABLE actors (
    actor_name text,
    actorid text,
    films film_details[],
    quality_class quality_class,
    is_active boolean,
    current_season integer,
    PRIMARY KEY (actorid, current_season)
);

-- Tabela de histórico (SCD Tipo 2)
CREATE TABLE actors_history_scd (
    actor_name text,
    actorid text,
    quality_class quality_class,
    is_active boolean,
    start_season integer,
    end_season integer,
    current_season integer,
    PRIMARY KEY (actorid, start_season, end_season)
);
```

## Processos Implementados

### 1. Tabela Cumulativa (actors)

#### Primeira Carga
- Identifica o ano mais antigo
- Processa todos os filmes do ano
- Calcula métricas iniciais
- Marca todos atores como ativos

#### Cargas Incrementais
- Usa estrutura yesterday/today
- Mantém histórico completo no array de filmes
- Atualiza métricas baseado no ano atual
- Atualiza status de atividade

### 2. Histórico Versionado (actors_history_scd)

#### Processo de Backfill
- Identifica períodos de mudança
- Agrupa períodos contínuos
- Gera registros históricos completos

#### Processo Incremental
- Identifica registros:
  - Históricos (fechados)
  - Inalterados
  - Modificados
  - Novos
- Mantém versionamento apropriado

## Técnicas Avançadas Utilizadas

### 1. Window Functions
```sql
AVG(rating) OVER (PARTITION BY actorid)
ROW_NUMBER() OVER (PARTITION BY actorid ORDER BY rating DESC)
LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_season)
```

### 2. Arrays e Agregações
```sql
ARRAY_AGG(ROW(film, votes, rating, filmid)::film_details)
films || ARRAY[ROW(...)]
```

### 3. CTEs Estruturadas
- Organização lógica do processamento
- Separação clara de responsabilidades
- Facilidade de manutenção

## Boas Práticas Implementadas

1. **Documentação**
   - Comentários explicativos
   - Documentação de estruturas
   - Explicação de lógicas complexas

2. **Organização de Código**
   - CTEs bem nomeadas
   - Estrutura lógica de processamento
   - Separação clara de responsabilidades

3. **Tratamento de Dados**
   - Validações apropriadas
   - Tratamento de casos especiais
   - Garantia de integridade

4. **Performance**
   - Índices apropriados
   - Processamento eficiente
   - Controle de duplicatas

## Conclusão
O projeto implementa um sistema robusto de análise histórica, combinando técnicas avançadas de SQL com boas práticas de desenvolvimento e modelagem de dados. A solução permite tanto a análise cumulativa quanto o acompanhamento detalhado de mudanças ao longo do tempo.