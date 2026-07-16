# pipeline-ibge

Pipeline de dados que extrai informações públicas do **IBGE** (estados, municípios e estimativas anuais de população), armazena em um data warehouse **PostgreSQL** e as transforma em tabelas analíticas prontas para consumo. Toda a orquestração é feita com **Apache Airflow**, e o ambiente é 100% reproduzível via **Docker Compose**.

Construído do zero, exclusivamente com ferramentas open source do *Modern Data Stack*.

![Airflow](https://img.shields.io/badge/Apache%20Airflow-3.3.0-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)

---

## Em uma linha

**5.571 municípios · ~50 mil registros de população · backfill anual desde 2015 · DAG idempotente · quebra de série metodológica tratada na modelagem.**

O ponto mais interessante do projeto não é técnico: as estimativas do IBGE mudaram de base após o Censo de 2022, e comparar 2021 com 2024 diretamente faz São Paulo "perder" 675 mil habitantes — variação que nunca existiu. A solução está em [Nota metodológica](#nota-metodológica-quebra-de-série).

---

## Arquitetura

O fluxo segue o padrão de camadas `raw` → `marts`, comum em equipes de dados modernas:

```
APIs do IBGE  ─►  Airflow (extrai + carrega)  ─►  PostgreSQL: schema raw  ─►  transformação SQL  ─►  PostgreSQL: schema marts
```

- **`raw`** — dados crus, fiéis à fonte (registro histórico confiável).
- **`marts`** — dados tratados, cruzados e agregados, prontos para análise.

O pipeline é um único DAG (`pipeline_ibge`) com as etapas encadeadas por dependências explícitas, garantindo a ordem correta de execução:

```
estados ─► municípios ─► população ─► marts
```

Essa ordem não é arbitrária: reflete as chaves estrangeiras do modelo (`municipios` referencia `estados`; `populacao` referencia `municipios`; os `marts` cruzam as três tabelas).

### Dimensões e fato temporal

O pipeline lida com dois tipos de dado de naturezas distintas:

- **Dimensões** (estados, municípios) — não variam com o tempo. São carregadas via **UPSERT** (`INSERT ... ON CONFLICT DO UPDATE`), o que permite recarregá-las a qualquer momento sem apagar os dados dependentes.
- **Fato temporal** (população) — uma linha por município **e ano**, com chave primária composta `(municipio_id, ano)`. Cada execução carrega apenas o ano correspondente à sua *data lógica*, removendo antes só aquele ano (`DELETE ... WHERE ano = %s`) e preservando os demais.

### Agendamento e backfill

O DAG roda com `schedule="@yearly"` e `catchup=True`, a partir de 2015. Na primeira ativação, o Airflow executa automaticamente um *run* por ano (**backfill**), cada um carregando a população do seu respectivo ano a partir da data lógica. O parâmetro `max_active_runs=1` garante execução sequencial.

Todas as tarefas têm `retries=3` com intervalo de 30s, para absorver falhas transitórias das APIs (rate limiting, timeouts).

---

## Stack

| Ferramenta | Papel no projeto |
|------------|------------------|
| **Apache Airflow 3.3.0** | Orquestração — ordem das tarefas, execução, logs, agendamento e backfill |
| **PostgreSQL 16** | Data warehouse — armazena os dados e executa as transformações SQL |
| **Docker + Docker Compose** | Ambiente reproduzível — sobe Airflow e Postgres com um comando |
| **Python** | Extração (`requests`) e definição do DAG (TaskFlow API) |
| **Git + GitHub Codespaces** | Versionamento e ambiente de desenvolvimento na nuvem |

O Airflow roda com **LocalExecutor**. Há dois bancos: o de *metadados* do Airflow e um `warehouse` separado, onde moram os dados do projeto.

---

## Fontes de dados

Todas as APIs do IBGE são públicas e não exigem autenticação.

- **Localidades** — estados e municípios
  `https://servicodados.ibge.gov.br/api/v1/localidades/estados`
  `https://servicodados.ibge.gov.br/api/v1/localidades/municipios`
- **Agregados (SIDRA)** — estimativas de população (tabela 6579, variável 9324)
  `https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/{ano}/variaveis/9324?localidades=N6[all]`

> Nem todos os anos possuem estimativa (anos de Censo, como 2010 e 2022, não têm). Nesses casos a API retorna `[]` com status HTTP 200 — o pipeline trata essa resposta e conclui o *run* sem carregar nada, em vez de falhar.

---

## Estrutura do projeto

```
pipeline-ibge/
├── dags/
│   └── pipeline_ibge.py                        # pipeline completo (extração → carga → transformação)
├── sql/
│   ├── marts_municipio_mais_populoso_uf.sql    # maior município de cada estado
│   └── marts_populacao_uf_evolucao.sql         # série anual e variação por estado
├── include/                                    # reservado para funções auxiliares (uso futuro)
├── docker-compose.yaml                         # Airflow + Postgres (metadados e warehouse)
├── .env                                        # AIRFLOW_UID e FERNET_KEY (não versionado)
├── .gitignore
└── README.md
```

---

## Como executar

Pré-requisitos: Docker e Docker Compose (ou abrir o repositório no GitHub Codespaces, que já traz tudo).

**1. Criar o arquivo `.env`** com o UID do usuário e uma chave de criptografia:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
echo "FERNET_KEY=$(python3 -c 'import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())')" >> .env
```

**2. Inicializar o Airflow** (cria as tabelas de metadados e o usuário admin):

```bash
docker compose up airflow-init
```

**3. Subir todos os serviços:**

```bash
docker compose up -d
```

**4. Acessar a interface** em `http://localhost:8080` (no Codespace, use a porta encaminhada). Login padrão: `airflow` / `airflow`.

**5. Cadastrar a conexão com o warehouse** em *Admin → Connections*:

| Campo | Valor |
|-------|-------|
| Connection Id | `warehouse` |
| Connection Type | `Postgres` |
| Host | `warehouse` |
| Database | `warehouse` |
| Login / Password | `dados` / `dados` |
| Port | `5432` |

**6. Ativar o DAG `pipeline_ibge`.** O backfill começa automaticamente, executando um *run* por ano desde 2015.

Para consultar os dados diretamente no banco:

```bash
docker compose exec warehouse psql -U dados -d warehouse
```

---

## O que o pipeline produz

### Camada `raw`

| Tabela | Conteúdo | Linhas |
|--------|----------|--------|
| `raw.estados` | 27 unidades da federação | 27 |
| `raw.municipios` | municípios brasileiros (FK para estados) | 5.571 |
| `raw.populacao` | população por município **e ano** (2015–2025) | ~50.000 |

### Camada `marts`

| Tabela | Conteúdo |
|--------|----------|
| `marts.municipio_mais_populoso_uf` | o município mais populoso de cada estado, sempre no ano mais recente disponível |
| `marts.populacao_uf_evolucao` | série anual de população por estado, com variação absoluta e percentual ano a ano |

---

## Nota metodológica: quebra de série

As estimativas do IBGE mudaram de base após o **Censo de 2022**. Os anos de 2015 a 2021 derivam do Censo 2010; 2024 e 2025 derivam do Censo 2022. Comparar valores entre as duas séries produz variações **falsas** — São Paulo, por exemplo, aparenta ter "perdido" 675 mil habitantes entre 2021 e 2024, quando o que mudou foi a metodologia.

O mart `populacao_uf_evolucao` trata isso na modelagem: a coluna `serie_metodologica` identifica cada série, e o cálculo de variação usa `PARTITION BY estado_sigla, serie_metodologica`. Assim, a *window function* nunca compara através da fronteira entre séries — a variação do primeiro ano de cada série é `NULL`, sinalizando "não comparável" em vez de exibir um número inválido.

A decisão de fundo: um pipeline que entrega número errado com confiança é pior que um pipeline quebrado. O quebrado avisa.

---

## Conceitos aplicados

- **Padrão de camadas `raw` → `marts`** — separação entre dado bruto e dado tratado.
- **Idempotência** — reexecutar não duplica nem corrompe dados (`UPSERT` nas dimensões, `DELETE` por ano no fato).
- **Integridade referencial** — chaves primárias (simples e compostas) e estrangeiras garantindo consistência.
- **Robustez a dados imperfeitos** — tratamento de valores nulos, sentinelas não-numéricas e respostas vazias com status 200.
- **Orquestração de dependências** — ordem garantida via operador `>>` da TaskFlow API.
- **Data lógica e backfill** — cada execução processa o período que representa; o histórico é reconstruído automaticamente.
- **Resiliência** — `retries` para absorver falhas transitórias de APIs externas.
- **SQL analítico** — CTEs (`WITH`), *window functions* (`LAG ... OVER (PARTITION BY ...)`), `CASE WHEN` e `JOIN` entre múltiplas tabelas.

---

## Roadmap

- Tarefas de validação de qualidade de dados (ex.: falhar se vierem menos de 5.000 municípios).
- Expansão da camada `marts` (análises por região, por faixa de população).
- Refatoração da lógica de extração para módulos reutilizáveis em `include/`.
- Carga da série histórica completa (o IBGE disponibiliza estimativas desde 2001).

---

## Autor

**Thiago Vinicius** — Analytics Engineer | Analista de Dados & BI
[LinkedIn](https://www.linkedin.com/in/thiagovinicius1/) · [GitHub](https://github.com/ThiagoVinicius2)
