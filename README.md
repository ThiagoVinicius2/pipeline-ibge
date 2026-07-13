# pipeline-ibge

Pipeline de dados que extrai informações públicas do **IBGE** (estados, municípios e estimativas de população), armazena em um data warehouse **PostgreSQL** e as transforma em tabelas analíticas prontas para consumo. Toda a orquestração é feita com **Apache Airflow**, e o ambiente é 100% reproduzível via **Docker Compose**.

Projeto construído exclusivamente com ferramentas open source, do zero, como estudo prático de engenharia de dados e do *Modern Data Stack*.

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

Essa ordem não é arbitrária: ela reflete as chaves estrangeiras do modelo (`municipios` referencia `estados`; `populacao` referencia `municipios`; os `marts` cruzam as três tabelas).

---

## Stack

| Ferramenta | Papel no projeto |
|------------|------------------|
| **Apache Airflow 3.3.0** | Orquestração — define a ordem das tarefas, executa, registra logs e agenda |
| **PostgreSQL 16** | Data warehouse — armazena os dados e onde as transformações SQL rodam |
| **Docker + Docker Compose** | Ambiente reproduzível — sobe Airflow e Postgres com um comando |
| **Python** | Extração (`requests`) e definição das DAGs (TaskFlow API) |
| **Git + GitHub Codespaces** | Versionamento e ambiente de desenvolvimento na nuvem |

O Airflow roda com **LocalExecutor** (mais leve, ideal para desenvolvimento). Há dois bancos: o de *metadados* do Airflow e um `warehouse` separado, onde moram os dados do projeto.

---

## Fontes de dados

Todas as APIs do IBGE são públicas e não exigem autenticação.

- **Localidades** — estados e municípios
  `https://servicodados.ibge.gov.br/api/v1/localidades/estados`
  `https://servicodados.ibge.gov.br/api/v1/localidades/municipios`
- **Agregados (SIDRA)** — estimativas de população (tabela 6579, variável 9324)
  `https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/{ano}/variaveis/9324?localidades=N6[all]`

---

## Estrutura do projeto

```
pipeline-ibge/
├── dags/
│   └── pipeline_ibge.py       # o pipeline completo (extração → carga → transformação)
├── sql/
│   └── marts_municipio_mais_populoso_uf.sql   # transformação da camada marts
├── include/                   # reservado para funções auxiliares (uso futuro)
├── docker-compose.yaml        # Airflow + Postgres (metadados e warehouse)
├── .env                       # AIRFLOW_UID e FERNET_KEY (não versionado)
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

**6. Ativar o DAG `pipeline_ibge`** na interface. Ele roda automaticamente todo dia (`@daily`) ou pode ser disparado manualmente.

Para consultar os dados diretamente no banco:

```bash
docker compose exec warehouse psql -U dados -d warehouse
```

---

## O que o pipeline produz

Ao final de uma execução, o warehouse contém:

| Tabela | Conteúdo | Linhas |
|--------|----------|--------|
| `raw.estados` | 27 unidades da federação | 27 |
| `raw.municipios` | municípios brasileiros (com FK para estados) | 5.571 |
| `raw.populacao` | estimativa de população por município e ano | ~5.570 |
| `marts.municipio_mais_populoso_uf` | o município mais populoso de cada estado | 27 |

Exemplo de consulta analítica possível (município mais populoso por estado), que revela padrões reais — como o fato de o maior município do Espírito Santo ser Serra (não a capital) e o de Santa Catarina ser Joinville.

---

## Conceitos aplicados

- **Padrão de camadas `raw` → `marts`** — separação entre dado bruto e dado tratado.
- **Idempotência** — rodar o pipeline novamente não duplica nem corrompe dados (`TRUNCATE`/`DELETE` antes de inserir).
- **Integridade referencial** — chaves primárias e estrangeiras garantindo consistência entre tabelas.
- **Robustez a dados imperfeitos** — tratamento de valores nulos e sentinelas não-numéricas do IBGE.
- **Orquestração de dependências** — ordem de execução garantida via operador `>>` da TaskFlow API.
- **Agendamento** — execução automática recorrente sem intervenção manual.

---

## Próximos passos possíveis

- Usar a *data lógica* do Airflow para carregar múltiplos anos de população automaticamente.
- Adicionar tarefas de validação de qualidade de dados.
- Expandir a camada `marts` (análises por região, por faixa de população).
- Refatorar a lógica de extração para módulos reutilizáveis em `include/`.

---

*Projeto de estudo — engenharia de dados com ferramentas open source.*