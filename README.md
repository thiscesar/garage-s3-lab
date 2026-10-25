# Laboratório Garage S3 — Tema 3, Ênfase A (modos de consistência)

Trabalho M2 de Sistemas Distribuídos (Ciência da Computação, UNIVALI 2026/2). Um cluster de **3 nós
[Garage](https://garagehq.deuxfleurs.fr/) v2.3.0** em contêineres Docker, com uma bateria de experimentos
automatizada (E1–E9) que mede replicação, quóruns, partição, recuperação e coordenação. A Ênfase A compara os
três `consistency_mode` do Garage — `consistent`, `degraded` e `dangerous` — em disponibilidade e frescor das
leituras.

> **Sobre o kit do roteiro:** o roteiro cita um kit (`docker-compose.yml`, `garage1.toml`, `iniciar.sh`, `s3.py`),
> mas não diz onde obtê-lo. A equipe construiu o próprio ambiente a partir dos trechos do roteiro (§7.2), com os
> mesmos nomes de arquivos, portas, bucket (`sdlab`) e chave (`sd-key`).

Este README tem duas partes: [resultados](#1-resultados) e [como executar](#2-como-executar).

## Visão geral

| Nó | Contêiner | Rede `interna` (RPC entre nós) | Rede `clientes` (S3 e Admin) | Zona |
|---|---|---|---|---|
| 1 | `garage1` | `<REDE_INTERNA_PREFIXO>.21` | `<REDE_CLIENTES_PREFIXO>.21` | dz1 |
| 2 | `garage2` | `<REDE_INTERNA_PREFIXO>.22` | `<REDE_CLIENTES_PREFIXO>.22` | dz2 |
| 3 | `garage3` | `<REDE_INTERNA_PREFIXO>.23` | `<REDE_CLIENTES_PREFIXO>.23` | dz3 |
| — | `cliente` (boto3) | — | `<REDE_CLIENTES_PREFIXO>.100` | — |

Portas em cada nó: **3900** S3 · **3901** RPC · **3903** Admin. Faixas padrão (as do roteiro): interna
`172.30.0.0/24`, clientes `172.31.0.0/24` — configuráveis no `.env` ([ver abaixo](#faixas-de-rede-env)).
`replication_factor = 3`, 1 GB por nó, limite de memória de 128 MiB por contêiner.

Uma **partição** é feita com `docker network disconnect` da rede `interna`: o nó para de falar com os outros, mas
continua acessível ao cliente pela rede `clientes`.

| Modo | W (escrita) | R (leitura) | R + W > N (N = 3)? |
|---|---|---|---|
| `consistent` | 2 | 2 | sim → leitura após escrita garantida |
| `degraded` | 2 | 1 | não |
| `dangerous` | 1 | 1 | não |

### Mapa do repositório

| Caminho | Conteúdo |
|---|---|
| `lab/` | biblioteca Python: configuração, Docker, Admin API do Garage, clientes S3, gerador de carga, oráculo, estatísticas, figuras, relatórios e a CLI `labctl` |
| `painel/` | painel web da demo ao vivo (FastAPI + SSE; página em `painel/static/`, com o Chart.js 4.4.7 incluído para funcionar sem internet) |
| `lab/experiments/` | um módulo por experimento (`e1_topologia.py` … `e9_modos.py`) e a classe base (pré-condição, repetições, manifest, restauração do cluster) |
| `predicoes/` | previsões de cada experimento (`E1.yaml` … `E9.yaml`), com fontes; o resultado é confrontado com elas automaticamente |
| `config/garage.toml.j2` | modelo único da configuração; gerado por nó e por modo em `generated/` |
| `scripts/` | `iniciar.sh` (monta o cluster), `reset.sh`, `modo.sh`, `particionar.sh`, `curar.sh` |
| `cliente/` | imagem do contêiner `cliente` (Python + boto3 fixado) e o `s3.py` do roteiro |
| `tests/` | testes unitários (sem Docker) e de integração (com o cluster) |
| `docker-compose.yml`, `Makefile`, `pyproject.toml`, `.env.example` | infraestrutura e dependências |

Ficam **fora do git** (no `.gitignore`): `.env` (segredos), `dados/` (discos dos nós), `generated/`, `results/`
(saídas dos experimentos) e os documentos de apoio ao relatório.

---

## 1. Resultados

### Experimentos E1–E9 (execuções de referência de 25/10/2026, 5 repetições)

Placar das previsões: **36 de 42 confirmadas**. As refutações foram investigadas e registradas, nunca ajustadas
depois de rodar (o critério de cada previsão é definido no código antes da execução).

| Exp. | O que mostra | Resultado principal | Previsões |
|---|---|---|---|
| **E1** Subida e topologia | nós, zonas, layout, memória, ausência de líder | 3 nós em dz1–dz3 com 1 GB; layout com 256 partições, todas em cada nó; ~9 MiB de memória por nó | 5/5 |
| **E2** Caminho de uma operação | qualquer nó é gateway; para onde vão os RPCs de um PUT e de um GET | PUT vai às 3 réplicas e **começa com uma leitura** (para calcular o carimbo); GET em `consistent` consulta 2 nós; o bloco de 2 MiB é lido só do disco do nó de entrada | 5/5 |
| **E3** Replicação | objeto de 8 MiB nos 3 discos; latência; propagação | 8,000 MiB em cada nó e SHA-256 igual; PUT ≈ 52 ms × GET ≈ 13 ms (4 KiB); propagação 0 em 90/90 leituras | 4/4 |
| **E4** Consistência | leitura logo após a escrita, sem falhas | `consistent`: 0 desatualizadas em 2 000; `degraded` 0,5–1,0% e `dangerous` 1,8–2,7%. **Refutada:** a listagem em `dangerous` falhou em ~14% (previsto ≤ 5%); sondagem mostrou listagem = GET quando medidos no mesmo ritmo (hipótese: efeito da cadência das escritas) | 3/4 |
| **E5** Falha e quórum | parar 1 e 2 nós em cada modo | em regime, o quadro "nós ativos × leitura × escrita" bate com N/R/W nos 9 casos. **Refutadas** por transitórios: em `consistent`, logo após parar um nó, até ~60 s de timeouts por um nó de entrada; voltar a escrever depois de religar levou 13–19 s (previsto < 15 s) | 1/4 |
| **E6** Partição de rede | garage3 isolado por 120 s em `consistent` | maioria 96,4% disponível (timeouts só na janela de detecção); minoria 0/2 299 (nenhuma leitura desatualizada); detecção da partição ≈ 79 s; malha refeita ≈ 25 s após curar → **C em vez de A** | 5/5 |
| **E7** Recuperação | o nó que volta recebe o que perdeu? anti-entropia e `repair` | sem `repair`: tabelas iguais em ~475 s e dados em ~1 075 s (+600 s de agendamento no código); com `repair`: ~34 s; estado final idêntico. **Refutada:** previsão de ~10 min para a convergência natural completa | 3/4 |
| **E8** Coordenação e ordem | sem líder; PUTs concorrentes; layout versionado; `CreateBucket` concorrente | valor único em 250/250 pares (LWW), venceu o PUT que começou depois em 84%; `apply` com versão errada recusado; `CreateBucket` concorrente gerou 3 nomes duplicados e 17 buckets órfãos | 5/5 |
| **E9** Ênfase A: modos sob partição | garage3 isolado por 120 s em cada modo; escrita pela maioria, leitura pelos dois lados, listagem e escrita pela minoria | minoria: `consistent` 0/2 313 respondidas (C); `degraded` e `dangerous` 100% respondidas e **100% desatualizadas** (A), garage3 congelado na versão do início da partição; escrita pela minoria só aceita em `dangerous`. **Refutada:** em `dangerous`, ~9% das leituras da maioria desatualizadas (previsto ≤ 5%) — a mesma taxa sem partição, efeito de W = 1/R = 1 sob carga | 5/6 |

> **Correção de 25/10/2026:** no teste deste README descobrimos que o cliente boto3 fazia **uma nova tentativa**
> depois de timeout ou de erro 503 (no botocore, `max_attempts: 1` = "1 *nova* tentativa"; "sem retries" é
> `total_max_attempts: 1`). O cliente foi corrigido (com teste) e E5 e E6 foram refeitos: o placar não mudou, mas a
> nova tentativa escondia falhas (ex.: no E6 a maioria tinha aparecido com 99,6% de disponibilidade). E1–E4, E7 e
> E8 não tiveram erro S3, então não mudam.

### Onde estão os resultados

Cada execução grava em `results/<experimento>/<data-hora>/`:

| Arquivo | Conteúdo |
|---|---|
| `summary.md` | previsão × resultado, tabelas, figuras e a seção **"Análise (a preencher pela equipe)"** |
| `manifest.json` | commit, versões (Garage, Docker, Python, boto3), host, faixas de rede, parâmetros, início/fim, repetições |
| `predicao.yaml` | cópia da previsão usada naquela execução |
| `ops*.jsonl`, `metrics.csv` | dados brutos (uma linha por operação) e agregados |
| `evidence/`, `figures/` | saídas de comandos do Garage com cabeçalho e as figuras (PNG + SVG) |

`results/` **não vai para o git**. As execuções de referência estão na máquina da equipe e serão compartilhadas à
parte: E1 `2026-10-25T00-47-26`, E2 `00-56-51`, E3 `01-11-14`, E4 `01-21-39`, E5 `14-21-46`, E6 `14-46-04`,
E7 `07-38-20`, E8 `02-56-57`, E9 `16-03-06`.

---

## 2. Como executar

### Pré-requisitos

| Ferramenta | Mínimo | Testado |
|---|---|---|
| Linux (Docker nativo; o host precisa alcançar os IPs das redes bridge) | — | Ubuntu 24.04 |
| Docker Engine, usável **sem sudo** (usuário no grupo `docker`) | 24 | 29.9.1 |
| Plugin Docker Compose (`docker compose`) | v2 | v5.6.0 |
| Python | 3.11 | 3.13.1 |
| GNU make, Git, OpenSSL | — | 4.3 · 2.43 · 3.6 |

Na primeira vez é preciso internet (imagens Docker e pacotes Python). Depois disso, o laboratório sobe offline.
Imagem do Garage fixada em `dxflrs/garage:v2.3.0`.

### Subir do zero

```bash
git clone https://github.com/thiscesar/garage-s3-lab.git
```

```bash
cd garage-s3-lab
```

```bash
make up
```

O `make up` faz tudo, na ordem:
1. cria a `.venv` e instala o pacote `lab` (`make venv`);
2. cria o `.env` a partir do `.env.example`, gera os segredos e o UID/GID, e **confere se as faixas de rede colidem**
   com redes já existentes no host (`make env`);
3. gera a configuração dos 3 nós em modo `consistent`, sobe os contêineres e roda `scripts/iniciar.sh` (conecta os
   nós, aplica o layout, cria o bucket `sdlab` e a chave `sd-key`, grava as credenciais no `.env`).

Termina com `cluster pronto: layout versão 1, bucket sdlab, chave sd-key`. Para usar o `labctl` sem digitar o
caminho, ative a venv (ou use `.venv/bin/labctl`):

```bash
source .venv/bin/activate
```

```bash
labctl status
```

O cluster está pronto quando os 3 nós aparecem `running`/`healthy`, `/health` 200, `interna` = sim, e a coluna
**vê UP** mostra `g1,g2,g3` em todos (malha completa).

### Faixas de rede (`.env`)

O `.env.example` usa as faixas do roteiro: `REDE_INTERNA_PREFIXO=172.30.0` e `REDE_CLIENTES_PREFIXO=172.31.0`.
Se alguma delas já estiver em uso no seu host (outra rede Docker, VPN, Wi-Fi), o `make up` para com:

```
ERRO: REDE_INTERNA_PREFIXO (172.30.0.0/24) colide com rede Docker <nome> (172.30.0.0/16)
Ajuste o prefixo no .env e rode de novo.
```

Edite o `.env` (criado nessa primeira tentativa), troque o prefixo por uma faixa livre — por exemplo
`REDE_INTERNA_PREFIXO=10.30.0` — e rode `make up` de novo. Só os três primeiros números mudam; os finais
(.21, .22, .23, .100) são fixos. Na máquina da equipe a rede interna usa `10.30.0.0/24` por esse motivo. A faixa
efetiva de cada execução fica registrada no `manifest.json`.

> Se o cluster já subiu com uma faixa, para trocá-la rode `make reset` antes (a rede Docker é criada com a faixa
> antiga e o `iniciar.sh` guarda os endereços dos nós).

### Comandos do dia a dia

| Comando | Faz |
|---|---|
| `labctl status` | estado dos nós, rede, saúde, quem vê quem, memória, modo (N/R/W) e versão do layout |
| `labctl mode set degraded` | troca o modo: gera a configuração, reinicia os 3 nós em paralelo e espera a malha completa (~15–25 s) |
| `labctl net partition 3` / `labctl net heal 3` | isola / reconecta o garage3 da rede interna (com o IP fixo) |
| `labctl net state` | quais nós estão na rede interna |
| `labctl net heal --all` | **restaura tudo**: cura partições, religa nós parados e volta para `consistent` |
| `labctl node stop 2` / `labctl node start 2` | para / liga o garage2 |
| `labctl s3 put 1 teste ola` | PUT da chave `teste` com o valor `ola` entrando pelo garage1 |
| `labctl s3 get 3 teste` · `labctl s3 ls 2` · `labctl s3 del 1 teste` | GET / listagem / DELETE pelo nó indicado |
| `labctl evidence snapshot antes-da-demo` | retrato completo do cluster em `results/snapshots/` |
| `labctl --help`, `labctl <comando> --help` | ajuda de cada comando |

Toda ação que muda o cluster é registrada em `results/_journal.jsonl`. Os scripts do roteiro também existem e
chamam o `labctl`: `scripts/modo.sh degraded`, `scripts/particionar.sh garage3`, `scripts/curar.sh garage3`.
O cliente do roteiro roda dentro do contêiner `cliente`:

```bash
docker exec cliente python /s3.py 2 put teste ola
```

```bash
docker exec cliente python /s3.py 3 get teste
```

```bash
docker exec cliente python /s3.py 2 bench 20
```

### Painel da demo

```bash
make painel
```

Abre em **http://127.0.0.1:8080** (só nesta máquina; `labctl painel --port 8090` usa outra porta; Ctrl+C para sair).
Funciona sem internet. Mostra:

- **cartões dos 3 nós:** contêiner, `/health`, se está na rede interna, quem ele vê como ativo, memória, disco de
  `data/` e itens da tabela de objetos;
- **banner do modo:** N, R, W e se R + W > N;
- **controles:** particionar/curar o garage3, parar/ligar cada nó, trocar o modo (pede confirmação no próprio
  botão), carga (presets A1, A2, A4), `repair tables`/`repair blocks` e "Restaurar tudo";
- **ao vivo:** linha do tempo dos últimos 60 s, uma faixa por papel (verde = atualizada/ok, laranja =
  desatualizada, vermelho = erro), com as ações marcadas, e contadores de disponibilidade, % de desatualizadas e
  versão lida × confirmada;
- **eventos:** ações registradas (journal) e o último minuto de log de cada nó.

Uma ação por vez: enquanto uma troca de modo (~20 s) ou outra ação roda, os botões ficam desabilitados.

**Roteiro da demo da Ênfase A (testado só pelo navegador):**
1. Modo `degraded` → **Trocar modo** (clique duas vezes para confirmar) → espere a ação terminar (~20 s).
2. Carga **A1** → **Iniciar**: as faixas ficam verdes.
3. **Particionar g3:** a faixa "g3 leitura (minoria)" fica **laranja** na hora (responde, mas com a versão de antes
   da partição; compare "versão" lida × confirmada). Em ~75 s os cartões mostram a detecção (g3 vê só a si mesmo).
4. **Curar g3:** ~20 s depois, a faixa do g3 volta a verde. Os "objetos" do g3 ficam abaixo dos outros (chaves
   escritas na partição); **repair tables** iguala as contagens (E7).
5. Modo `consistent` → particionar de novo: a faixa do g3 fica **vermelha** (sem quórum: não responde). Até a
   detecção, a leitura da maioria (g2) também pode falhar; depois volta a verde.
6. **Curar g3**, **Parar** a carga e **Restaurar tudo**.

Dica: faça partições de mais de ~80 s. Mais curtas que isso, o Garage ainda não fechou a conexão e o nó que volta
demora a receber escritas (ver "Problemas comuns").

### Rodar os experimentos

```bash
labctl run E1
```

Cada experimento confere a pré-condição (3 nós no ar, na rede interna, malha completa, modo `consistent`), roda as
repetições, **restaura o cluster no final mesmo se der erro** e imprime o placar das previsões e o caminho do
`summary.md`. Opções: `--quick` (versão curta, 1 repetição, para testar; marcada no manifest) e `--reps N`.

| Experimento | Duração da execução completa (medida) |
|---|---|
| `labctl run E1` | ~1 min |
| `labctl run E2` | ~30 s (`LAB_DEBUG=1 labctl run E2` recria os nós com log de depuração e volta ao normal no fim) |
| `labctl run E3` | ~5 min |
| `labctl run E4` | ~7 min |
| `labctl run E5` | ~25 min |
| `labctl run E6` | ~14 min |
| `labctl run E7` | **~1 h 53 min** (espera a convergência natural, até 25 min por ciclo) |
| `labctl run E8` | ~3 min (sobe a versão do layout em 2 a cada execução; é esperado) |
| `labctl run E9` | ~50 min (3 modos × 5 repetições; `--mode degraded` roda só um modo) |

Para execuções longas, deixe a máquina sem outras cargas pesadas (as latências entram nos resultados).

### Testes

```bash
make test
```

Testes unitários, sem Docker (~5 s): oráculo, estatísticas (comparadas com numpy/scipy), configuração, carga com
S3 falso.

```bash
make test-integration
```

Com o cluster no ar (~1–2 min): PUT/GET pelos 3 nós, troca de modo sem perder dados, nó isolado acessível pela
Admin API e uma carga curta em `consistent` com 0 leituras desatualizadas.

### Parar e recomeçar

| Comando | Efeito |
|---|---|
| `make down` | para e remove os contêineres; **mantém** `dados/` (o próximo `make up` retoma o mesmo cluster) |
| `make reset` | derruba tudo e **apaga** `dados/` e `generated/` (mantém o `.env`); depois, `make up` sobe do zero |

### Problemas comuns

| Sintoma | Causa e solução |
|---|---|
| `make up` falha com "colide com rede Docker …" | faixa de rede em uso no host → troque o prefixo no `.env` ([acima](#faixas-de-rede-env)) |
| `permission denied` no Docker | usuário fora do grupo `docker` → `sudo usermod -aG docker $USER` e entre de novo na sessão |
| Após `mode set` ou restart, um nó vê só a si mesmo, ou escritas não chegam a uma réplica | `/health` 200 não garante a malha completa. O `labctl mode set` já espera todos verem todos; se reiniciou à mão, confira a coluna **vê UP** do `labctl status` e espere (o Garage tenta reconectar a cada 30 s) |
| Logo após particionar, o nó isolado dá *timeout* em vez de "Could not reach quorum" | esperado: o Garage leva ~80 s para detectar a partição (medido no E6); depois disso a resposta passa a ser "Could not reach quorum of 2" |
| Depois de uma partição **curta** (menos de ~80 s), o nó que voltou continua sem receber escritas por algum tempo | observado no E9 `--quick` (40 s de partição): o Garage ainda não tinha fechado a conexão, e as escritas seguintes não chegaram ao garage3 por mais de 10 s após curar. Para a demo, use partições acima de ~80 s ou espere a malha e alguns segundos antes de mostrar a convergência |
| garage3 `unhealthy` durante a partição | esperado: o healthcheck usa a CLI, que não alcança o próprio nó isolado. Não afeta nada |
| Experimento interrompido (Ctrl+C, erro) ou "pré-condição: …" | `labctl net heal --all` e depois `labctl status` |
| `labctl status` mostra "layout: versão 0" / alerta de layout | o `iniciar.sh` não terminou → rode `make up` de novo (é idempotente) |
| Versão do layout maior que 1 | normal depois do E8 (versões de layout só sobem) |
| Qualquer estado estranho | `make reset` seguido de `make up` (apaga os dados do cluster, não os `results/`) |
