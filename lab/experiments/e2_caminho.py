"""E2 — Caminho de uma operação (roteiro §4.2 e §7.3).

Evidência principal: os contadores `rpc_request_counter{from, to, rpc_endpoint}` de cada nó, lidos antes e
depois de UMA operação. A diferença mostra para quais nós o gateway mandou RPC da tabela de objetos e de blocos.
Complementos: log INFO do servidor S3 (só no nó de entrada), matriz gateway (PUT/GET por cada nó) e o
mapeamento real bucket → partição → nós.

Com LAB_DEBUG=1, os nós são recriados com RUST_LOG=garage=debug durante o experimento (tentativa extra; o
código não tem log por RPC em table.rs/rpc_helper.rs) e voltam ao padrão no teardown.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import time
from collections import defaultdict
from datetime import UTC, datetime

from lab import cluster, garage
from lab import s3 as lab_s3
from lab.config import NOS, RAIZ
from lab.experiments.base import Experiment
from lab.report import tabela_md

TABELA_OBJETOS = "garage_table/table.rs/Rpc:object"
BLOCOS = "garage_block/manager.rs/Rpc"
PEQUENO = 100  # bytes: < INLINE_THRESHOLD (3072), fica dentro dos metadados
GRANDE = 2 * 2**20  # 2 blocos de 1 MiB

SEQUENCIA_PUT = """sequenceDiagram
  participant C as cliente
  participant G1 as garage1 (gateway)
  participant G2 as garage2
  participant G3 as garage3
  C->>G1: PUT /sdlab/chave
  par RPC de escrita da tabela de objetos (medido: 3 destinos)
    G1->>G1: insert
    G1->>G2: insert
    G1->>G3: insert
  end
  Note over G1,G3: as métricas mostram os destinos, não a ordem das respostas
  G1-->>G1: ok (réplica local)
  G2-->>G1: ok (ou G3: a 1ª que responder)
  Note over G1: quórum W atingido (2 de 3 em consistent) → responde
  G1->>C: 200 OK
  G3-->>G1: ok (a outra réplica, em segundo plano)"""


def _particao_do_hash(h: bytes) -> int:
    return h[0]  # 8 bits altos do hash (version.rs l. 102–105, PARTITION_BITS = 8)


def _hash_bloco(dados: bytes) -> bytes:
    return hashlib.blake2b(dados, digest_size=64).digest()[:32]  # Blake2b-512 truncado (data.rs l. 130–136)


class E2(Experiment):
    nome = "E2"
    descricao = "Caminho de uma operação"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.debug = os.environ.get("LAB_DEBUG") == "1"
        self.id_para_nome: dict[str, str] = {}

    # ------------------------------------------------------------ preparação (LAB_DEBUG opcional)

    def _recriar_nos(self, rust_log: str | None) -> None:
        env = {**os.environ}
        if rust_log:
            env["RUST_LOG"] = rust_log
        else:
            env.pop("RUST_LOG", None)
        cluster.eco(f"RUST_LOG={rust_log or 'garage=info'} docker compose up -d garage1 garage2 garage3")
        subprocess.run(
            ["docker", "compose", "up", "-d", "garage1", "garage2", "garage3"],
            cwd=RAIZ,
            env=env,
            check=True,
            capture_output=True,
        )
        garage.esperar_saude(NOS, limite_s=90)

    def precondition(self) -> None:
        super().precondition()
        st = self.api.cluster_status(1)
        self.id_para_nome = {x["id"][:16]: x["hostname"] for x in st["nodes"]}
        if self.debug:
            self._recriar_nos("garage=debug")

    def teardown(self) -> dict:
        if self.debug:
            self._recriar_nos(None)
        return super().teardown()

    # ------------------------------------------------------------ medição

    def _contadores(self) -> tuple[dict[tuple[str, str, str], float], dict[str, tuple[float, float]]]:
        """RPCs emitidos (rpc_request_counter) e bytes de bloco lidos/escritos em disco por nó."""
        rpcs, discos = {}, {}
        for n in NOS:
            texto = self.api.metrics(n)
            rpcs |= garage.contadores_rpc(texto)
            discos[f"garage{n}"] = (
                garage.metrica(texto, "block_bytes_read") or 0.0,
                garage.metrica(texto, "block_bytes_written") or 0.0,
            )
        return rpcs, discos

    def _medir(self, operacao) -> tuple[lab_s3.Resultado, dict, dict]:
        """Executa UMA operação e devolve os RPCs emitidos ({endpoint: {(de, para): delta}}) e os bytes de bloco
        lidos/escritos por nó. A leitura de bloco não passa por RpcHelper::call e não conta em rpc_request_counter
        (rpc_helper.rs l. 184; manager.rs l. 296–300): para ela vale block_bytes_read (monitoring.md l. 219)."""
        antes, disco_antes = self._contadores()
        res = operacao()
        time.sleep(1.0)  # deixa terminar a parte da escrita que segue em segundo plano
        depois, disco_depois = self._contadores()
        disco = {
            no: {
                "lidos": disco_depois[no][0] - disco_antes[no][0],
                "escritos": disco_depois[no][1] - disco_antes[no][1],
            }
            for no in disco_depois
        }
        rpcs: dict[str, dict[tuple[str, str], float]] = defaultdict(dict)
        for (de, para, endpoint), valor in depois.items():
            delta = valor - antes.get((de, para, endpoint), 0.0)
            if delta > 0 and (endpoint == TABELA_OBJETOS or endpoint.startswith(BLOCOS) or "table.rs" in endpoint):
                rpcs[endpoint][(self.id_para_nome.get(de, de), self.id_para_nome.get(para, para))] = delta
        return res, dict(rpcs), disco

    def run_rep(self, i: int) -> dict:
        clientes = {n: lab_s3.cliente_s3(n) for n in NOS}
        prefixo = f"e2/{self.inicio.strftime('%H%M%S')}-r{i}"
        pequeno = os.urandom(PEQUENO)
        grande = os.urandom(GRANDE)
        inicio_logs = datetime.now(UTC)

        ops = {}
        for nome, no, func in (
            ("PUT pequeno via garage1", 1, lambda: lab_s3.put(clientes[1], 1, f"{prefixo}/pequeno", pequeno)),
            ("GET pequeno via garage3", 3, lambda: lab_s3.get(clientes[3], 3, f"{prefixo}/pequeno")),
            ("PUT 2 MiB via garage1", 1, lambda: lab_s3.put(clientes[1], 1, f"{prefixo}/grande", grande)),
            ("GET 2 MiB via garage3", 3, lambda: lab_s3.get(clientes[3], 3, f"{prefixo}/grande")),
        ):
            res, rpcs, disco = self._medir(func)
            ops[nome] = {
                "no_entrada": no,
                "ok": res.ok,
                "erro": res.erro,
                "latencia_ms": round(res.latencia_ms, 1),
                "rpcs": {ep: [[de, para, d] for (de, para), d in v.items()] for ep, v in rpcs.items()},
                "disco": disco,
            }
        integro = ops["GET 2 MiB via garage3"]["ok"]
        if integro:
            integro = lab_s3.get(clientes[3], 3, f"{prefixo}/grande").dados == grande

        # Matriz gateway: PUT por cada nó, GET por cada nó
        matriz = {}
        for w in NOS:
            chave = f"{prefixo}/via-{w}"
            ok_put = lab_s3.put(clientes[w], w, chave, f"v{w}".encode()).ok
            for r in NOS:
                res = lab_s3.get(clientes[r], r, chave)
                matriz[f"{w}->{r}"] = ok_put and res.ok and res.dados == f"v{w}".encode()

        # Log INFO de requisição S3 por nó (só o nó de entrada deve registrar)
        logs_s3, contagem_logs = {}, {}
        for n in NOS:
            texto = cluster.logs(f"garage{n}", desde=inicio_logs)
            linhas = [x for x in texto.splitlines() if "generic_server" in x and f"/sdlab/{prefixo}" in x]
            logs_s3[f"garage{n}"] = linhas
            contagem_logs[f"garage{n}"] = {
                "medidas": sum(1 for x in linhas if f"{prefixo}/pequeno" in x or f"{prefixo}/grande" in x),
                "matriz": sum(1 for x in linhas if f"{prefixo}/via-" in x),
            }
        self.evidencia(
            f"rep{i}_logs_s3.txt",
            "docker logs garageN (linhas INFO do servidor S3 desta repetição)",
            lambda: "\n".join(f"== {k} ==\n" + "\n".join(v) for k, v in logs_s3.items()),
        )
        if self.debug:
            for n in NOS:
                self.evidencia(
                    f"rep{i}_logs_debug_garage{n}.txt",
                    f"docker logs garage{n} (RUST_LOG=garage=debug)",
                    lambda n=n: cluster.logs(f"garage{n}", desde=inicio_logs),
                )
        self.evidencia(
            f"rep{i}_rpcs.txt", "diferença de rpc_request_counter antes/depois de cada operação", lambda: ops
        )

        blocos = [grande[k : k + 2**20] for k in range(0, GRANDE, 2**20)]
        return {
            "ops": ops,
            "integro": integro,
            "matriz": matriz,
            "logs_s3": contagem_logs,
            "particoes_blocos": [_particao_do_hash(_hash_bloco(b)) for b in blocos],
        }

    # ------------------------------------------------------------ análise

    def analyze(self) -> dict:
        reps = [r["resultado"] for r in self.repeticoes if r.get("situacao") == "ok"]
        if not reps:
            return {"resultados": []}
        info_bucket = self.api._json("GET", 1, "GetBucketInfo", params={"globalAlias": "sdlab"})
        id_bucket = info_bucket["id"]
        particao = int(id_bucket[:2], 16)
        self.evidencia("bucket_info.txt", "GET /v2/GetBucketInfo?globalAlias=sdlab", lambda: info_bucket)
        todos = {f"garage{n}" for n in NOS}

        def destinos(rep, op, filtro) -> list[set[str]]:
            return [{para for de, para, _ in v} for ep, v in rep["ops"][op]["rpcs"].items() if filtro(ep)]

        obj = lambda ep: ep == TABELA_OBJETOS
        blk = lambda ep: ep.startswith(BLOCOS)
        p1 = all(all(r["matriz"].values()) for r in reps)
        # operações medidas entram por garage1 (PUT) e garage3 (GET): garage2 não deve registrar nenhuma
        logs_ok = all(
            r["logs_s3"]["garage2"]["medidas"] == 0
            and r["logs_s3"]["garage1"]["medidas"] >= 2
            and r["logs_s3"]["garage3"]["medidas"] >= 2
            for r in reps
        )
        p2 = all(destinos(r, "PUT pequeno via garage1", obj) == [todos] for r in reps)
        p3_dest = [destinos(r, "GET pequeno via garage3", obj) for r in reps]
        p3 = all(len(d) == 1 and len(d[0]) == 2 and "garage3" in d[0] for d in p3_dest)
        minimo = 0.99 * GRANDE
        p4_put = all(
            any(s == todos for s in destinos(r, "PUT 2 MiB via garage1", blk))
            and all(v["escritos"] >= minimo for v in r["ops"]["PUT 2 MiB via garage1"]["disco"].values())
            for r in reps
        )
        leitores = [
            [no for no, v in r["ops"]["GET 2 MiB via garage3"]["disco"].items() if v["lidos"] >= minimo] for r in reps
        ]
        p4 = p4_put and all(len(x) == 1 for x in leitores) and all(r["integro"] for r in reps)

        linhas_csv = []
        for k, r in enumerate(reps):
            for op, dados in r["ops"].items():
                for ep, v in dados["rpcs"].items():
                    for de, para, d in v:
                        linhas_csv.append(
                            {
                                "rep": k,
                                "operacao": op,
                                "endpoint": ep,
                                "de": de,
                                "para": para,
                                "rpcs": d,
                                "latencia_ms": dados["latencia_ms"],
                            }
                        )
        self.gravar_csv("metrics.csv", linhas_csv)

        exemplo = reps[0]["ops"]
        linhas_tab = []
        for op, dados in exemplo.items():
            for ep, v in sorted(dados["rpcs"].items()):
                linhas_tab.append(
                    [
                        op,
                        ep.split("/")[-1] if "table" in ep else ep,
                        ", ".join(f"{de}→{para} ×{d:g}" for de, para, d in sorted(v)),
                    ]
                )
        disco_tab = []
        for op, dados in exemplo.items():
            disco_tab.append(
                [op]
                + [
                    f"{dados['disco'][f'garage{n}']['lidos'] / 2**20:.2f} / "
                    f"{dados['disco'][f'garage{n}']['escritos'] / 2**20:.2f}"
                    for n in NOS
                ]
            )
        sequencia = SEQUENCIA_PUT
        resultados = [
            {
                "id": "P1",
                "situacao": "confirmada" if p1 and logs_ok else "refutada",
                "resultado": f"matriz gateway 9/9 ok em {sum(all(r['matriz'].values()) for r in reps)}/{len(reps)} reps; "
                f"linhas INFO do S3 das operações medidas (PUT via g1, GET via g3), última rep: "
                f"{ {k: v['medidas'] for k, v in reps[-1]['logs_s3'].items()} }",
            },
            {
                "id": "P2",
                "situacao": "confirmada" if p2 else "refutada",
                "resultado": "destinos de Rpc:object no PUT pequeno: "
                + "; ".join(str(sorted(set().union(*destinos(r, "PUT pequeno via garage1", obj)))) for r in reps),
            },
            {
                "id": "P3",
                "situacao": "confirmada" if p3 else "refutada",
                "resultado": "destinos de Rpc:object no GET pequeno via garage3: "
                + "; ".join(str([sorted(s) for s in d]) for d in p3_dest),
            },
            {
                "id": "P4",
                "situacao": "confirmada" if p4 else "refutada",
                "resultado": "PUT 2 MiB: RPC de bloco para os 3 nós e ≥ 2 MiB escritos em disco em cada nó: "
                f"{'sim' if p4_put else 'não'}; GET via garage3: nós que leram ≥ 2 MiB do disco por rep "
                f"(block_bytes_read): {leitores}; conteúdo íntegro: {all(r['integro'] for r in reps)}",
            },
            {
                "id": "P5",
                "situacao": "confirmada",
                "resultado": f"ID do bucket {id_bucket[:16]}… → partição 0x{id_bucket[:2]} = {particao} (1º byte); "
                f"partições dos 2 blocos da 1ª rep: {reps[0]['particoes_blocos']}; os 3 nós guardam as 256 "
                "partições (E1, coluna Part.)",
            },
        ]
        return {
            "resultados": resultados,
            "secoes": [
                (
                    "RPCs emitidos por operação (1ª repetição)",
                    tabela_md(["operação", "endpoint", "de→para ×RPCs"], linhas_tab)
                    + "\n\nLeitura da tabela: no PUT, o gateway primeiro **lê** o objeto existente para calcular o "
                    "carimbo (put.rs l. 156, 711–717), com R pedidos (rpc_helper.rs l. 337–397), e depois **escreve** nas "
                    "3 réplicas (rpc_helper.rs l. 413–431). Por isso `Rpc:object` no PUT pequeno = 1 leitura para 2 nós + 1 escrita para 3 nós. "
                    "`Rpc:bucket_object_counter` são os contadores do bucket (atualizados por todos os nós). A leitura "
                    "de **blocos** não aparece aqui (não passa por RpcHelper::call): ver a tabela de disco.",
                ),
                (
                    "Bytes de bloco lidos / escritos em disco por nó, MiB (1ª repetição)",
                    tabela_md(["operação", "garage1", "garage2", "garage3"], disco_tab),
                ),
                (
                    "Diagrama de sequência do PUT (destinos medidos; momento do quórum pelo código)",
                    f"```mermaid\n{sequencia}\n```",
                ),
                (
                    "Mapeamento real",
                    f"Metadados: chave de partição = ID do bucket `{id_bucket}` → hash = o próprio ID (schema.rs l. 25–32) "
                    f"→ partição = 1º byte = **{particao}** (version.rs l. 102–105) → nós do layout dessa partição = "
                    "garage1, garage2 e garage3 (N = 3, 3 nós). O nome do objeto é só a chave de ordenação (object_table.rs "
                    "l. 696–700): **todos** os objetos do bucket compartilham essa partição de metadados. Dados: cada bloco "
                    "vai para a partição do 1º byte do seu hash Blake2 (put.rs l. 460)."
                    + (" LAB_DEBUG=1: logs debug salvos em evidence/." if self.debug else ""),
                ),
            ],
        }
