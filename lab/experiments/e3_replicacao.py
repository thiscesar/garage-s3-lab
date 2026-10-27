"""E3 — Replicação (roteiro §4.2 e §7.3).

Por repetição:
1. objeto de 8 MiB (bytes aleatórios) via garage1: `du -sb dados/g*/data` antes/depois e SHA-256 lido por cada nó;
2. latência de 20 PUT + 20 GET de 4 KiB por nó de entrada (modo consistent);
3. tempo de propagação nos 3 modos: PUT via garage1 e, logo após a confirmação, leituras em laço via garage2 e
   garage3 (em paralelo, a cada 5 ms) até ver o valor novo. Se a 1ª leitura já é o valor novo, a propagação é 0.
Na repetição 0 também roda o comando do roteiro: `docker exec cliente python /s3.py 2 bench 20`.

Limites da propagação: o intervalo de 5 ms não é o que limita a medição, porque a 1ª leitura
sai logo depois da confirmação. Limitam (a) o tamanho da amostra: 15 tentativas por nó e modo; com as taxas do E4
(0,5% a 2,7% por leitura), a chance de ver 0 casos em 15 vai de 93% a 66%; e (b) o atraso da 1ª leitura, registrado
em `atraso_1a_leitura_ms`. A janela de leitura desatualizada é medida com carga contínua no E4 e no E9.

Por que o PUT de 4 KiB é mais lento que o GET (código Garage v2.3.0, commit 7b119c0): 4 KiB está acima de
`INLINE_THRESHOLD = 3072` (src/block/manager.rs l. 44), então o PUT não é só "ler e escrever o objeto" (como abaixo
desse limite, src/api/s3/put.rs l. 172–217): ele lê o objeto existente (put.rs l. 150), grava o objeto "Uploading"
(l. 249), a versão (l. 263), o bloco + versão + referência em paralelo (l. 588–592) e o objeto "Complete" (l. 313),
cada etapa esperando o quórum de escrita, em sequência. Hipótese não verificada (exigiria captura de pacotes): a
cauda de ~45 ms que aparece só com RPC entre nós no caminho pode vir da interação do algoritmo de Nagle com o ACK
atrasado do TCP, já que não há `set_nodelay` em src/.
"""

from __future__ import annotations

import hashlib
import os
import threading
import time
import uuid

from lab import cluster, garage, plots, stats
from lab import s3 as lab_s3
from lab.config import MODOS, NOS
from lab.experiments.base import Experiment
from lab.report import tabela_md

OBJETO_GRANDE = 8 * 2**20
TAM_LATENCIA = 4096
SONDAGEM_S = 0.005
LIMITE_PROPAGACAO_S = 5.0


class E3(Experiment):
    nome = "E3"
    descricao = "Replicação"

    def _replicacao(self, i: int, clientes: dict) -> dict:
        dados = os.urandom(OBJETO_GRANDE)
        sha = hashlib.sha256(dados).hexdigest()
        antes = cluster.uso_disco_dados()
        res = lab_s3.put(clientes[1], 1, f"e3/grande/{uuid.uuid4().hex[:8]}", dados)
        time.sleep(2.0)  # a 3ª réplica recebe em segundo plano
        depois = cluster.uso_disco_dados()
        lidos = {}
        for n in NOS:
            g = lab_s3.get(clientes[n], n, res.chave)
            lidos[f"garage{n}"] = hashlib.sha256(g.dados).hexdigest() if g.ok else f"erro: {g.erro}"
        self.evidencia(
            f"rep{i}_du.txt",
            "du -sb dados/g{1,2,3}/data (antes e depois do PUT de 8 MiB)",
            lambda: {"antes": antes, "depois": depois},
        )
        return {
            "put_ok": res.ok,
            "sha_enviado": sha,
            "sha_lido": lidos,
            "crescimento": {k: depois[k] - antes[k] for k in depois},
        }

    def _latencia(self, clientes: dict, n_ops: int) -> dict:
        corpo = os.urandom(TAM_LATENCIA)
        saida = {}
        for n in NOS:
            prefixo = f"e3/lat/{uuid.uuid4().hex[:8]}"
            puts = [lab_s3.put(clientes[n], n, f"{prefixo}/{k}", corpo) for k in range(n_ops)]
            gets = [lab_s3.get(clientes[n], n, f"{prefixo}/{k}") for k in range(n_ops)]
            saida[f"garage{n}"] = {
                "put_ms": [r.latencia_ms for r in puts if r.ok],
                "get_ms": [r.latencia_ms for r in gets if r.ok],
                "erros": sum(not r.ok for r in puts + gets),
            }
        return saida

    @staticmethod
    def _sondar(c, n: int, chave: str, valor: bytes, t_ack: float, por_no: dict) -> None:
        """Lê `chave` via garage`n` (cliente `c`, criado antes do PUT) a cada 5 ms até ver `valor`; propagação = 0 se
        a 1ª leitura já o vê. Registra também o atraso da 1ª leitura em relação à confirmação."""
        leituras_antigas, atraso_1a = 0, None
        while True:
            g = lab_s3.get(c, n, chave)
            if atraso_1a is None:
                atraso_1a = (g.t_inicio - t_ack) * 1000
            if g.ok and g.dados == valor:
                primeira = leituras_antigas == 0
                por_no[f"garage{n}"] = {
                    "propagacao_ms": 0.0 if primeira else (g.t_inicio - t_ack) * 1000,
                    "leituras_antigas": leituras_antigas,
                    "atraso_1a_leitura_ms": atraso_1a,
                }
                return
            leituras_antigas += 1
            if time.monotonic() - t_ack > LIMITE_PROPAGACAO_S:
                por_no[f"garage{n}"] = {
                    "propagacao_ms": None,
                    "leituras_antigas": leituras_antigas,
                    "atraso_1a_leitura_ms": atraso_1a,
                }
                return
            time.sleep(SONDAGEM_S)

    def _propagacao(self, clientes: dict, tentativas: int) -> list[dict]:
        resultados = []
        for _ in range(tentativas):
            chave = f"e3/prop/{uuid.uuid4().hex[:8]}"
            lab_s3.put(clientes[1], 1, chave, b"v0")
            time.sleep(0.5)
            valor = uuid.uuid4().hex.encode()
            put = lab_s3.put(clientes[1], 1, chave, valor)
            if not put.ok:
                resultados.append({"put_ok": False, "erro": put.erro})
                continue
            por_no: dict[str, dict] = {}
            threads = [
                threading.Thread(target=self._sondar, args=(clientes[n], n, chave, valor, put.t_fim, por_no))
                for n in (2, 3)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            resultados.append({"put_ok": True, **por_no})
        return resultados

    def run_rep(self, i: int) -> dict:
        clientes = {n: lab_s3.cliente_s3(n) for n in NOS}
        n_ops = 5 if self.quick else 20
        tentativas = 1 if self.quick else 3
        if i == 0:
            self.evidencia(
                "roteiro_s3py_bench.txt",
                "docker exec cliente python /s3.py 2 bench 20",
                lambda: cluster.executar_servico("cliente", ["python", "/s3.py", "2", "bench", "20"]),
            )
        resultado = {
            "replicacao": self._replicacao(i, clientes),
            "latencia": self._latencia(clientes, n_ops),
            "propagacao": {},
        }
        for modo in MODOS:
            if modo != "consistent":
                garage.definir_modo(modo)
            resultado["propagacao"][modo] = self._propagacao({n: lab_s3.cliente_s3(n) for n in NOS}, tentativas)
        garage.definir_modo("consistent")
        return resultado

    # ------------------------------------------------------------ análise

    def analyze(self) -> dict:
        reps = [r["resultado"] for r in self.repeticoes if r.get("situacao") == "ok"]
        if not reps:
            return {"resultados": []}
        nomes = [f"garage{n}" for n in NOS]

        # 1. Replicação
        cresc = {no: [r["replicacao"]["crescimento"][no] for r in reps] for no in nomes}
        sha_ok = all(set(r["replicacao"]["sha_lido"].values()) == {r["replicacao"]["sha_enviado"]} for r in reps)
        cresc_ok = all(0.98 * OBJETO_GRANDE <= v <= 1.05 * OBJETO_GRANDE for vs in cresc.values() for v in vs)

        # 2. Latência: média por repetição, depois estatística entre repetições (R7)
        linhas_csv, lat = [], {}
        for no in nomes:
            for op in ("put_ms", "get_ms"):
                medias = [stats.resumir(r["latencia"][no][op]).media for r in reps if r["latencia"][no][op]]
                todas = [v for r in reps for v in r["latencia"][no][op]]
                lat[(no, op)] = (stats.resumir(medias), stats.resumir(todas))
                for k, r in enumerate(reps):
                    for v in r["latencia"][no][op]:
                        linhas_csv.append({"secao": "latencia", "rep": k, "no": no, "op": op[:3].upper(), "ms": v})
        p3 = all(lat[(no, "put_ms")][0].media > lat[(no, "get_ms")][0].media for no in nomes)
        medias_put = [lat[(no, "put_ms")][0].media for no in nomes]
        equivalentes = max(medias_put) <= 2 * min(medias_put)

        # 3. Propagação
        prop: dict[tuple[str, str], list[float | None]] = {}
        for modo in MODOS:
            for no in ("garage2", "garage3"):
                valores = [t[no]["propagacao_ms"] for r in reps for t in r["propagacao"][modo] if t.get("put_ok")]
                prop[(modo, no)] = valores
                for v in valores:
                    linhas_csv.append({"secao": "propagacao", "modo": modo, "no": no, "ms": v})
        self.gravar_csv("metrics.csv", linhas_csv)

        def positivos(modo):
            return [v for no in ("garage2", "garage3") for v in prop[(modo, no)] if v is None or v > 0]

        def total(modo):
            return sum(len(prop[(modo, no)]) for no in ("garage2", "garage3"))

        p4 = len(positivos("consistent")) == 0 and all(
            len(positivos(m)) <= total(m) // 2 for m in ("degraded", "dangerous")
        )

        # Figuras
        series = {
            "PUT": (
                [lat[(no, "put_ms")][0].media for no in nomes],
                [
                    0 if lat[(no, "put_ms")][0].n < 2 else lat[(no, "put_ms")][0].ic95[1] - lat[(no, "put_ms")][0].media
                    for no in nomes
                ],
            ),
            "GET": (
                [lat[(no, "get_ms")][0].media for no in nomes],
                [
                    0 if lat[(no, "get_ms")][0].n < 2 else lat[(no, "get_ms")][0].ic95[1] - lat[(no, "get_ms")][0].media
                    for no in nomes
                ],
            ),
        }
        plots.salvar(
            plots.barras_agrupadas(
                nomes,
                series,
                "Latência de 4 KiB por nó de entrada (média entre reps ± IC95)",
                "ms",
                {"PUT": "#2b6cb0", "GET": "#2f855a"},
            ),
            self.dir_figuras,
            "latencia_por_no",
        )
        fracs = [len(positivos(m)) / max(1, total(m)) * 100 for m in MODOS]
        plots.salvar(
            plots.barras_com_erro(
                list(MODOS),
                fracs,
                [0] * 3,
                "Tentativas com propagação > 0 (garage2 e garage3)",
                "% das leituras",
                [plots.CORES_MODO[m] for m in MODOS],
            ),
            self.dir_figuras,
            "propagacao_por_modo",
        )

        tab_lat = tabela_md(
            ["nó", "op", "média entre reps (ms)", "desvio (ms)", "IC95 (ms)", "p95 de todas (ms)", "n ops"],
            [
                [
                    no,
                    op[:3].upper(),
                    f"{a.media:.2f}",
                    f"{a.desvio:.2f}",
                    f"{a.ic95[0]:.2f}–{a.ic95[1]:.2f}",
                    f"{b.p95:.2f}",
                    b.n,
                ]
                for (no, op), (a, b) in lat.items()
            ],
        )
        tab_prop = tabela_md(
            ["modo", "nó", "tentativas", "= 0", "0 < x ≤ 5 s", "não viu em 5 s", "máx observado (ms)"],
            [
                [
                    m,
                    no,
                    len(v),
                    sum(1 for x in v if x == 0),
                    sum(1 for x in v if x is not None and x > 0),
                    sum(1 for x in v if x is None),
                    f"{max((x for x in v if x is not None), default=0):.1f}",
                ]
                for (m, no), v in prop.items()
            ],
        )
        tab_rep = tabela_md(
            ["nó", "crescimento por rep (MiB)"],
            [[no, ", ".join(f"{v / 2**20:.3f}" for v in vs)] for no, vs in cresc.items()],
        )
        resultados = [
            {
                "id": "P1",
                "situacao": "confirmada" if cresc_ok else "refutada",
                "resultado": "crescimento de cada dados/gN/data por PUT de 8 MiB: "
                + "; ".join(f"{no} {sum(vs) / len(vs) / 2**20:.3f} MiB (média)" for no, vs in cresc.items()),
            },
            {
                "id": "P2",
                "situacao": "confirmada" if sha_ok else "refutada",
                "resultado": f"SHA-256 lido = enviado nos 3 nós em {sum(1 for r in reps if set(r['replicacao']['sha_lido'].values()) == {r['replicacao']['sha_enviado']})}/{len(reps)} reps",
            },
            {
                "id": "P3",
                "situacao": "confirmada" if p3 and equivalentes else "refutada",
                "resultado": "; ".join(
                    f"{no}: PUT {lat[(no, 'put_ms')][0].media:.1f} ms, GET {lat[(no, 'get_ms')][0].media:.1f} ms"
                    for no in nomes
                )
                + f"; PUT equivalentes entre nós (máx ≤ 2× mín): {'sim' if equivalentes else 'não'}",
            },
            {
                "id": "P4",
                "situacao": "confirmada" if p4 else "refutada",
                "resultado": "; ".join(
                    f"{m}: {len(positivos(m))}/{total(m)} leituras com propagação > 0" for m in MODOS
                ),
            },
        ]
        return {
            "resultados": resultados,
            "secoes": [
                ("Replicação (du -sb)", tab_rep),
                ("Latência (consistent)", tab_lat),
                (
                    "Propagação por modo",
                    tab_prop
                    + "\n\nDefinição: tempo da confirmação do PUT (via garage1) até o início da 1ª leitura que "
                    "devolve o valor novo; 0 se a 1ª leitura (iniciada logo após a confirmação) já o devolve. "
                    "Sondagem a cada 5 ms, garage2 e garage3 em paralelo, limite de 5 s. O limite da medição não é o "
                    "intervalo de 5 ms, e sim a amostra (15 tentativas por nó e modo: com taxas de 0,5% a 2,7% por "
                    "leitura, a chance de ver 0 casos em 15 vai de 93% a 66%) e o atraso da 1ª leitura "
                    "(`atraso_1a_leitura_ms`). A janela desatualizada é medida "
                    "com carga contínua no E4 (atraso das leituras desatualizadas) e no E9.",
                ),
                (
                    "Por que o PUT é mais lento que o GET",
                    (
                        "O objeto de 4 KiB está acima de `INLINE_THRESHOLD = 3072` (src/block/manager.rs l. 44), então o PUT "
                        "passa por várias etapas com quórum, em sequência (src/api/s3/put.rs): leitura do objeto existente "
                        '(l. 150), objeto "Uploading" (l. 249), versão (l. 263), bloco + versão + referência em paralelo '
                        '(l. 588–592) e objeto "Complete" (l. 313). O GET em consistent lê o objeto (R = 2) e o bloco. '
                        "Hipótese, não verificada: a cauda de ~45 ms com RPC entre nós pode vir de Nagle × ACK atrasado do "
                        "TCP (não há `set_nodelay` em src/)."
                    ),
                ),
            ],
            "figuras": ["latencia_por_no", "propagacao_por_modo"],
        }
