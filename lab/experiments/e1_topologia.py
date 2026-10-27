"""E1 — Subida e topologia (roteiro §4.2 e §7.3).

Coleta o estado do cluster (CLI e Admin API), IPs e limites de memória dos contêineres, amostra a memória
de cada nó, monta a tabela de nós e um diagrama Mermaid a partir dos dados reais e confronta com a previsão.
"""

from __future__ import annotations

import re
import time

from lab import cluster, garage, plots, stats
from lab.config import NOS, Topologia
from lab.experiments.base import Experiment
from lab.report import tabela_md

MEM_LIMIT = 128 * 2**20
PORTAS = {"S3": 3900, "RPC": 3901, "Admin": 3903}


def _particoes_por_no(freeform: str) -> dict[str, int]:
    """Coluna "Part." do texto de GetClusterStatistics: {hostname: partições}."""
    resultado = {}
    for linha in freeform.splitlines():
        m = re.match(r"\s+[0-9a-f]{16}\s+(\S+)\s+\S+\s+[\d.]+ \S+\s+(\d+)\s", linha)
        if m:
            resultado[m.group(1)] = int(m.group(2))
    return resultado


def _diagrama(nos: list[dict], topo: Topologia) -> str:
    linhas = [
        "flowchart LR",
        f"  subgraph clientes[rede clientes {topo.subrede_clientes}]",
        f"    C[cliente {topo.ip_cliente}]",
        "    H[host: labctl]",
        "  end",
        f"  subgraph interna[rede interna {topo.subrede_interna} - RPC 3901]",
    ]
    for no in nos:
        linhas.append(
            f'    G{no["no"]}["{no["hostname"]} · {no["zona"]} · {no["capacidade"]}<br/>'
            f'interna {no["ip_interno"]} · clientes {no["ip_clientes"]}<br/>S3 3900 · Admin 3903"]'
        )
    linhas.append("  end")
    linhas += [f"  C -- S3 3900 --> G{n}" for n in NOS]
    linhas += [f"  H -- Admin 3903 --> G{n}" for n in NOS]
    linhas += ["  G1 <--> G2", "  G2 <--> G3", "  G1 <--> G3"]
    return "\n".join(linhas)


class E1(Experiment):
    nome = "E1"
    descricao = "Subida e topologia"
    reps_padrao = 1  # coleta de estado; a memória é amostrada várias vezes dentro da repetição

    def run_rep(self, i: int) -> dict:
        amostras = 2 if self.quick else 5
        topo = Topologia.do_env()

        self.evidencia("garage_status.txt", "garage -h <id>@127.0.0.1:3901 status (garage1)", garage.status_texto)
        self.evidencia("layout_show.txt", "garage -h <id>@127.0.0.1:3901 layout show (garage1)", garage.layout_show)
        self.evidencia(
            "layout_history.txt", "garage -h <id>@127.0.0.1:3901 layout history (garage1)", garage.layout_history
        )
        status = {n: self.api.cluster_status(n) for n in NOS}
        saude = self.api.cluster_health(1)
        estatisticas = self.api._json("GET", 1, "GetClusterStatistics")["freeform"]
        for n in NOS:
            self.evidencia(
                f"cluster_status_garage{n}.txt", f"GET /v2/GetClusterStatus (garage{n})", lambda n=n: status[n]
            )
        self.evidencia("cluster_health.txt", "GET /v2/GetClusterHealth (garage1)", lambda: saude)
        self.evidencia("cluster_statistics.txt", "GET /v2/GetClusterStatistics (garage1)", lambda: estatisticas)
        estados = {s: cluster.estado(int(s[-1])) if s.startswith("garage") else None for s in cluster.SERVICOS}
        self.evidencia(
            "docker_inspect.txt",
            "docker inspect (estado, IPs, mem_limit)",
            lambda: (
                {s: cluster.container(s).attrs["HostConfig"]["Memory"] for s in cluster.SERVICOS}
                | {"nos": [cluster.estado(n) for n in NOS]}
            ),
        )

        memoria: dict[str, list[int]] = {s: [] for s in cluster.SERVICOS}
        for k in range(amostras):
            for s in cluster.SERVICOS:
                memoria[s].append(cluster.memoria(s))
            if k < amostras - 1:
                time.sleep(2)
        self.evidencia("docker_stats.txt", f"docker stats ({amostras} amostras, bytes, uso − cache)", lambda: memoria)

        nos = []
        for n in NOS:
            st = next(x for x in status[1]["nodes"] if x["hostname"] == f"garage{n}")
            nos.append(
                {
                    "no": n,
                    "id": st["id"][:16],
                    "hostname": st["hostname"],
                    "zona": (st.get("role") or {}).get("zone"),
                    "capacidade": f"{(st.get('role') or {}).get('capacity', 0) / 1e9:g} GB",
                    "ip_interno": topo.ip_interno(n),
                    "ip_clientes": topo.ip_clientes(n),
                    "mem_limit": estados[f"garage{n}"]["mem_limit"],
                }
            )
        return {
            "nos": nos,
            "visao_up": {n: sorted(x["hostname"] for x in status[n]["nodes"] if x.get("isUp")) for n in NOS},
            "layout_versao": status[1]["layoutVersion"],
            "saude": saude,
            "particoes_por_no": _particoes_por_no(estatisticas),
            "memoria": memoria,
            "papeis": {x["hostname"]: x.get("role") for x in status[1]["nodes"]},
        }

    def analyze(self) -> dict:
        reps_ok = [r["resultado"] for r in self.repeticoes if r.get("situacao") == "ok"]
        if not reps_ok:
            return {"resultados": []}
        d = reps_ok[0]
        topo = Topologia.do_env()
        todos = [f"garage{n}" for n in NOS]
        zonas = sorted(no["zona"] for no in d["nos"])
        capacidades = {no["capacidade"] for no in d["nos"]}

        linhas_csv, resumo_mem = [], {}
        for s, valores in d["memoria"].items():
            for k, v in enumerate(valores):
                linhas_csv.append({"servico": s, "amostra": k, "memoria_bytes": v})
            resumo_mem[s] = stats.resumir([v / 2**20 for v in valores if v is not None])
        self.gravar_csv("metrics.csv", linhas_csv)

        nos_mem = [resumo_mem[f"garage{n}"] for n in NOS]
        fig = plots.barras_com_erro(
            [f"garage{n}" for n in NOS],
            [r.media for r in nos_mem],
            [0 if r.n < 2 else r.desvio for r in nos_mem],
            "Memória por nó (média ± desvio)",
            "MiB",
            [plots.CORES_NO[n] for n in NOS],
        )
        plots.salvar(fig, self.dir_figuras, "memoria_por_no")

        mermaid = _diagrama(d["nos"], topo)
        (self.dir_evidencias / "topologia.mmd").write_text(mermaid + "\n")

        p1 = all(d["visao_up"][n] == todos for n in NOS) and zonas == ["dz1", "dz2", "dz3"] and capacidades == {"1 GB"}
        s = d["saude"]
        p2 = d["layout_versao"] >= 1 and s["partitions"] == 256 and s["partitionsQuorum"] == 256
        p3 = d["particoes_por_no"] == {h: 256 for h in todos}
        mem_max = max(r.maximo for r in nos_mem)
        p4 = mem_max * 2**20 < MEM_LIMIT
        na_faixa = all(5 <= r.media * 1.048576 <= 15 for r in nos_mem)  # MiB → MB
        papeis = d["papeis"]
        p5 = all(p and p.get("zone") and p.get("capacity") for p in papeis.values()) and len(papeis) == 3

        resultados = [
            {
                "id": "P1",
                "situacao": "confirmada" if p1 else "refutada",
                "resultado": f"visão UP: {d['visao_up']}; zonas {zonas}; capacidades {sorted(capacidades)}",
            },
            {
                "id": "P2",
                "situacao": "confirmada" if p2 else "refutada",
                "resultado": f"layout v{d['layout_versao']}; partições {s['partitions']}, com quórum "
                f"{s['partitionsQuorum']}, todas ok {s['partitionsAllOk']}; status {s['status']}",
            },
            {
                "id": "P3",
                "situacao": "confirmada" if p3 else "refutada",
                "resultado": f"partições por nó (GetClusterStatistics, coluna Part.): {d['particoes_por_no']}",
            },
            {
                "id": "P4",
                "situacao": "confirmada" if p4 and na_faixa else ("inconclusiva" if p4 else "refutada"),
                "resultado": "; ".join(
                    f"garage{n}: {r.media:.1f} MiB (máx {r.maximo:.1f})" for n, r in zip(NOS, nos_mem, strict=True)
                )
                + f"; limite 128 MiB; faixa do roteiro {'respeitada' if na_faixa else 'NÃO respeitada'}",
            },
            {
                "id": "P5",
                "situacao": "confirmada" if p5 else "refutada",
                "resultado": "papéis: "
                + "; ".join(
                    f"{h}: zona {p.get('zone')}, {p.get('capacity', 0) / 1e9:g} GB" for h, p in sorted(papeis.items())
                )
                + "; nenhum campo de líder no GetClusterStatus",
            },
        ]
        tabela = tabela_md(
            [
                "nó",
                "ID (16)",
                "zona",
                "capacidade",
                "IP interno",
                "IP clientes",
                "portas",
                "mem_limit",
                "memória média",
            ],
            [
                [
                    no["hostname"],
                    no["id"],
                    no["zona"],
                    no["capacidade"],
                    no["ip_interno"],
                    no["ip_clientes"],
                    " · ".join(f"{k} {v}" for k, v in PORTAS.items()),
                    f"{no['mem_limit'] / 2**20:.0f} MiB",
                    f"{resumo_mem[no['hostname']].media:.1f} MiB",
                ]
                for no in d["nos"]
            ],
        )
        return {
            "resultados": resultados,
            "secoes": [
                ("Tabela de nós (dados reais)", tabela),
                ("Diagrama (gerado dos dados reais)", f"```mermaid\n{mermaid}\n```"),
                (
                    "Memória do cliente",
                    f"cliente: {resumo_mem['cliente'].media:.1f} MiB (média de {resumo_mem['cliente'].n} amostras)",
                ),
            ],
            "figuras": ["memoria_por_no"],
        }
