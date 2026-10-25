"""E6 — Partição de rede (roteiro §4.2 e §7.3).

Modo consistent. Por repetição: carga nos dois lados (escritores e leitores via garage1 = maioria e garage3 =
minoria) com fases baseline (10 s) → partition (120 s; garage3 fora da rede interna) → heal (até a malha
completa + 10 s). Um monitor lê GetClusterStatus de garage1 e garage3 a cada 1 s para medir quando cada lado
passa a ver o outro como indisponível (janela de detecção) e quando a malha volta.
"""

from __future__ import annotations

import json
import threading
import time
from collections import Counter

from lab import cluster, garage, plots, stats
from lab.experiments.base import Experiment, fase_por_intervalo
from lab.report import tabela_md
from lab.workload import ConfigCarga, SinkJSONL, Workload

LADOS = {1: "maioria", 3: "minoria"}


class E6(Experiment):
    nome = "E6"
    descricao = "Partição de rede"

    def _monitor(self, t0: float, parar: threading.Event, saida: list) -> None:
        while not parar.is_set():
            for n in (1, 3):
                try:
                    st = self.api.cluster_status(n)
                    up = sorted(x["hostname"] for x in st["nodes"] if x.get("isUp") and x.get("hostname"))
                except garage.ErroGarage:
                    up = None
                saida.append({"t": round(time.monotonic() - t0, 2), "no": n, "ve_up": up})
            parar.wait(1.0)

    def run_rep(self, i: int) -> dict:
        baseline, particao = (5, 40) if self.quick else (10, 120)
        cfg = ConfigCarga(
            escritores=(1, 3),
            leitores=(1, 3),
            taxa_escrita=5,
            taxa_fria=0,
            taxa_leitura=5,
            chave_quente=f"e6/r{i}-{int(time.time())}",
            lados={1: "maj", 3: "min"},
        )
        sink = SinkJSONL(self.dir / f"ops_rep{i}.jsonl")
        w = Workload(cfg, {"run": self.dir.name, "rep": i, "mode": "consistent", "scenario": "E6"}, [sink])
        monitor, parar = [], threading.Event()
        th = threading.Thread(target=self._monitor, args=(w.t0, parar, monitor))
        marcos = {}
        try:
            th.start()
            w.fase = "baseline"
            w.iniciar()
            time.sleep(baseline)
            marcos["partition_start"] = round(w.agora(), 3)  # antes do disconnect
            cluster.particionar(3)
            marcos["partition_at"] = round(w.agora(), 3)  # depois de o disconnect terminar
            w.fase = "partition"
            self.evidencia(
                f"rep{i}_status_inicio_particao.txt",
                "GetClusterStatus garage1 e garage3 (logo após particionar)",
                lambda: {n: self.api.cluster_status(n) for n in (1, 3)},
            )
            time.sleep(particao)
            self.evidencia(
                f"rep{i}_status_fim_particao.txt",
                "GetClusterStatus garage1 e garage3 (fim da partição)",
                lambda: {n: self.api.cluster_status(n) for n in (1, 3)},
            )
            marcos["heal_start"] = round(w.agora(), 3)  # antes do connect
            cluster.curar(3)
            marcos["heal_at"] = round(w.agora(), 3)
            w.fase = "heal"
            t_malha = garage.esperar_malha(limite_s=180, api=self.api)
            marcos["converged_at"] = round(w.agora(), 2)
            time.sleep(10)
        finally:
            w.parar()
            sink.fechar()
            parar.set()
            th.join()
        (self.dir / f"phases_rep{i}.json").write_text(json.dumps(marcos))
        return {
            "marcos": marcos,
            "t_malha_s": round(t_malha, 2),
            "monitor": monitor,
            "ops": self._resumir_ops(self.dir / f"ops_rep{i}.jsonl", marcos),
        }

    @staticmethod
    def _resumir_ops(arq, marcos: dict) -> dict:
        """Por (fase, lado, op): tentativas, ok, timeouts, erros de quórum, stale, latências; e instantes-chave."""
        res: dict = {}
        prim_quorum_min = prim_ok_min_pos_cura = None
        for linha in arq.read_text().splitlines():
            ev = json.loads(linha)
            lado = LADOS.get(ev["node"], str(ev["node"]))
            # fase pelo intervalo inteiro da operação; quem atravessa uma fronteira vai para "transicao"
            fase = fase_por_intervalo(ev["t_start"], ev["t_end"], marcos)
            ev["phase"] = fase
            d = res.setdefault(
                f"{fase}|{lado}|{ev['op']}",
                {"tentativas": 0, "ok": 0, "timeout": 0, "quorum": 0, "outro_erro": 0, "stale": 0, "lat_ms": []},
            )
            d["tentativas"] += 1
            erro = ev.get("error") or ""
            if ev["status"] == "ok":
                d["ok"] += 1
                d["lat_ms"].append(round((ev["t_end"] - ev["t_start"]) * 1000, 1))
                if ev.get("class") == "stale":
                    d["stale"] += 1
                if ev["phase"] == "heal" and lado == "minoria" and prim_ok_min_pos_cura is None:
                    prim_ok_min_pos_cura = ev["t_end"] - marcos["heal_at"]
            elif erro.startswith("NoSuchKey"):
                d["ok"] += 1  # o nó respondeu (chave ainda sem escrita): disponível
                d.setdefault("nosuchkey", 0)
                d["nosuchkey"] += 1
            elif "Could not reach quorum" in erro:
                d["quorum"] += 1
                if ev["phase"] == "partition" and lado == "minoria":
                    t = ev["t_end"] - marcos["partition_at"]
                    prim_quorum_min = t if prim_quorum_min is None else min(prim_quorum_min, t)
            elif "timeout" in erro.lower():
                d["timeout"] += 1
            else:
                d["outro_erro"] += 1
        return {
            "por_grupo": res,
            "primeiro_erro_quorum_minoria_s": prim_quorum_min,
            "primeiro_ok_minoria_apos_cura_s": prim_ok_min_pos_cura,
        }

    # ------------------------------------------------------------ análise

    def analyze(self) -> dict:
        reps = [r["resultado"] for r in self.repeticoes if r.get("situacao") == "ok"]
        if not reps:
            return {"resultados": []}
        soma: dict[str, Counter] = {}
        lat: dict[str, list[float]] = {}
        for r in reps:
            for g, d in r["ops"]["por_grupo"].items():
                soma.setdefault(g, Counter()).update({k: v for k, v in d.items() if k != "lat_ms"})
                lat.setdefault(g, []).extend(d["lat_ms"])

        def disp(fase, lado, op=None):
            ops = ("PUT", "GET") if op is None else (op,)
            t = sum(soma.get(f"{fase}|{lado}|{o}", Counter())["tentativas"] for o in ops)
            ok = sum(soma.get(f"{fase}|{lado}|{o}", Counter())["ok"] for o in ops)
            return ok, t

        # detecção: 1º instante (após partition_at) em que garage1 não vê garage3 e garage3 não vê garage1
        det_g1, det_g3 = [], []
        for r in reps:
            pa, ha = r["marcos"]["partition_at"], r["marcos"]["heal_at"]
            m1 = [
                x
                for x in r["monitor"]
                if x["no"] == 1 and pa <= x["t"] <= ha and x["ve_up"] is not None and "garage3" not in x["ve_up"]
            ]
            m3 = [
                x
                for x in r["monitor"]
                if x["no"] == 3 and pa <= x["t"] <= ha and x["ve_up"] is not None and "garage1" not in x["ve_up"]
            ]
            det_g1.append(m1[0]["t"] - pa if m1 else None)
            det_g3.append(m3[0]["t"] - pa if m3 else None)
        prim_quorum = [r["ops"]["primeiro_erro_quorum_minoria_s"] for r in reps]
        volta_min = [r["ops"]["primeiro_ok_minoria_apos_cura_s"] for r in reps]
        malha = [r["t_malha_s"] for r in reps]

        def resumo(v):
            x = [a for a in v if a is not None]
            if not x:
                return "—"
            s = stats.resumir(x)
            return f"{s.media:.1f} ± {0 if s.n < 2 else s.desvio:.1f} s (n={s.n})"

        linhas_tab, linhas_csv = [], []
        for g in sorted(soma):
            fase, lado, op = g.split("|")
            c = soma[g]
            l50 = stats.resumir(lat[g]).p50 if lat[g] else None
            l95 = stats.resumir(lat[g]).p95 if lat[g] else None
            linhas_tab.append(
                [
                    fase,
                    lado,
                    op,
                    c["tentativas"],
                    c["ok"],
                    c["timeout"],
                    c["quorum"],
                    c["outro_erro"],
                    c["stale"],
                    f"{l50:.1f}" if l50 else "—",
                    f"{l95:.1f}" if l95 else "—",
                ]
            )
            linhas_csv.append({"fase": fase, "lado": lado, "op": op, **c, "p50_ms": l50, "p95_ms": l95})
        self.gravar_csv("metrics.csv", linhas_csv)

        ok_maj, t_maj = disp("partition", "maioria")
        ok_min, t_min = disp("partition", "minoria")
        stale_min = sum(soma.get(f"partition|minoria|{o}", Counter())["stale"] for o in ("GET",))
        maj_erros = sum(
            soma.get(f"partition|maioria|{o}", Counter())[k]
            for o in ("PUT", "GET")
            for k in ("timeout", "quorum", "outro_erro")
        )
        lat_maj_base = stats.resumir(lat.get("baseline|maioria|GET") or [0]).p95
        lat_maj_part = stats.resumir(lat.get("partition|maioria|GET") or [0]).p95

        fases = ["baseline", "partition", "heal"]
        series = {
            lado: ([disp(f, lado)[0] / max(1, disp(f, lado)[1]) * 100 for f in fases], [0] * 3)
            for lado in ("maioria", "minoria")
        }
        plots.salvar(
            plots.barras_agrupadas(
                fases,
                series,
                "Disponibilidade por lado e fase (consistent)",
                "% de operações ok",
                {"maioria": "#2b6cb0", "minoria": "#c53030"},
            ),
            self.dir_figuras,
            "disponibilidade_por_lado",
        )

        p1 = ok_maj / max(1, t_maj) >= 0.9
        p2 = ok_min == 0 and stale_min == 0
        # critério definido antes das execuções: detecção (quórum e isUp) entre 30 e 120 s após particionar
        p3 = all(q is not None and 30 <= q <= 120 for q in prim_quorum) and all(
            d is not None and 30 <= d <= 120 for d in det_g1 + det_g3
        )
        p4_obs = maj_erros > 0 or lat_maj_part > 2 * lat_maj_base
        p5 = all(v is not None for v in volta_min)
        resultados = [
            {
                "id": "P1",
                "situacao": "confirmada" if p1 else "refutada",
                "resultado": f"maioria na partição: {ok_maj}/{t_maj} ok ({ok_maj / max(1, t_maj) * 100:.1f}%)",
            },
            {
                "id": "P2",
                "situacao": "confirmada" if p2 else "refutada",
                "resultado": f"minoria na partição: {ok_min}/{t_min} ok; leituras desatualizadas: {stale_min}",
            },
            {
                "id": "P3",
                "situacao": "confirmada" if p3 else "refutada",
                "resultado": f'1º "Could not reach quorum" na minoria após particionar: {resumo(prim_quorum)}; '
                f"garage1 deixa de ver garage3: {resumo(det_g1)}; garage3 deixa de ver garage1: {resumo(det_g3)}",
            },
            {
                "id": "P4",
                "situacao": "confirmada" if p4_obs else "inconclusiva",
                "resultado": f"maioria na partição: {maj_erros} erros; p95 GET baseline {lat_maj_base:.1f} ms × "
                f"partição {lat_maj_part:.1f} ms" + ("" if p4_obs else " (efeito 'pode' não observado)"),
            },
            {
                "id": "P5",
                "situacao": "confirmada" if p5 else "refutada",
                "resultado": f"malha completa após curar: {resumo(malha)}; 1º ok da minoria após curar: {resumo(volta_min)}",
            },
        ]
        conclusao = (
            "Em `consistent`, sob partição, a minoria **não responde** (nem com a cópia local) e a maioria "
            "segue atendendo: o Garage escolhe **consistência (C)** e sacrifica a **disponibilidade (A)** do "
            "lado minoritário."
            if p2 and p1
            else "Ver tabela: o comportamento observado não bateu integralmente com a previsão; analisar antes de concluir."
        )
        return {
            "resultados": resultados,
            "secoes": [
                (
                    "Operações por fase e lado (todas as repetições)",
                    tabela_md(
                        [
                            "fase",
                            "lado",
                            "op",
                            "tentativas",
                            "ok",
                            "timeout",
                            "erro de quórum",
                            "outro erro",
                            "stale",
                            "p50 (ms)",
                            "p95 (ms)",
                        ],
                        linhas_tab,
                    ),
                ),
                (
                    "Janela de detecção e reconexão",
                    tabela_md(
                        ["medida", "média ± desvio"],
                        [
                            ["1º erro de quórum na minoria após particionar", resumo(prim_quorum)],
                            ["garage1 marca garage3 como indisponível", resumo(det_g1)],
                            ["garage3 marca garage1 como indisponível", resumo(det_g3)],
                            ["malha completa após curar", resumo(malha)],
                            ["1º ok da minoria após curar", resumo(volta_min)],
                        ],
                    ),
                ),
                ("Conclusão C × A (roteiro E6.3)", conclusao),
            ],
            "figuras": ["disponibilidade_por_lado"],
        }
