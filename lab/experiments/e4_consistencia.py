"""E4 — Consistência (roteiro §4.2 e §7.3).

Por repetição e por modo (consistent, degraded, dangerous):
1. sobrescritas: o escritor faz PUT das versões 1..N na chave via garage1; logo após cada confirmação, um leitor
   via garage2 e outro via garage3 (em paralelo) leem a chave uma vez. O oráculo classifica cada leitura;
   como a leitura começa depois do ack, `fresh` exige a versão recém-confirmada (ou posterior);
2. listagem: cria K objetos via garage1; logo após cada confirmação, ListObjectsV2 via garage2 e garage3 (em
   paralelo) verifica se o objeto recém-criado aparece e quantos dos já confirmados faltam.
Eventos de leitura vão para ops.jsonl no formato da
"""

from __future__ import annotations

import json
import threading
import time
import uuid

from lab import garage, plots, stats
from lab import s3 as lab_s3
from lab.config import MODOS
from lab.experiments.base import Experiment
from lab.oracle import Oraculo
from lab.report import tabela_md
from lab.workload import SinkJSONL

LEITORES = (2, 3)


class E4(Experiment):
    nome = "E4"
    descricao = "Consistência"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.t0 = time.monotonic()  # tempos dos eventos relativos ao início da execução

    def run_rep(self, i: int) -> dict:
        n_versoes = 30 if self.quick else 200
        n_lista = 10 if self.quick else 50
        sink = SinkJSONL(self.dir / "ops.jsonl")
        resultado = {}
        try:
            for modo in MODOS:
                if modo != "consistent":
                    garage.definir_modo(modo)
                resultado[modo] = {
                    "sobrescritas": self._sobrescritas(i, modo, n_versoes, sink),
                    "listagem": self._listagem(modo, n_lista),
                }
        finally:
            sink.fechar()
            garage.definir_modo("consistent")
        return resultado

    def _sobrescritas(self, i: int, modo: str, n_versoes: int, sink: SinkJSONL) -> dict:
        escritor = lab_s3.cliente_s3(1)
        leitores = {n: lab_s3.cliente_s3(n) for n in LEITORES}
        chave = f"e4/{uuid.uuid4().hex[:8]}"
        oraculo = Oraculo()
        contagem = {n: {"fresh": 0, "stale": 0, "error": 0, "anomaly": 0} for n in LEITORES}
        exemplos_stale = []
        lock = threading.Lock()

        def ler(n: int, v_confirmada: int) -> None:
            res = lab_s3.get(leitores[n], n, chave)
            nosuchkey = bool(res.erro and res.erro.startswith("NoSuchKey"))
            versao = None
            if res.ok:
                try:
                    versao = int(json.loads(res.dados)["v"])
                except (ValueError, KeyError, TypeError):
                    versao = None
            c = oraculo.classificar(chave, res.t_inicio, res.t_fim, versao, None if res.ok else res.erro, nosuchkey)
            with lock:
                contagem[n][c.classe] += 1
                if c.classe == "stale" and len(exemplos_stale) < 20:
                    exemplos_stale.append(
                        {
                            "no": n,
                            "confirmada": v_confirmada,
                            "lida": 0 if nosuchkey else versao,
                            "motivo": c.motivo,
                            "lag_ms": round(c.lag_ms or 0, 1),
                        }
                    )
                sink(
                    {
                        "run": self.dir.name,
                        "rep": i,
                        "mode": modo,
                        "scenario": "E4-sobrescrita",
                        "phase": "sem-falha",
                        "op": "GET",
                        "node": n,
                        "key": chave,
                        "t_start": round(res.t_inicio - self.t0, 4),
                        "t_end": round(res.t_fim - self.t0, 4),
                        "status": "ok" if res.ok else "error",
                        "http": res.http,
                        "error": res.erro,
                        "version_written": None,
                        "version_read": 0 if nosuchkey else versao,
                        "class": c.classe,
                        "motivo": c.motivo,
                        "lag_versions": c.lag_versions,
                        "lag_ms": c.lag_ms,
                    }
                )

        falhas_put = 0
        for v in range(1, n_versoes + 1):
            oraculo.inicio_escrita(chave, v)
            res = lab_s3.put(
                escritor, 1, chave, json.dumps({"v": v, "writer": "maj", "node": 1}).encode(), {"version": str(v)}
            )
            oraculo.fim_escrita(chave, v, res.t_fim, res.ok)
            if not res.ok:
                falhas_put += 1
                continue
            threads = [threading.Thread(target=ler, args=(n, v)) for n in LEITORES]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        return {"contagem": contagem, "falhas_put": falhas_put, "exemplos_stale": exemplos_stale}

    def _listagem(self, modo: str, n_objetos: int) -> dict:
        escritor = lab_s3.cliente_s3(1)
        leitores = {n: lab_s3.cliente_s3(n) for n in LEITORES}
        prefixo = f"e4/lista/{uuid.uuid4().hex[:8]}/"
        confirmadas: list[str] = []
        sem_o_novo = {n: 0 for n in LEITORES}
        faltando_total = {n: 0 for n in LEITORES}
        erros = {n: 0 for n in LEITORES}
        lock = threading.Lock()

        def listar(n: int, nova: str, esperadas: set[str]) -> None:
            res = lab_s3.listar(leitores[n], n, prefixo)
            with lock:
                if not res.ok:
                    erros[n] += 1
                    return
                vistas = set(res.dados)
                if nova not in vistas:
                    sem_o_novo[n] += 1
                faltando_total[n] += len(esperadas - vistas)

        for k in range(n_objetos):
            chave = f"{prefixo}{k:04d}"
            if not lab_s3.put(escritor, 1, chave, b"x").ok:
                continue
            confirmadas.append(chave)
            esperadas = set(confirmadas)
            threads = [threading.Thread(target=listar, args=(n, chave, esperadas)) for n in LEITORES]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        return {
            "listagens_por_no": len(confirmadas),
            "sem_o_novo": sem_o_novo,
            "faltando_total": faltando_total,
            "erros": erros,
            "modo": modo,
        }

    # ------------------------------------------------------------ análise

    def analyze(self) -> dict:
        reps = [r["resultado"] for r in self.repeticoes if r.get("situacao") == "ok"]
        if not reps:
            return {"resultados": []}
        linhas_csv, tab_leit, tab_lista, taxas = [], [], [], {}
        for modo in MODOS:
            stale = leit = anom = err = 0
            for n in LEITORES:
                c_tot = {
                    k: sum(r[modo]["sobrescritas"]["contagem"][n][k] for r in reps)
                    for k in ("fresh", "stale", "error", "anomaly")
                }
                ok = c_tot["fresh"] + c_tot["stale"]
                p = stats.wilson(c_tot["stale"], ok) if ok else None
                tab_leit.append(
                    [
                        modo,
                        f"garage{n}",
                        ok,
                        c_tot["stale"],
                        c_tot["anomaly"],
                        c_tot["error"],
                        f"{p.p * 100:.2f}% [{p.ic95[0] * 100:.2f}–{p.ic95[1] * 100:.2f}]" if p else "n/a",
                    ]
                )
                stale += c_tot["stale"]
                leit += ok
                anom += c_tot["anomaly"]
                err += c_tot["error"]
                for k, r in enumerate(reps):
                    cc = r[modo]["sobrescritas"]["contagem"][n]
                    linhas_csv.append({"secao": "sobrescrita", "rep": k, "modo": modo, "no": n, **cc})
            taxas[modo] = (stale, leit, anom, err)
            for n in LEITORES:
                erros_l = sum(r[modo]["listagem"]["erros"][n] for r in reps)
                # denominador = listagens respondidas (como no GET, erros ficam fora da taxa)
                listagens = sum(r[modo]["listagem"]["listagens_por_no"] for r in reps) - erros_l
                sem_novo = sum(r[modo]["listagem"]["sem_o_novo"][n] for r in reps)
                faltando = sum(r[modo]["listagem"]["faltando_total"][n] for r in reps)
                ic = stats.wilson(sem_novo, listagens) if listagens else None
                taxa = f"{ic.p * 100:.2f}% [{ic.ic95[0] * 100:.2f}–{ic.ic95[1] * 100:.2f}]" if ic else "n/a"
                tab_lista.append([modo, f"garage{n}", listagens, sem_novo, faltando, erros_l, taxa])
                for k, r in enumerate(reps):
                    linhas_csv.append(
                        {
                            "secao": "listagem",
                            "rep": k,
                            "modo": modo,
                            "no": n,
                            "sem_o_novo": r[modo]["listagem"]["sem_o_novo"][n],
                            "faltando": r[modo]["listagem"]["faltando_total"][n],
                        }
                    )
        self.gravar_csv("metrics.csv", linhas_csv)

        fracs = [taxas[m][0] / taxas[m][1] * 100 if taxas[m][1] else 0 for m in MODOS]
        ics = [stats.wilson(taxas[m][0], taxas[m][1]) if taxas[m][1] else None for m in MODOS]
        erros_ic = [(ic.ic95[1] * 100 - f) if ic else 0 for ic, f in zip(ics, fracs, strict=True)]
        plots.salvar(
            plots.barras_com_erro(
                list(MODOS),
                fracs,
                erros_ic,
                "Leituras desatualizadas sem falha (E4)",
                "% das leituras ok (IC95 Wilson, limite superior)",
                [plots.CORES_MODO[m] for m in MODOS],
            ),
            self.dir_figuras,
            "stale_por_modo",
        )

        lista = {m: [x for x in tab_lista if x[0] == m] for m in MODOS}
        p1 = taxas["consistent"][0] == 0 and taxas["consistent"][2] == 0
        p2 = all(x[3] == 0 and x[4] == 0 for x in lista["consistent"])
        sem_falha = all(taxas[m][2] == 0 for m in MODOS)
        p3_raro = all(taxas[m][0] <= 0.05 * max(1, taxas[m][1]) for m in ("degraded", "dangerous"))
        p4 = all(
            sum(x[3] for x in lista[m]) <= 0.05 * max(1, sum(x[2] for x in lista[m])) for m in ("degraded", "dangerous")
        )
        exemplos = [e | {"modo": m} for r in reps for m in MODOS for e in r[m]["sobrescritas"]["exemplos_stale"]][:15]
        # atraso das leituras desatualizadas (lag_ms do oráculo: início da leitura − 1ª confirmação de uma escrita mais
        # nova); só sai dos exemplos guardados (até 20 por modo e repetição), então registra se a amostra está completa
        tab_atraso = []
        for m in MODOS:
            lags = [
                e["lag_ms"] for r in reps for e in r[m]["sobrescritas"]["exemplos_stale"] if e.get("lag_ms") is not None
            ]
            if not lags:
                tab_atraso.append([m, 0, "—", "—", "sim" if taxas[m][0] == 0 else "não"])
                continue
            s = stats.resumir(lags)
            desvio = 0 if s.n < 2 else s.desvio
            tab_atraso.append(
                [m, s.n, f"{s.media:.2f} ± {desvio:.2f}", f"{s.maximo:.2f}", "sim" if s.n == taxas[m][0] else "não"]
            )
        resultados = [
            {
                "id": "P1",
                "situacao": "confirmada" if p1 else "refutada",
                "resultado": f"consistent: {taxas['consistent'][0]} stale e {taxas['consistent'][2]} anomaly em "
                f"{taxas['consistent'][1]} leituras ok ({taxas['consistent'][3]} erros)",
            },
            {
                "id": "P2",
                "situacao": "confirmada" if p2 else "refutada",
                "resultado": "consistent: listagens sem o objeto novo: "
                + ", ".join(f"{x[1]} {x[3]}/{x[2]}" for x in lista["consistent"]),
            },
            {
                "id": "P3",
                "situacao": ("confirmada" if p3_raro and sem_falha else "refutada"),
                "resultado": "; ".join(
                    f"{m}: {taxas[m][0]} stale em {taxas[m][1]} ({taxas[m][0] / max(1, taxas[m][1]) * 100:.2f}%)"
                    for m in ("degraded", "dangerous")
                )
                + (
                    " — nenhum caso observado: 'possíveis' não demonstrado nesta execução"
                    if all(taxas[m][0] == 0 for m in ("degraded", "dangerous"))
                    else ""
                ),
            },
            {
                "id": "P4",
                "situacao": "confirmada" if p4 else "refutada",
                "resultado": "; ".join(
                    f"{m}: sem o novo " + ", ".join(f"{x[1]} {x[3]}/{x[2]}" for x in lista[m])
                    for m in ("degraded", "dangerous")
                ),
            },
        ]
        return {
            "resultados": resultados,
            "secoes": [
                (
                    "Leituras após cada confirmação (sobrescritas)",
                    tabela_md(
                        [
                            "modo",
                            "leitor",
                            "leituras ok",
                            "stale",
                            "anomaly",
                            "error",
                            "taxa stale por nó [IC95 Wilson]",
                        ],
                        tab_leit,
                    ),
                ),
                (
                    "Listagem logo após cada criação",
                    tabela_md(
                        [
                            "modo",
                            "leitor",
                            "listagens respondidas",
                            "sem o objeto novo",
                            "objetos confirmados faltando (soma)",
                            "erros",
                            "taxa sem o novo por nó [IC95 Wilson]",
                        ],
                        tab_lista,
                    )
                    + "\n\nCritério da P4 (definido antes das execuções): listagens sem o objeto novo ≤ 5% em "
                    "degraded e dangerous. Comparar as taxas e os IC95 com os do GET (tabela anterior).",
                ),
                (
                    "Atraso das leituras desatualizadas",
                    tabela_md(
                        ["modo", "n", "atraso médio ± desvio (ms)", "máximo (ms)", "todas as desatualizadas"],
                        tab_atraso,
                    )
                    + "\n\nAtraso = início da leitura desatualizada − 1ª confirmação de uma escrita mais nova (oráculo): "
                    "limite inferior da janela em que a réplica lida ainda não tinha a escrita.",
                ),
                (
                    "Exemplos de leituras desatualizadas (até 15)",
                    tabela_md(
                        ["modo", "nó", "versão confirmada", "versão lida", "motivo", "lag (ms)"],
                        [
                            [e["modo"], e["no"], e["confirmada"], e["lida"], e["motivo"] or "—", e["lag_ms"]]
                            for e in exemplos
                        ],
                    )
                    if exemplos
                    else "_nenhuma_",
                ),
            ],
            "figuras": ["stale_por_modo"],
        }
