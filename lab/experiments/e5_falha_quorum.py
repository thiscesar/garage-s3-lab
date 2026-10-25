"""E5 — Falha e quórum (roteiro §4.2 e §7.3).

Por repetição e por modo:
- estado A (3 nós): 20 PUT + 20 GET por nó vivo (os PUTs viram as chaves lidas nos estados seguintes);
- para o garage3 → estado B (2 nós): 20 PUT + 20 GET por nó vivo;
- para o garage2, medindo o tempo até a 1ª escrita via garage1 falhar → estado C (1 nó): 20 PUT + 20 GET via garage1;
- religa garage2 e garage3, medindo o tempo até a 1ª escrita via garage1 voltar a funcionar; espera a malha completa.
Registra sucesso, erro e a mensagem de cada operação; monta o quadro nós ativos × leitura × escrita.
"""

from __future__ import annotations

import re
import threading
import time
import uuid
from collections import Counter

from lab import cluster, garage, plots, stats
from lab import s3 as lab_s3
from lab.config import MODOS, NOS, QUORUNS
from lab.experiments.base import Experiment
from lab.report import tabela_md

OPS = 20
LIMITE_SONDA_S = 60.0
ESTADOS = {"A": (1, 2, 3), "B": (1, 2), "C": (1,)}


def _normalizar(erro: str | None) -> str:
    """Mensagem de erro sem IDs de nós e números variáveis, para agrupar."""
    if not erro:
        return ""
    texto = re.sub(r"[0-9a-f]{16,64}", "<id>", erro)
    texto = re.sub(r"\d+ of \d+ request", "N of M request", texto)
    return texto[:160]


class E5(Experiment):
    nome = "E5"
    descricao = "Falha e quórum"

    def _operar(self, nos_vivos: tuple[int, ...], chaves_leitura: list[str], prefixo: str, n_ops: int) -> dict:
        saida = {}
        for n in nos_vivos:
            c = lab_s3.cliente_s3(n)
            puts = [lab_s3.put(c, n, f"{prefixo}/g{n}-{k}", b"e5") for k in range(n_ops)]
            gets = [lab_s3.get(c, n, chaves_leitura[k % len(chaves_leitura)]) for k in range(n_ops)]
            saida[f"garage{n}"] = {
                "put_ok": sum(r.ok for r in puts),
                "put_total": len(puts),
                "get_ok": sum(r.ok for r in gets),
                "get_total": len(gets),
                "erros": dict(Counter(_normalizar(r.erro) for r in puts + gets if not r.ok)),
                "chaves_ok": [r.chave for r in puts if r.ok],
            }
        return saida

    def _sonda_escrita(
        self, querer_ok: bool, parar: threading.Event, saida: dict, t_ref: list[float], limite: float
    ) -> None:
        """PUT via garage1 a cada 100 ms até o resultado virar `querer_ok`; grava o instante em saida["t"]."""
        c = lab_s3.cliente_s3(1)
        k = 0
        while not parar.is_set():
            res = lab_s3.put(c, 1, f"e5/sonda/{uuid.uuid4().hex[:8]}-{k}", b"s")
            k += 1
            if t_ref and res.ok == querer_ok:
                saida["t"] = res.t_fim
                saida["erro"] = res.erro
                return
            if t_ref and time.monotonic() - t_ref[0] > limite:
                saida["t"] = None
                return
            time.sleep(0.1)

    def _medir_transicao(self, acao, querer_ok: bool, limite: float) -> tuple[float | None, str | None]:
        """Executa `acao` (parar/ligar nós) com a sonda de escrita rodando; devolve segundos até a sonda mudar."""
        parar, saida, t_ref = threading.Event(), {}, []
        sonda = threading.Thread(target=self._sonda_escrita, args=(querer_ok, parar, saida, t_ref, limite))
        sonda.start()
        t_ref.append(time.monotonic())
        acao()
        sonda.join(limite + 5)
        parar.set()
        sonda.join()
        t = saida.get("t")
        return (None if t is None else round(t - t_ref[0], 2)), saida.get("erro")

    def run_rep(self, i: int) -> dict:
        n_ops = 5 if self.quick else OPS
        resultado = {}
        try:
            for modo in MODOS:
                if modo != "consistent":
                    garage.definir_modo(modo)
                base = f"e5/{modo}/r{i}-{uuid.uuid4().hex[:6]}"
                r = {"estados": {}}
                semeador = lab_s3.cliente_s3(1)  # chaves lidas nos 3 estados, gravadas com os 3 nós ativos
                chaves = [
                    res.chave for k in range(n_ops) if (res := lab_s3.put(semeador, 1, f"{base}/semente-{k}", b"e5")).ok
                ]
                r["estados"]["A"] = self._operar(ESTADOS["A"], chaves, f"{base}/A", n_ops)
                cluster.parar(3)
                r["estados"]["B"] = self._operar(ESTADOS["B"], chaves, f"{base}/B", n_ops)
                limite = 20.0 if modo == "dangerous" else LIMITE_SONDA_S  # W = 1: não deve falhar
                r["t_ate_falhar_s"], r["erro_ao_falhar"] = self._medir_transicao(
                    lambda: cluster.parar(2), False, limite
                )
                r["estados"]["C"] = self._operar(ESTADOS["C"], chaves, f"{base}/C", n_ops)

                def religar():
                    cluster.ligar(2)
                    cluster.ligar(3)

                r["t_ate_voltar_s"], _ = self._medir_transicao(religar, True, LIMITE_SONDA_S)
                r["t_ate_malha_s"] = round(garage.esperar_malha(NOS, limite_s=180), 2)
                resultado[modo] = r
        finally:
            garage.restaurar()
        return resultado

    # ------------------------------------------------------------ análise

    def analyze(self) -> dict:
        reps = [r["resultado"] for r in self.repeticoes if r.get("situacao") == "ok"]
        if not reps:
            return {"resultados": []}
        quadro, linhas_csv, mensagens = [], [], Counter()
        observado: dict[tuple[str, str], tuple[bool, bool]] = {}
        for modo in MODOS:
            r_q, w_q = QUORUNS[modo]
            for estado, vivos in ESTADOS.items():
                put_ok = put_tot = get_ok = get_tot = 0
                for rep in reps:
                    for no, d in rep[modo]["estados"][estado].items():
                        put_ok += d["put_ok"]
                        put_tot += d["put_total"]
                        get_ok += d["get_ok"]
                        get_tot += d["get_total"]
                        mensagens.update({m: c for m, c in d["erros"].items() if m})
                        linhas_csv.append(
                            {
                                "modo": modo,
                                "estado": estado,
                                "no": no,
                                **{k: v for k, v in d.items() if k in ("put_ok", "put_total", "get_ok", "get_total")},
                            }
                        )
                k = len(vivos)
                leitura_ok, escrita_ok = get_ok == get_tot, put_ok == put_tot
                observado[(modo, estado)] = (leitura_ok, escrita_ok)
                quadro.append(
                    [
                        modo,
                        f"{k} ({', '.join(f'g{n}' for n in vivos)})",
                        f"N=3 R={r_q} W={w_q}",
                        f"{'OK' if k >= r_q else 'falha'} / {'OK' if k >= w_q else 'falha'}",
                        f"{get_ok}/{get_tot}",
                        f"{put_ok}/{put_tot}",
                    ]
                )
        self.gravar_csv("metrics.csv", linhas_csv)

        esperado = {
            (m, e): (len(v) >= QUORUNS[m][0], len(v) >= QUORUNS[m][1]) for m in MODOS for e, v in ESTADOS.items()
        }
        p1 = observado == esperado
        falhas_quorum = [m for m in mensagens if "Could not reach quorum" in m]
        p2 = bool(falhas_quorum) and sum(mensagens[m] for m in falhas_quorum) == sum(mensagens.values())
        t_falha = {m: [rep[m]["t_ate_falhar_s"] for rep in reps] for m in MODOS}
        t_volta = {m: [rep[m]["t_ate_voltar_s"] for rep in reps] for m in MODOS}
        t_malha = {m: [rep[m]["t_ate_malha_s"] for rep in reps] for m in MODOS}
        p3 = all(t is not None and t < 10 for m in ("consistent", "degraded") for t in t_falha[m]) and all(
            t is None for t in t_falha["dangerous"]
        )
        voltas = [t for m in ("consistent", "degraded") for t in t_volta[m]]
        p4 = all(t is not None and t < 15 for t in voltas)

        def resumo(valores):
            v = [x for x in valores if x is not None]
            if not v:
                return "não mudou no limite"
            r = stats.resumir(v)
            return f"{r.media:.2f} ± {0 if r.n < 2 else r.desvio:.2f} s (n={r.n}" + (
                f"; {len(valores) - len(v)} sem mudança)" if len(v) < len(valores) else ")"
            )

        tab_tempos = tabela_md(
            ["modo", "parar garage2 → 1ª escrita falha", "religar → 1ª escrita ok", "malha completa"],
            [[m, resumo(t_falha[m]), resumo(t_volta[m]), resumo(t_malha[m])] for m in MODOS],
        )
        medias_volta = [
            stats.resumir([x for x in t_volta[m] if x is not None]).media if any(t_volta[m]) else 0 for m in MODOS
        ]
        plots.salvar(
            plots.barras_com_erro(
                list(MODOS),
                medias_volta,
                [0] * 3,
                "Religar nós → 1ª escrita via garage1 ok",
                "s",
                [plots.CORES_MODO[m] for m in MODOS],
            ),
            self.dir_figuras,
            "tempo_de_recuperacao",
        )
        resultados = [
            {
                "id": "P1",
                "situacao": "confirmada" if p1 else "refutada",
                "resultado": "observado (leitura, escrita) = esperado pelo quórum em todos os 9 casos"
                if p1
                else "diferenças: "
                + "; ".join(
                    f"{m}/{e}: obs {observado[(m, e)]} × esp {esperado[(m, e)]}"
                    for (m, e) in esperado
                    if observado[(m, e)] != esperado[(m, e)]
                ),
            },
            {
                "id": "P2",
                "situacao": "confirmada" if p2 else "refutada",
                "resultado": "; ".join(f'{c}× "{m[:110]}"' for m, c in mensagens.most_common(4)) or "nenhum erro",
            },
            {
                "id": "P3",
                "situacao": "confirmada" if p3 else "refutada",
                "resultado": "; ".join(f"{m}: {resumo(t_falha[m])}" for m in MODOS),
            },
            {
                "id": "P4",
                "situacao": "confirmada" if p4 else "refutada",
                "resultado": "; ".join(f"{m}: {resumo(t_volta[m])}" for m in MODOS),
            },
        ]
        return {
            "resultados": resultados,
            "secoes": [
                (
                    "Quadro nós ativos × leitura × escrita (roteiro E5.3)",
                    tabela_md(
                        ["modo", "nós ativos", "quórum", "teoria (leitura / escrita)", "GET ok", "PUT ok"], quadro
                    ),
                ),
                (
                    "Tempos de transição",
                    tab_tempos
                    + '\n\nSonda: PUT via garage1 a cada 100 ms. "Parar garage2" com o garage3 já parado; "religar" = '
                    "docker start garage2 e garage3.",
                ),
                (
                    "Mensagens de erro (agrupadas, IDs removidos)",
                    tabela_md(["ocorrências", "mensagem"], [[c, m] for m, c in mensagens.most_common(8)])
                    if mensagens
                    else "_nenhuma_",
                ),
            ],
            "figuras": ["tempo_de_recuperacao"],
        }
