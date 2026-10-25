"""Análise do E9 com dados sintéticos (sem Docker): classificação, critérios P1–P6 e figuras."""

import json

from lab.experiments.e9_modos import E9

MARCOS = {"partition_start": 10.0, "partition_at": 10.1, "heal_start": 130.0, "heal_at": 130.1, "converged_at": 150.0}
QUORUM = "ServiceUnavailable: Internal error: Could not reach quorum of 2 (sets=None)"
TIMEOUT = 'ReadTimeoutError: Read timeout on endpoint URL: "http://x/sdlab/k"'


def _ev(op, no, t, status="ok", classe=None, erro=None, **extra):
    return {
        "op": op,
        "node": no,
        "key": "k",
        "t_start": t,
        "t_end": t + 0.01 if status == "ok" else t + 3.0,
        "status": status,
        "error": erro,
        "class": classe,
        "motivo": None,
        "lag_ms": extra.pop("lag_ms", None),
        **extra,
    }


def _eventos(modo, minoria_responde=None):
    responde = modo != "consistent" if minoria_responde is None else minoria_responde
    evs = [_ev("GET", 3, 1.0, classe="fresh"), _ev("GET", 2, 1.0, classe="fresh"), _ev("PUT", 1, 1.0)]
    for k in range(20):
        t = 20.0 + k
        evs.append(_ev("GET", 2, t, classe="fresh"))
        evs.append(_ev("PUT", 1, t))
        if responde:
            evs.append(_ev("GET", 3, t, classe="stale", lag_ms=100.0 * (k + 1)))
            evs.append(_ev("LIST", 3, t, faltando=k + 1, esperadas=k + 5))
        else:
            evs.append(_ev("GET", 3, t, status="error", classe="error", erro=TIMEOUT if k < 10 else QUORUM))
            evs.append(_ev("LIST", 3, t, status="error", erro=QUORUM))
        if modo == "dangerous":
            evs.append(_ev("PUT", 3, t))
        else:
            evs.append(_ev("PUT", 3, t, status="error", erro=QUORUM))
    evs.append(_ev("GET", 3, 140.0, classe="fresh"))
    return evs


def _experimento(tmp_path, eventos_por_modo, faltando_por_modo):
    e = E9(reps=1, base=tmp_path)
    e.dir.mkdir(parents=True)
    rodadas = {}
    for modo, evs in eventos_por_modo.items():
        (e.dir / f"ops_rep0_{modo}.jsonl").write_text("\n".join(json.dumps(x) for x in evs) + "\n")
        rodadas[modo] = {
            "marcos": MARCOS,
            "t_malha_s": 19.9,
            "monitor": [{"t": 90.0, "no": 1, "ve_up": ["garage1", "garage2"]}],
            "verificacao": {
                "ultima_versao_confirmada": 1500,
                "quente_lida": {"g1": 1500, "g2": 1500, "g3": 1500},
                "quente_atualizada_nos": ["g1", "g2", "g3"],
                "frias_da_particao": 240,
                "frias_faltando": {"g3": faltando_por_modo[modo], "g2": 0},
                "frias_erros": {"g3": 0, "g2": 0},
            },
        }
    rep = {"rep": 0, "situacao": "ok", "resultado": {"ordem": list(rodadas), "rodadas": rodadas}}
    e.repeticoes = [json.loads(json.dumps(rep))]  # como volta do manifest.json
    return e


def _situacoes(analise):
    return {r["id"]: r["situacao"] for r in analise["resultados"]}


def test_comportamento_previsto_confirma_tudo(tmp_path):
    modos = ("consistent", "degraded", "dangerous")
    e = _experimento(tmp_path, {m: _eventos(m) for m in modos}, {"consistent": 0, "degraded": 30, "dangerous": 25})
    analise = e.analyze()
    assert _situacoes(analise) == dict.fromkeys(("P1", "P2", "P3", "P4", "P5", "P6"), "confirmada")
    for nome in analise["figuras"]:
        assert (e.dir_figuras / f"{nome}.png").exists()
    assert {"timeline_consistent", "timeline_degraded", "timeline_dangerous"} <= set(analise["figuras"])
    texto = "\n".join(corpo for _, corpo in analise["secoes"])
    assert "n/a" in texto  # taxa de desatualizadas sem respostas (P9)


def test_minoria_respondendo_em_consistent_refuta_p1(tmp_path):
    modos = ("consistent", "degraded", "dangerous")
    eventos = {m: _eventos(m) for m in modos}
    eventos["consistent"] = _eventos("consistent", minoria_responde=True)
    e = _experimento(tmp_path, eventos, {"consistent": 0, "degraded": 30, "dangerous": 25})
    sit = _situacoes(e.analyze())
    assert sit["P1"] == "refutada"
    assert sit["P5"] == "refutada"


def test_frias_presentes_em_degraded_refuta_p6(tmp_path):
    modos = ("consistent", "degraded", "dangerous")
    e = _experimento(tmp_path, {m: _eventos(m) for m in modos}, {"consistent": 0, "degraded": 0, "dangerous": 25})
    assert _situacoes(e.analyze())["P6"] == "refutada"


def test_fase_pelo_intervalo_e_classificacao_de_erros(tmp_path):
    evs = _eventos("consistent")
    evs.append(_ev("GET", 3, 129.0, status="error", classe="error", erro=TIMEOUT))  # atravessa heal_start
    e = _experimento(tmp_path, {"consistent": evs}, {"consistent": 0})
    e.modos = ("consistent",)
    e.analyze()
    linhas = (e.dir / "metrics.csv").read_text().splitlines()
    cab = linhas[0].split(",")
    por = [dict(zip(cab, linha.split(","), strict=False)) for linha in linhas[1:]]
    trans = [r for r in por if r["fase"] == "transicao"]
    assert len(trans) == 1 and trans[0]["timeout"] == "1"
    part_g3 = next(r for r in por if r["fase"] == "partition" and r["op"] == "GET" and r["no"] == "3")
    assert part_g3["timeout"] == "10" and part_g3["quorum"] == "10"
