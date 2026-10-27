"""Tempo sem resposta (B8): fração da fase coberta por operações sem resposta, por papel (op, nó)."""

import json

from lab import metricas

MARCOS = {"partition_start": 10.0, "partition_at": 10.0, "heal_start": 130.0, "heal_at": 130.0, "converged_at": 150.0}


def _ev(op, no, t0, t1, ok=True, classe=None, erro=None):
    return {
        "op": op,
        "node": no,
        "t_start": t0,
        "t_end": t1,
        "status": "ok" if ok else "error",
        "class": classe,
        "error": erro,
    }


def test_uniao_recorta_e_junta_intervalos():
    assert metricas.uniao([(5, 15), (12, 20), (30, 33), (125, 140)], 10, 130) == 5 + 5 + 3 + 5


def test_tempo_sem_resposta_por_papel():
    eventos = [
        _ev("GET", 3, 10.0, 13.0, ok=False, classe="error", erro="ReadTimeoutError"),  # 3 s sem resposta
        _ev("GET", 3, 13.0, 13.01, ok=False, classe="error", erro="Could not reach quorum"),  # erro rápido: ~0
        _ev("GET", 3, 20.0, 20.01, ok=False, classe="stale", erro="NoSuchKey"),  # NoSuchKey classificado = respondeu
        _ev("GET", 2, 10.0, 70.0, ok=False, classe="error", erro="ReadTimeoutError"),
        _ev("PUT", 1, 50.0, 50.01),
    ]
    r = metricas.tempo_sem_resposta(eventos, MARCOS)
    assert r[("GET", 3)]["sem_resposta_s"] == 3.01 and r[("GET", 3)]["tentativas"] == 3
    assert r[("GET", 2)]["fracao"] == 0.5
    assert r[("PUT", 1)]["sem_resposta_s"] == 0


def test_de_execucao_le_ops_e_fases(tmp_path):
    (tmp_path / "phases_rep0_degraded.json").write_text(json.dumps(MARCOS))
    evs = [_ev("GET", 3, 10.0, 40.0, ok=False, classe="error", erro="ReadTimeoutError")]
    (tmp_path / "ops_rep0_degraded.jsonl").write_text("\n".join(json.dumps(e) for e in evs) + "\n")
    linhas = metricas.de_execucao(tmp_path)
    assert linhas == [
        {
            "rep": 0,
            "modo": "degraded",
            "op": "GET",
            "no": 3,
            "tentativas": 1,
            "janela_s": 120.0,
            "sem_resposta_s": 30.0,
            "fracao": 0.25,
        }
    ]


def test_tabela_por_papel_agrega_repeticoes():
    linhas = [
        {"rep": 0, "modo": "x", "op": "GET", "no": 3, "fracao": 0.5, "sem_resposta_s": 60.0},
        {"rep": 1, "modo": "x", "op": "GET", "no": 3, "fracao": 0.7, "sem_resposta_s": 84.0},
    ]
    assert metricas.tabela_por_papel(linhas, {("GET", 3): "leitura minoria"}) == [
        ["x", "leitura minoria", 2, "60.0 ± 14.1%", "72.0"]
    ]
