"""E4 mínimo no cluster real (roteiro §7.3 E4§12).

Modo consistent (R = 2, W = 2, R + W > N): o escritor sobrescreve a chave via garage1 e leitores leem em laço
via garage2 e garage3. Previsão (quóruns que se cruzam, C6): 0 leituras desatualizadas e 0 anomalias.
Uma única `anomaly` invalida a execução.
"""

import json
import uuid
from collections import Counter

import pytest

from lab import garage
from lab.config import NOS, modo_atual
from lab.workload import ConfigCarga, SinkJSONL, Workload

pytestmark = pytest.mark.integration

DURACAO_S = 15


def test_e4_minimo_consistent_sem_stale_nem_anomaly(tmp_path):
    garage.esperar_saude(NOS, limite_s=30)
    assert modo_atual() == "consistent", "rode `labctl mode set consistent` antes"

    sink = SinkJSONL(tmp_path / "ops.jsonl")
    cfg = ConfigCarga(
        escritores=(1,),
        leitores=(2, 3),
        chave_quente=f"e4/{uuid.uuid4().hex[:8]}",
        prefixo_frias=f"e4/cold-{uuid.uuid4().hex[:4]}/",
    )
    w = Workload(cfg, {"run": "e4-minimo", "rep": 0, "mode": "consistent", "scenario": "E4"}, [sink])
    w.executar_por(DURACAO_S)
    sink.fechar()

    eventos = [json.loads(x) for x in (tmp_path / "ops.jsonl").read_text().splitlines()]
    leituras = [e for e in eventos if e["op"] == "GET"]
    escritas = [e for e in eventos if e["op"] == "PUT"]
    por_classe = Counter((e["node"], e["class"]) for e in leituras)
    print(f"\nE4 mínimo ({DURACAO_S} s, consistent): {len(escritas)} PUT, {len(leituras)} GET")
    for chave, n in sorted(w.resumo().items()):
        print(f"  {chave}: {n}")

    assert len(escritas) >= DURACAO_S * 5 and len(leituras) >= DURACAO_S * 10
    assert all(e["status"] == "ok" for e in escritas), "PUT falhou em cluster saudável"
    assert sum(n for (_, c), n in por_classe.items() if c == "anomaly") == 0, "anomalia invalida a execução"
    assert sum(n for (_, c), n in por_classe.items() if c == "stale") == 0, "stale em consistent: investigar"
    assert {c for (_, c) in por_classe} == {"fresh"}
