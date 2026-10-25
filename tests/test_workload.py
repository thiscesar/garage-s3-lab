"""Carga + oráculo sem Docker: S3 falso em memória com uma réplica por nó."""

import json
import threading
import time

import pytest

from lab import workload
from lab.s3 import Resultado
from lab.workload import ConfigCarga, SinkJSONL, Workload

CAMPOS_EVENTO = {
    "run",
    "rep",
    "mode",
    "scenario",
    "phase",
    "op",
    "node",
    "key",
    "t_start",
    "t_end",
    "status",
    "http",
    "error",
    "version_written",
    "version_read",
    "class",
    "lag_versions",
    "lag_ms",
}


class S3Falso:
    """Uma réplica por nó. Escritas vão às réplicas de `aplicar_em`; cada leitura vê só a réplica do nó."""

    def __init__(self, aplicar_em=(1, 2, 3)):
        self.replicas = {n: {} for n in (1, 2, 3)}
        self.aplicar_em = aplicar_em
        self.lock = threading.Lock()


@pytest.fixture
def s3_falso(monkeypatch):
    estado = {"s3": S3Falso()}

    def cliente_s3(n, **_):
        return n

    def put(no, _no, chave, corpo, _meta=None):
        t0 = time.monotonic()
        with estado["s3"].lock:
            for r in estado["s3"].aplicar_em:
                estado["s3"].replicas[r][chave] = corpo
        return Resultado("PUT", no, chave, t0, time.monotonic(), True, http=200)

    def get(no, _no, chave):
        t0 = time.monotonic()
        with estado["s3"].lock:
            corpo = estado["s3"].replicas[no].get(chave)
        if corpo is None:
            return Resultado("GET", no, chave, t0, time.monotonic(), False, http=404, erro="NoSuchKey: Key not found")
        return Resultado("GET", no, chave, t0, time.monotonic(), True, http=200, dados=corpo)

    monkeypatch.setattr(workload.lab_s3, "cliente_s3", cliente_s3)
    monkeypatch.setattr(workload.lab_s3, "put", put)
    monkeypatch.setattr(workload.lab_s3, "get", get)
    return estado


def rodar(segundos=1.0, **kw):
    eventos = []
    cfg = ConfigCarga(
        taxa_escrita=40, taxa_fria=10, taxa_leitura=50, chave_quente="t/hot", prefixo_frias="t/cold/", **kw
    )
    w = Workload(cfg, {"run": "teste", "rep": 0, "mode": "consistent", "scenario": "T"}, [eventos.append])
    w.executar_por(segundos)
    return w, eventos


def test_replicas_sempre_iguais_dao_so_fresh(s3_falso):
    w, eventos = rodar()
    leituras = [e for e in eventos if e["op"] == "GET"]
    escritas = [e for e in eventos if e["op"] == "PUT"]
    assert len(leituras) > 50 and len(escritas) > 30
    assert {e["class"] for e in leituras} == {"fresh"}
    assert all(e["status"] == "ok" for e in escritas)
    assert w.chaves_frias and all(k.startswith("t/cold/g1-") for k in w.chaves_frias)


def test_eventos_seguem_o_formato_da_spec(s3_falso, tmp_path):
    sink = SinkJSONL(tmp_path / "ops.jsonl")
    cfg = ConfigCarga(taxa_escrita=20, taxa_fria=0, taxa_leitura=20, chave_quente="t/hot")
    w = Workload(cfg, {"run": "r", "rep": 1, "mode": "degraded", "scenario": "A1"}, [sink])
    w.fase = "partition"
    w.executar_por(0.5)
    sink.fechar()
    linhas = [json.loads(x) for x in (tmp_path / "ops.jsonl").read_text().splitlines()]
    assert linhas
    for ev in linhas:
        assert CAMPOS_EVENTO <= set(ev)
        assert ev["phase"] == "partition" and ev["mode"] == "degraded" and ev["scenario"] == "A1"
        assert 0 <= ev["t_start"] <= ev["t_end"]
    puts = [e["version_written"] for e in linhas if e["op"] == "PUT"]
    assert sorted(puts) == list(range(1, len(puts) + 1))  # versões únicas e contíguas na chave quente


def test_replica_atrasada_gera_stale_e_nosuchkey(s3_falso):
    # garage3 nunca recebe as escritas (como uma réplica fora do quórum de escrita W = 2)
    s3_falso["s3"] = S3Falso(aplicar_em=(1, 2))
    w, eventos = rodar(escritores=(1,), leitores=(2, 3))
    via2 = [e for e in eventos if e["op"] == "GET" and e["node"] == 2]
    via3 = [e for e in eventos if e["op"] == "GET" and e["node"] == 3]
    assert {e["class"] for e in via2} == {"fresh"}
    stale3 = [e for e in via3 if e["class"] == "stale"]
    assert stale3 and all(e["motivo"] == "nosuchkey" and e["version_read"] == 0 for e in stale3)
    assert all(e["lag_versions"] >= 1 and e["lag_ms"] >= 0 for e in stale3)
    assert not [e for e in eventos if e["class"] == "anomaly"]
    assert w.resumo()["GET/g3/stale"] == len(stale3)


def test_falha_numa_thread_aparece_no_parar(monkeypatch):
    def quebra(*_a, **_k):
        raise RuntimeError("sem credenciais")

    monkeypatch.setattr(workload.lab_s3, "cliente_s3", quebra)
    w = Workload(ConfigCarga(taxa_fria=0), {}, [])
    with pytest.raises(RuntimeError, match="falha numa thread"):
        w.executar_por(0.2)
