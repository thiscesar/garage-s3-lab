"""Painel sem Docker: rotas, uma ação por vez, validações, lotes do SSE e páginas sem recurso externo."""

import re
import threading
import time

import pytest
from fastapi.testclient import TestClient

from painel import app as painel_app


@pytest.fixture
def cliente():
    p = painel_app.Painel()
    p.rapido = {"agora": 0.0, "nos": {}, "modo": "consistent", "quorum": {"N": 3, "R": 2, "W": 2, "R+W>N": True}}
    app = painel_app.criar_app(p, iniciar_coletores=False)
    with TestClient(app) as c:
        yield c, p


def _evento(op, no, chave, versao, status="ok", classe=None, t=1.0):
    return {
        "t_start": t,
        "t_end": t + 0.01,
        "op": op,
        "node": no,
        "key": chave,
        "status": status,
        "class": classe,
        "version_read": versao if op == "GET" else None,
        "version_written": versao if op == "PUT" else None,
        "lag_ms": None,
        "error": None if status == "ok" else "ReadTimeoutError",
    }


def test_pagina_e_estado(cliente):
    c, _ = cliente
    r = c.get("/")
    assert r.status_code == 200 and '<canvas id="linha-do-tempo">' in r.text
    assert c.get("/static/chart.umd.min.js").text.startswith("/*!\n * Chart.js v4.4.7")
    e = c.get("/api/state").json()
    assert e["modo"] == "consistent" and e["quorum"]["R+W>N"] is True
    assert set(e["presets"]) == {"A1", "A2", "A4"} and e["ocupado"] is False


def test_uma_acao_por_vez(cliente, monkeypatch):
    c, p = cliente
    liberar = threading.Event()
    monkeypatch.setattr(painel_app.cluster, "particionar", lambda n: liberar.wait(5) or True)
    assert c.post("/api/net/3/partition").status_code == 200
    r = c.post("/api/net/3/heal")
    assert r.status_code == 409 and "particionar g3" in r.json()["detail"]
    liberar.set()
    for _ in range(50):
        if not p.estado()["ocupado"]:
            break
        time.sleep(0.02)
    assert p.acao["situacao"] == "ok" and p.estado()["ocupado"] is False
    assert [m["rotulo"] for m in p.marcos] == ["particionar g3"]


def test_erro_da_acao_aparece_no_estado(cliente, monkeypatch):
    c, p = cliente

    def falha(n):
        raise RuntimeError("docker fora do ar")

    monkeypatch.setattr(painel_app.cluster, "parar", falha)
    c.post("/api/node/2/stop")
    for _ in range(50):
        if not p.estado()["ocupado"]:
            break
        time.sleep(0.02)
    assert p.acao["situacao"] == "erro" and "docker fora do ar" in p.acao["resultado"]


@pytest.mark.parametrize(
    ("metodo", "url", "corpo", "codigo"),
    [
        ("post", "/api/net/4/partition", None, 404),
        ("post", "/api/net/3/explodir", None, 404),
        ("post", "/api/node/1/reboot", None, 404),
        ("post", "/api/mode", {"mode": "eventual"}, 422),
        ("post", "/api/repair", {"tipo": "tudo"}, 422),
        ("post", "/api/workload/start", {"preset": "A9"}, 422),
    ],
)
def test_validacoes(cliente, metodo, url, corpo, codigo):
    c, _ = cliente
    assert getattr(c, metodo)(url, json=corpo).status_code == codigo


def test_lotes_e_versoes():
    p = painel_app.Painel()
    p.chave_quente = "k"
    p._guardar(_evento("PUT", 1, "k", 5))
    p._guardar(_evento("PUT", 1, "k", 6, status="error"))  # não confirmada: não conta
    p._guardar(_evento("GET", 3, "k", 4, classe="stale"))
    p._guardar(_evento("GET", 2, "outra", 9, classe="fresh"))  # outra chave: não muda as versões
    lote, total = p.lote(0)
    assert total == 4 and [e["cls"] for e in lote] == ["ok", "error", "stale", "fresh"]
    assert p.versoes == {"confirmada": 5, "lida": {3: 4}}
    assert p.lote(total) == ([], 4)
    for i in range(painel_app.MAX_LOTE + 50):
        p._guardar(_evento("GET", 2, "k", 1, classe="fresh", t=float(i)))
    lote, total = p.lote(4)
    assert len(lote) == painel_app.MAX_LOTE and total == 4 + painel_app.MAX_LOTE + 50
    assert [e["t"] for e in lote] == sorted(e["t"] for e in lote)


def test_paginas_sem_recurso_externo():
    # a demo precisa funcionar sem internet: nada de CDN no HTML/JS/CSS do painel
    for nome in ("index.html", "app.js", "style.css"):
        texto = (painel_app.ESTATICOS / nome).read_text()
        assert not re.search(r"(src|href)=\"https?://", texto), nome
        assert "cdn" not in texto.lower(), nome


def _esperar_acao(p, limite=60.0):
    fim = time.monotonic() + limite
    while p.estado()["ocupado"] and time.monotonic() < fim:
        time.sleep(0.2)
    return p.acao


@pytest.mark.integration
def test_particao_e_cura_pela_api():
    """Cluster real: partição e cura do garage3 pelo painel; carga A1 gera eventos; estado volta ao padrão."""
    from lab import cluster, garage

    p = painel_app.Painel()
    with TestClient(painel_app.criar_app(p, iniciar_coletores=False)) as c:
        try:
            assert c.post("/api/workload/start", json={"preset": "A1"}).status_code == 200
            assert c.post("/api/net/3/partition").status_code == 200
            assert _esperar_acao(p)["situacao"] == "ok"
            assert not cluster.na_rede_interna(3)
            time.sleep(3)
            assert c.post("/api/net/3/heal").status_code == 200
            assert _esperar_acao(p)["situacao"] == "ok"
            assert cluster.na_rede_interna(3)
            resumo = c.post("/api/workload/stop").json()
            assert resumo["preset"] == "A1" and resumo["erro"] is None
            lote, total = p.lote(0)
            assert total > 0 and {e["op"] for e in lote} == {"GET", "PUT"}
        finally:
            garage.restaurar()


def test_filtro_de_logs_mantem_s3_e_tira_admin():
    s3 = "INFO garage_api_common::generic_server: 172.31.0.1:40242 (key GKcc88) PUT /sdlab/demo"
    erro = (
        "WARN garage_api_common::generic_server: error 503 Service Unavailable, Internal error: Could not reach quorum"
    )
    admin = "INFO garage_api_common::generic_server: 172.31.0.1:32850 GET /v2/GetClusterStatus"
    saude = "INFO garage_api_common::generic_server: 172.31.0.1:32850 GET /health"
    proxy = "INFO garage_api_admin::api_server: Proxied admin API request: GetClusterLayout"
    assert not painel_app.linha_do_painel(s3) and not painel_app.linha_do_painel(erro)
    assert all(painel_app.linha_do_painel(x) for x in (admin, saude, proxy))


def test_separa_requisicoes_s3_dos_outros_eventos():
    s3 = [f"INFO garage_api_common::generic_server: 172.31.0.100:5 (key GK1) PUT /sdlab/k{i}" for i in range(6)]
    outros = [f"INFO garage_net::netapp: Connection from {i} closed" for i in range(8)]
    r_s3, r_outros = painel_app.separar_logs([*s3[:3], *outros, *s3[3:]])
    assert r_s3 == s3[-4:] and r_outros == outros[-6:]
