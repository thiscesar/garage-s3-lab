"""Smoke test com o cluster real (make test-integration).

Pré-requisito: `make up` já executado. O teste sempre restaura o cluster no fim (cura, religa, consistent).
"""

import uuid

import pytest

from lab import cluster, garage
from lab import s3 as lab_s3
from lab.config import NOS, modo_atual

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def api():
    a = garage.AdminAPI()
    try:
        garage.esperar_saude(NOS, limite_s=30, api=a)
    except garage.ErroGarage as e:
        pytest.fail(f"cluster não está saudável; rode `make up` antes: {e}")
    yield a
    garage.restaurar()
    a.fechar()


@pytest.fixture(scope="module")
def clientes():
    return {n: lab_s3.cliente_s3(n) for n in NOS}


def test_put_e_get_por_todos_os_nos(api, clientes):
    sufixo = uuid.uuid4().hex[:8]
    for w in NOS:
        chave = f"smoke/{sufixo}/via-{w}"
        assert lab_s3.put(clientes[w], w, chave, f"v-{w}".encode()).ok
        for r in NOS:
            res = lab_s3.get(clientes[r], r, chave)
            assert res.ok and res.dados == f"v-{w}".encode(), f"GET {chave} via garage{r}: {res.erro}"


def test_trocar_de_modo_preserva_os_objetos(api, clientes):
    """FS10: trocar de modo não apaga dados.

    Em degraded (R = 1) a leitura vai só à cópia local (C3); uma réplica que ainda não recebeu a escrita
    responde NoSuchKey (observado no laboratório). Por isso, em degraded, lê-se pelo nó que gravou;
    em consistent (R = 2), pelos 3 nós.
    """
    chave = f"smoke/modo/{uuid.uuid4().hex[:8]}"
    assert lab_s3.put(clientes[1], 1, chave, b"antes-da-troca").ok
    try:
        garage.definir_modo("degraded")
        assert modo_atual() == "degraded"
        res = lab_s3.get(lab_s3.cliente_s3(1), 1, chave)
        assert res.ok and res.dados == b"antes-da-troca", f"degraded, garage1: {res.erro}"
    finally:
        garage.definir_modo("consistent")
    for n in NOS:
        res = lab_s3.get(lab_s3.cliente_s3(n), n, chave)
        assert res.ok and res.dados == b"antes-da-troca", f"consistent, garage{n}: {res.erro}"


def test_no_isolado_continua_acessivel_pela_api_e_pela_cli(api):
    """Admin API (rede clientes) e CLI com -h 127.0.0.1 funcionam no nó particionado."""
    try:
        assert cluster.particionar(3)
        assert not cluster.na_rede_interna(3)
        assert api.health(3) in (200, 503)  # responde (o código depende de já ter detectado a falha)
        status = api.cluster_status(3)  # JSON pela rede clientes
        assert {x["hostname"] for x in status["nodes"]} >= {"garage3"}
        assert "Current cluster layout version" in garage.layout_show(3)  # CLI com -h <id>@127.0.0.1:3901
    finally:
        cluster.curar(3)
    assert cluster.na_rede_interna(3)
    assert cluster.estado(3)["ips"]["t3_interna"].endswith(".23")  # IP fixo de volta
    garage.esperar_saude(NOS, limite_s=90, api=api)
