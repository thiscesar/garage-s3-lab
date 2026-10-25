"""Parsers das métricas Prometheus do Garage (lab.garage), com amostra real da v2.3.0 (E2)."""

from pathlib import Path

from lab.garage import contadores_rpc, metrica

AMOSTRA = (Path(__file__).parent / "fixtures" / "metrics_v2.3.0.txt").read_text()


def test_contadores_rpc_le_from_to_endpoint():
    c = contadores_rpc(AMOSTRA)
    assert c, "a amostra deve ter rpc_request_counter"
    for (de, para, endpoint), valor in c.items():
        assert len(de) == 16 and len(para) == 16
        assert endpoint.startswith(("garage_table/", "garage_block/"))
        assert valor >= 0


def test_contadores_rpc_ignora_comentarios_e_outras_metricas():
    texto = '# TYPE rpc_request_counter counter\nrpc_duration_sum{x="y"} 3\nrpc_request_counter{from="a",rpc_endpoint="e",to="b"} 7\n'
    assert contadores_rpc(texto) == {("a", "b", "e"): 7.0}


def test_metrica_sem_rotulos():
    assert metrica(AMOSTRA, "block_bytes_read") is not None
    assert metrica(AMOSTRA, "block_bytes_written") is not None
    assert metrica(AMOSTRA, "nao_existe") is None
    assert metrica("block_bytes_read 4194452\n", "block_bytes_read") == 4194452.0
