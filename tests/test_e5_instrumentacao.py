"""Instrumentação opcional do E5 (cenário `instrumentado`, sem Docker): eventos de conexão nos logs do Garage."""

import pytest

from lab.experiments.e5_falha_quorum import E5, eventos_de_conexao

LOGS = (
    "2026-10-27T19:02:49.538706117Z \x1b[2m2026-10-27T19:02:49.538642Z\x1b[0m  INFO garage_net::netapp: "
    "Connection from a357c87c3b88cac1 closed\n"
    "2026-10-27T19:03:00.100000000Z 2026-10-27T19:03:00.099000Z  WARN garage_net::peering: Too many failed pings "
    "from 0b63ad6b80851f29, closing connection.\n"
    "2026-10-27T19:03:01.200000000Z 2026-10-27T19:03:01.199000Z  INFO garage_net::netapp: Connection to "
    "6cbb0b6ef73960c8 closed\n"
    "2026-10-27T19:03:02.000000000Z 2026-10-27T19:03:02.000000Z  INFO garage_api: GET /sdlab/x\n"
)


def test_eventos_de_conexao_so_dos_nos_do_cluster():
    ev = eventos_de_conexao(LOGS, ["0b63ad6b80851f29", "6cbb0b6ef73960c8", "760fb39ebdb45d2f"])
    assert [e["texto"][:20] for e in ev] == ["Too many failed ping", "Connection to 6cbb0b"]
    assert ev[0]["t_utc"] == "2026-10-27T19:03:00.100000"


def test_cenario_valido(tmp_path):
    with pytest.raises(ValueError, match="cenário inválido"):
        E5(base=tmp_path, cenario="outro")
    assert E5(base=tmp_path, cenario="instrumentado").instrumentado
    assert not E5(base=tmp_path).instrumentado


def test_secao_da_instrumentacao():
    ins = {
        "paradas": [
            {"no": 3, "inicio_utc": "2026-10-27T19:02:58.000000", "duracao_s": 10.5},
            {"no": 2, "inicio_utc": "2026-10-27T19:03:30.000000", "duracao_s": 0.4},
        ],
        "falhas_estado_B": [{"no": 1, "inicio_utc": "2026-10-27T19:03:05.000000", "duracao_s": 3.0, "erro": "Read"}],
        "eventos_conexao": {"garage1": [{"t_utc": "2026-10-27T19:03:00.100000", "texto": "Connection to x closed"}]},
    }
    secoes = E5._secao_instrumentacao([{"consistent": {"instrumentacao": ins}}])
    assert secoes and "| 0 | consistent | g3 10.5 s; g2 0.4 s | 1 | +2.1 s: Connection to x closed |" in secoes[0][1]
    assert E5._secao_instrumentacao([{"consistent": {}}]) == []
