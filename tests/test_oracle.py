"""Oráculo (Malkhi e Reiter §3.2 e §5.1, Lema 5.3): casos da lista do fim da §9.4 e bordas."""

import pytest

from lab.oracle import Oraculo

K = "e9/hot"


def oraculo_com(*escritas):
    """escritas: (versão, t_início, t_fim, ok)."""
    o = Oraculo()
    for v, ti, tf, ok in escritas:
        o.inicio_escrita(K, v, t_inicio=ti)
        if tf is not None:
            o.fim_escrita(K, v, tf, ok)
    return o


def test_leitura_depois_do_ack_com_valor_novo_e_fresh():
    o = oraculo_com((1, 0.0, 1.0, True), (2, 2.0, 3.0, True))
    c = o.classificar(K, ts=4.0, te=4.1, versao_lida=2)
    assert (c.classe, c.acked_max) == ("fresh", 2)


def test_leitura_depois_do_ack_com_valor_antigo_e_stale_com_atraso():
    o = oraculo_com((1, 0.0, 1.0, True), (2, 2.0, 3.0, True), (3, 3.5, 4.0, True))
    c = o.classificar(K, ts=5.0, te=5.1, versao_lida=1)
    assert c.classe == "stale"
    assert c.lag_versions == 2
    assert c.lag_ms == pytest.approx(2000.0)  # ts − confirmação da versão 2 (1ª depois da lida)


@pytest.mark.parametrize("lida", [1, 2])
def test_leitura_concorrente_com_a_escrita_pode_ver_anterior_ou_nova(lida):
    # escrita 2 em [2, 3]; leitura em [2.5, 2.6]: concorrentes → as duas respostas são corretas
    o = oraculo_com((1, 0.0, 1.0, True), (2, 2.0, 3.0, True))
    assert o.classificar(K, ts=2.5, te=2.6, versao_lida=lida).classe == "fresh"


def test_leitura_que_comeca_antes_do_ack_e_termina_depois():
    o = oraculo_com((1, 0.0, 1.0, True), (2, 2.0, 3.0, True))
    assert o.classificar(K, ts=2.9, te=3.2, versao_lida=1).classe == "fresh"


def test_nosuchkey_sem_escrita_confirmada_e_fresh():
    o = oraculo_com((1, 2.0, 3.0, True))
    c = o.classificar(K, ts=1.0, te=1.1, versao_lida=None, nosuchkey=True)
    assert (c.classe, c.motivo) == ("fresh", "nosuchkey")


def test_nosuchkey_com_escrita_confirmada_e_stale():
    o = oraculo_com((1, 0.0, 1.0, True))
    c = o.classificar(K, ts=2.0, te=2.1, versao_lida=None, nosuchkey=True)
    assert (c.classe, c.motivo, c.lag_versions) == ("stale", "nosuchkey", 1)
    assert c.lag_ms == pytest.approx(1000.0)


def test_erro_http_ou_timeout_e_error():
    o = oraculo_com((1, 0.0, 1.0, True))
    c = o.classificar(K, ts=2.0, te=5.0, versao_lida=None, erro="ReadTimeoutError: ...")
    assert (c.classe, c.motivo) == ("error", "ReadTimeoutError: ...")


def test_versao_de_put_que_falhou_nao_e_anomalia():
    # PUT 2 deu timeout (ok=False) mas pode ter sido aplicado; começou antes da leitura
    o = oraculo_com((1, 0.0, 1.0, True), (2, 2.0, 5.0, False))
    c = o.classificar(K, ts=6.0, te=6.1, versao_lida=2)
    assert (c.classe, c.acked_max) == ("fresh", 1)
    # e ler a 1 continua fresh: a 2 nunca foi confirmada
    assert o.classificar(K, ts=6.0, te=6.1, versao_lida=1).classe == "fresh"


def test_versoes_fora_de_ordem_cada_leitura_classificada_sozinha():
    # leitor vê 3 e depois 2 (por exemplo, réplicas diferentes): a 2ª leitura é stale
    o = oraculo_com((1, 0.0, 0.5, True), (2, 1.0, 1.5, True), (3, 2.0, 2.5, True))
    assert o.classificar(K, ts=3.0, te=3.1, versao_lida=3).classe == "fresh"
    c = o.classificar(K, ts=3.2, te=3.3, versao_lida=2)
    assert (c.classe, c.lag_versions) == ("stale", 1)


def test_escritas_registradas_fora_de_ordem():
    o = Oraculo()
    o.inicio_escrita(K, 2, t_inicio=2.0)
    o.inicio_escrita(K, 1, t_inicio=0.0)
    o.fim_escrita(K, 2, 3.0, True)
    o.fim_escrita(K, 1, 1.0, True)
    assert [e.versao for e in o.escritas(K)] == [1, 2]
    assert o.classificar(K, ts=4.0, te=4.1, versao_lida=1).classe == "stale"


def test_anomalia_versao_do_futuro():
    o = oraculo_com((1, 0.0, 1.0, True), (2, 5.0, 6.0, True))
    c = o.classificar(K, ts=2.0, te=2.1, versao_lida=2)  # a 2 só começou em 5.0
    assert (c.classe, c.motivo) == ("anomaly", "versão começou depois da leitura")


def test_anomalia_versao_nunca_escrita_e_corpo_ilegivel():
    o = oraculo_com((1, 0.0, 1.0, True))
    assert o.classificar(K, ts=2.0, te=2.1, versao_lida=7).classe == "anomaly"
    assert o.classificar(K, ts=2.0, te=2.1, versao_lida=None).classe == "anomaly"


def test_pronto_so_quando_escritas_anteriores_terminam():
    o = Oraculo()
    o.inicio_escrita(K, 1, t_inicio=1.0)
    assert not o.pronto(K, te=2.0)  # escrita 1 começou antes e ainda não terminou
    assert o.pronto(K, te=0.5)  # leitura terminou antes de a escrita começar
    o.fim_escrita(K, 1, 3.0, True)
    assert o.pronto(K, te=2.0)


def test_versao_duplicada_e_rejeitada():
    o = oraculo_com((1, 0.0, 1.0, True))
    with pytest.raises(ValueError):
        o.inicio_escrita(K, 1, t_inicio=2.0)


def test_dois_escritores_concorrentes_qualquer_ordem_e_fresh():
    # v5 via garage3 em [0.40, 0.43]; v6 via garage1 em [0.42, 0.44] (concorrentes; LWW pode manter a 5)
    o = oraculo_com((5, 0.40, 0.43, True), (6, 0.42, 0.44, True))
    assert o.classificar(K, ts=0.50, te=0.51, versao_lida=5).classe == "fresh"
    assert o.classificar(K, ts=0.50, te=0.51, versao_lida=6).classe == "fresh"


def test_escrita_que_sucede_a_lida_torna_stale_mesmo_com_dois_escritores():
    # v5 termina em 0.43; v7 começa em 0.45 (depois) e é confirmada em 0.47 → ler 5 em 0.50 é stale
    o = oraculo_com((5, 0.40, 0.43, True), (6, 0.42, 0.44, True), (7, 0.45, 0.47, True))
    c = o.classificar(K, ts=0.50, te=0.51, versao_lida=5)
    assert (c.classe, c.lag_versions) == ("stale", 1)
    assert c.lag_ms == pytest.approx(30.0)


def test_fase_por_intervalo():
    from lab.experiments.base import fase_por_intervalo

    m = {"partition_start": 10.0, "partition_at": 10.2, "heal_start": 130.0, "heal_at": 130.5}
    assert fase_por_intervalo(1.0, 9.9, m) == "baseline"
    assert fase_por_intervalo(9.9, 10.1, m) == "transicao"  # atravessa o início
    assert fase_por_intervalo(10.3, 129.9, m) == "partition"
    assert fase_por_intervalo(129.8, 130.6, m) == "transicao"  # atravessa a cura
    assert fase_por_intervalo(130.6, 131.0, m) == "heal"
