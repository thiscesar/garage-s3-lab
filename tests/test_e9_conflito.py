"""Cenário opcional `conflito` do E9 (sem Docker): maioria e minoria escrevem a MESMA chave durante a partição."""

import pytest

from lab.experiments.e9_modos import E9, analisar_conflito

MARCOS = {"partition_start": 10.0, "partition_at": 10.1, "heal_start": 130.0, "heal_at": 130.1, "converged_at": 150.0}


def _put(no, t, v, ok=True):
    return {
        "op": "PUT",
        "node": no,
        "t_start": t,
        "t_end": t + 0.01,
        "status": "ok" if ok else "error",
        "version_written": v,
    }


def _get(no, t, v):
    return {
        "op": "GET",
        "node": no,
        "t_start": t,
        "t_end": t + 0.01,
        "status": "ok",
        "version_read": v,
        "class": "fresh",
    }


def test_escrita_da_minoria_confirmada_e_nunca_vista_fora_dela_e_descartada():
    eventos = [
        _put(1, 1.0, 1),
        _get(2, 1.5, 1),
        _get(3, 1.5, 1),  # baseline
        _put(3, 20.0, 2),  # minoria confirma v2 (dangerous)
        _get(3, 21.0, 2),  # só a própria minoria vê v2
        _put(3, 30.0, 4),  # minoria confirma v4 (nunca vista)
        _put(1, 25.0, 3),
        _get(2, 26.0, 3),  # maioria segue no seu ramo
        _put(3, 40.0, 5, ok=False),  # tentativa sem confirmação
        _put(1, 140.0, 6),
        _get(2, 141.0, 6),
        _get(3, 142.0, 6),  # após a cura
    ]
    r = analisar_conflito(eventos, MARCOS, {"g1": 6, "g2": 6, "g3": 6})
    assert r["particao"] == {"maj_tentativas": 1, "maj_ok": 1, "min_tentativas": 3, "min_ok": 2}
    assert r["min_ok_vistas_pela_maioria"] == 0
    assert r["min_descartadas"] == [2, 4]  # confirmadas, nunca lidas pela maioria e não são o valor final
    assert r["final"] == {"versao": 6, "escritor": "maioria", "nos_convergidos": 3}


def test_sem_escrita_na_minoria_nada_descartado():
    eventos = [_put(1, 1.0, 1), _put(3, 20.0, 2, ok=False), _put(1, 25.0, 3), _get(2, 26.0, 3)]
    r = analisar_conflito(eventos, MARCOS, {"g1": 3, "g2": 3, "g3": 3})
    assert r["particao"]["min_ok"] == 0 and r["min_descartadas"] == []


def test_cenario_valido(tmp_path):
    with pytest.raises(ValueError, match="cenário inválido"):
        E9(base=tmp_path, cenario="outro")
    assert E9(base=tmp_path, cenario="conflito").cenario == "conflito"


def test_analyze_do_cenario_conflito_le_os_ops(tmp_path):
    import json

    e = E9(reps=1, base=tmp_path, cenario="conflito", modo="dangerous")
    e.dir.mkdir(parents=True)
    evs = [_put(1, 1.0, 1), _put(3, 20.0, 2), _put(1, 25.0, 3), _get(2, 26.0, 3), _put(1, 140.0, 4)]
    (e.dir / "ops_rep0_dangerous.jsonl").write_text("\n".join(json.dumps(x) for x in evs) + "\n")
    rodada = {"marcos": MARCOS, "verificacao": {"final_por_no": {"g1": 4, "g2": 4, "g3": 4}}}
    e.repeticoes = [{"rep": 0, "situacao": "ok", "resultado": {"rodadas": {"dangerous": rodada}}}]
    a = e.analyze()
    assert a["resultados"] == []
    tabela = a["secoes"][0][1]
    assert "| 0 | dangerous | 1/1 | 1/1 | 0 | 1 | v4 (maioria, 3/3 nós) |" in tabela
