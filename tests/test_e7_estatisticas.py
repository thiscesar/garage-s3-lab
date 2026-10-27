"""Parser de GetNodeStatistics e critério de convergência do E7 (amostra real em tests/fixtures/)."""

import copy
from pathlib import Path

from lab.experiments.e7_recuperacao import E7
from lab.garage import ler_estatisticas_no

AMOSTRA = (Path(__file__).parent / "fixtures" / "node_statistics_v2.3.0.txt").read_text()


def test_le_tabelas_e_blocos_da_saida_real():
    e = ler_estatisticas_no(AMOSTRA)
    for t in ("object", "version", "block_ref", "bucket_v2"):
        assert set(e["tabelas"][t]) == {"Items", "MklItems", "MklTodo", "InsQueue", "GcTodo"}
    assert e["blocos"].keys() >= {"rc", "resync", "erros"}


def _estado(itens=10, rc=5, du=1000):
    no = {
        "tabelas": {
            t: {"Items": itens, "MklItems": itens, "MklTodo": 0, "InsQueue": 0, "GcTodo": 0}
            for t in ("object", "version", "block_ref")
        },
        "blocos": {"rc": rc, "resync": 0, "erros": 0},
    }
    return {"nos": {f"garage{n}": copy.deepcopy(no) for n in (1, 2, 3)}, "du": {f"garage{n}": du for n in (1, 2, 3)}}


def test_convergencia_exige_contagens_filas_e_disco_iguais():
    assert E7.convergido(_estado())[0]
    e = _estado()
    e["nos"]["garage3"]["tabelas"]["object"]["Items"] = 9
    ok, motivos = E7.convergido(e)
    assert not ok and any("object" in m for m in motivos)
    e = _estado()
    e["nos"]["garage2"]["tabelas"]["version"]["MklTodo"] = 3
    assert not E7.convergido(e)[0]
    e = _estado()
    e["nos"]["garage3"]["blocos"]["resync"] = 50  # conferência agendada de blocos novos: não é divergência
    assert E7.convergido(e)[0]
    e = _estado()
    e["du"]["garage3"] = 1000 + 200 * 1024
    assert not E7.convergido(e)[0]


def _ciclo(tipo, t_conv, desde=None):
    falta = _estado()
    falta["nos"]["garage3"]["tabelas"]["object"]["Items"] = 5
    return {
        "tipo": tipo,
        "escritos": 200,
        "t_malha_s": 20.0,
        "antes_cura": falta,
        "t_convergencia_s": t_conv,
        "t_tabelas_s": t_conv,
        "limite_s": 1500,
        "final": _estado(),
        "desde_ultimo_reparo_s": desde,
    }


def test_cenario_natural_primeiro_ordem_e_tempo_desde_reparo(tmp_path):
    import pytest

    with pytest.raises(ValueError, match="cenário inválido"):
        E7(base=tmp_path, cenario="outro")
    e = E7(reps=1, base=tmp_path, cenario="natural-primeiro")
    assert e.ordem == ("natural", "reparo") and e.parametros["ordem"] == ["natural", "reparo"]
    e.dir.mkdir(parents=True)
    e.repeticoes = [
        {
            "rep": 0,
            "situacao": "ok",
            "resultado": {"natural": _ciclo("natural", 300.0), "reparo": _ciclo("reparo", 30.0)},
        }
    ]
    a = e.analyze()
    ciclos = dict(a["secoes"])["Ciclos"]
    assert ciclos.index("| 0 | natural |") < ciclos.index("| 0 | reparo |")  # natural antes do reparo
    nota = dict(a["secoes"])["Ordem dos ciclos e efeito do reparo"]
    assert "natural → reparo" in nota and "repair.rs l. 43–47" in nota and "sync.rs l. 520" in nota
    assert E7(base=tmp_path).ordem == ("reparo", "natural")  # referência
