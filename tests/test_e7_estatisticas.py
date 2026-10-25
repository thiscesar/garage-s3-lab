"""Parser de GetNodeStatistics e critério de convergência do E7 (amostra real em tests/fixtures/)."""

import copy
from pathlib import Path

from lab.experiments.e7_recuperacao import E7, ler_estatisticas_no

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
    e["nos"]["garage3"]["blocos"]["resync"] = 1
    assert not E7.convergido(e)[0]
    e = _estado()
    e["du"]["garage3"] = 1000 + 200 * 1024
    assert not E7.convergido(e)[0]
