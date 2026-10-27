"""`labctl report` sem Docker: escolha da execução de referência, cópia para final/ e RELATORIO_DADOS.md."""

import json

import pytest
import yaml

from lab import report


def _execucao(base, exp, nome, quick=False, reps_ok=5, reps=5, fim=True, situacoes=("confirmada", "refutada"), **extra):
    d = base / exp / nome
    (d / "figures").mkdir(parents=True)
    (d / "figures" / "fig.png").write_bytes(b"png")
    resultados = [{"id": f"P{i + 1}", "resultado": f"medido {i + 1}", "situacao": s} for i, s in enumerate(situacoes)]
    manifest = {
        "experimento": exp,
        "descricao": f"descrição {exp}",
        "quick": quick,
        "inicio": "2026-10-25T10:00:00-03:00",
        "fim": "2026-10-25T10:05:00-03:00" if fim else None,
        "git": {"commit": "abcdef1234"},
        "versoes": {"garage": ["v2.3.0"], "docker": "29", "compose": "5", "python": "3.13", "boto3": "1.43"},
        "host": {"cpu": "cpu", "nucleos": 16, "ram_bytes": 2**34, "so": "Linux", "kernel": "7"},
        "redes": {"interna": "10.30.0.0/24", "clientes": "172.31.0.0/24"},
        "repeticoes": [{"rep": i, "situacao": "ok" if i < reps_ok else "erro"} for i in range(reps)],
        "analise": {
            "resultados": resultados,
            "secoes": [["Tabela", "| a |\n|---|\n| 1 |\n## Subtítulo"]],
            "figuras": ["fig"],
        },
        **extra,
    }
    (d / "manifest.json").write_text(json.dumps(manifest))
    (d / "summary.md").write_text("# summary")
    (d / "predicao.yaml").write_text(
        yaml.safe_dump(
            {"previsoes": [{"id": "P1", "afirmacao": "previsto 1"}, {"id": "P2", "afirmacao": "previsto 2"}]}
        )
    )
    return d


def test_escolhe_a_mais_recente_valida(tmp_path):
    _execucao(tmp_path, "E1", "2026-10-25T01-00-00")
    _execucao(tmp_path, "E1", "2026-10-25T02-00-00", quick=True)  # --quick nunca entra
    _execucao(tmp_path, "E1", "2026-10-25T03-00-00", reps_ok=4)  # repetição com erro
    _execucao(tmp_path, "E1", "2026-10-25T04-00-00", fim=False)  # interrompida
    esc = report.escolher_referencias(tmp_path)
    assert esc["E1"].name == "2026-10-25T01-00-00"
    assert esc["E2"] is None


def test_cenario_ou_modo_restrito_nunca_viram_referencia(tmp_path):
    # Execuções com cenário alternativo (ex.: E7 natural-primeiro, E9 conflito) ou com --mode (E9 só um modo)
    # não substituem a execução de referência, mesmo sendo mais recentes e completas.
    _execucao(tmp_path, "E7", "2026-10-25T01-00-00", cenario=None)
    _execucao(tmp_path, "E7", "2026-10-27T01-00-00", cenario="natural-primeiro")
    _execucao(tmp_path, "E9", "2026-10-25T01-00-00", parametros={"modos": ["consistent", "degraded", "dangerous"]})
    _execucao(tmp_path, "E9", "2026-10-27T01-00-00", parametros={"modos": ["degraded"]})
    esc = report.escolher_referencias(tmp_path)
    assert esc["E7"].name == "2026-10-25T01-00-00"
    assert esc["E9"].name == "2026-10-25T01-00-00"


def test_usar_escolhe_e_valida(tmp_path):
    _execucao(tmp_path, "E5", "2026-10-25T01-00-00")
    _execucao(tmp_path, "E5", "2026-10-25T02-00-00")
    _execucao(tmp_path, "E5", "2026-10-25T03-00-00", quick=True)
    assert report.escolher_referencias(tmp_path, {"e5": "2026-10-25T01-00-00"})["E5"].name == "2026-10-25T01-00-00"
    with pytest.raises(ValueError, match="não é uma execução completa"):
        report.escolher_referencias(tmp_path, {"E5": "2026-10-25T03-00-00"})


def test_gera_relatorio_e_copia_para_final(tmp_path):
    _execucao(tmp_path, "E1", "2026-10-25T01-00-00", situacoes=("confirmada", "confirmada"))
    _execucao(tmp_path, "E4", "2026-10-25T01-00-00")
    (tmp_path / "final" / "E9").mkdir(parents=True)  # sobra antiga: some
    arq = report.gerar_relatorio(tmp_path)
    texto = arq.read_text()
    assert (tmp_path / "final" / "E1" / "figures" / "fig.png").exists()
    assert not (tmp_path / "final" / "E9").exists()
    assert "**Total: 3 de 4 previsões confirmadas.**" in texto
    assert "P2 refutada" in texto and "**sem execução válida**" in texto
    assert "| P1 | previsto 1 | medido 1 | ✅ confirmada |" in texto
    assert "### Subtítulo" in texto  # títulos da análise um nível abaixo
    assert "![fig](final/E4/figures/fig.png)" in texto
    assert "10.30.0.0/24" in texto and "abcdef1" in texto
