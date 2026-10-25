"""report.gerar_summary e o parser de texto do E1 (amostra real em tests/fixtures/)."""

import json
from pathlib import Path

import yaml

from lab.experiments.e1_topologia import _particoes_por_no
from lab.report import gerar_summary

FIXTURES = Path(__file__).parent / "fixtures"


def test_particoes_por_no_na_saida_real_v230():
    texto = (FIXTURES / "cluster_statistics_v2.3.0.txt").read_text()
    assert _particoes_por_no(texto) == {"garage1": 256, "garage2": 256, "garage3": 256}


def test_summary_tem_previsao_resultado_e_analise(tmp_path):
    manifest = {
        "experimento": "EX",
        "descricao": "teste",
        "quick": True,
        "modo": "consistent",
        "cenario": None,
        "inicio": "i",
        "fim": "f",
        "git": {"commit": "abc", "alteracoes_locais": False},
        "versoes": {"garage": ["v2.3.0"], "docker": "29", "python": "3.13", "boto3": "1.43"},
        "redes": {"interna": "10.30.0.0/24", "clientes": "172.31.0.0/24"},
        "repeticoes": [{"rep": 0, "situacao": "ok"}, {"rep": 1, "situacao": "erro", "erro": "X: y"}],
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    (tmp_path / "predicao.yaml").write_text(
        yaml.safe_dump(
            {
                "experimento": "EX",
                "roteiro": "§7.3",
                "previsoes": [
                    {"id": "P1", "afirmacao": "a | b", "fundamento": "f", "fontes": ["s1"]},
                    {"id": "P2", "afirmacao": "c", "fundamento": "g"},
                ],
                "perguntas_guia": ["Por quê?"],
            },
            allow_unicode=True,
        )
    )
    analise = {
        "resultados": [{"id": "P1", "resultado": "r1", "situacao": "confirmada"}],
        "secoes": [("Extra", "texto extra")],
        "figuras": ["fig"],
    }
    md = gerar_summary(tmp_path, analise).read_text()
    assert "--quick" in md and "repetições: 1/2 ok" in md and "rep 1: `X: y`" in md
    assert "| P1 | a \\| b | f (s1) | r1 | ✅ confirmada |" in md
    assert "| P2 | c | g | — (não medido) | — |" in md
    assert "## Extra" in md and "![fig](figures/fig.png)" in md
    assert "## Análise (a preencher pela equipe)" in md and "- Por quê?" in md
