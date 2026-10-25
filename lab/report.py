"""Relatório de uma execução: summary.md com previsão × resultado (roteiro §4.2 e §10).

A previsão vem de `predicao.yaml` (cópia de `predicoes/<exp>.yaml` no momento da execução):

    experimento: E1
    roteiro: "§7.3 E1"
    previsoes:
      - id: P1
        afirmacao: "..."
        fundamento: "..."
        fontes: ["..."]
    perguntas_guia: ["..."]

O experimento devolve em `analyze()` uma lista de {id, resultado, situacao, evidencia} com
situacao ∈ {confirmada, refutada, inconclusiva}, e opcionalmente seções extras de Markdown.
"""

from __future__ import annotations

import json
from pathlib import Path

import yaml

SITUACOES = {"confirmada": "✅ confirmada", "refutada": "❌ refutada", "inconclusiva": "⚠️ inconclusiva"}


def ler_previsao(dir_execucao: Path) -> dict:
    arq = dir_execucao / "predicao.yaml"
    return yaml.safe_load(arq.read_text()) if arq.exists() else {}


def _celula(texto: object) -> str:
    return str(texto).replace("|", "\\|").replace("\n", " ")


def gerar_summary(dir_execucao: Path, analise: dict | None) -> Path:
    """Escreve summary.md a partir do manifest, da previsão e da análise do experimento."""
    manifest = json.loads((dir_execucao / "manifest.json").read_text())
    previsao = ler_previsao(dir_execucao)
    analise = analise or {}
    resultados = {r["id"]: r for r in analise.get("resultados", [])}

    linhas = [f"# {manifest['experimento']} — {manifest.get('descricao', '')}", ""]
    if manifest.get("quick"):
        linhas += [
            "> **Execução `--quick` (desenvolvimento):** durações e repetições reduzidas; não usar no relatório.",
            "",
        ]
    reps = manifest.get("repeticoes", [])
    ok = sum(1 for r in reps if r.get("situacao") == "ok")
    linhas += [
        f"- Início: {manifest.get('inicio')} · fim: {manifest.get('fim')}",
        (
            f"- Modo: `{manifest.get('modo')}` · cenário: {manifest.get('cenario') or '—'} · "
            f"repetições: {ok}/{len(reps)} ok"
        ),
        f"- Commit: `{(manifest.get('git') or {}).get('commit')}`"
        + (" (com alterações locais)" if (manifest.get("git") or {}).get("alteracoes_locais") else ""),
        (
            f"- Garage {', '.join(manifest['versoes'].get('garage') or ['?'])} · Docker {manifest['versoes'].get('docker')}"
            f" · Python {manifest['versoes'].get('python')} · boto3 {manifest['versoes'].get('boto3')}"
        ),
        f"- Redes: interna {manifest['redes']['interna']}, clientes {manifest['redes']['clientes']}",
        "",
    ]
    if manifest.get("precondicao"):
        linhas += [f"> **Pré-condição:** {manifest['precondicao']}", ""]
    erros = [r for r in reps if r.get("situacao") == "erro"]
    if erros:
        linhas += ["## Repetições com erro", ""]
        linhas += [f"- rep {r['rep']}: `{r.get('erro')}`" for r in erros]
        linhas.append("")

    linhas += ["## Previsão × resultado", ""]
    if previsao:
        linhas += [
            f"Roteiro: {previsao.get('roteiro', '—')}",
            "",
        ]
        linhas += ["| # | Previsão | Fundamento | Resultado | Situação |", "|---|---|---|---|---|"]
        for p in previsao.get("previsoes", []):
            r = resultados.get(p["id"], {})
            fundamento = p.get("fundamento", "")
            if p.get("fontes"):
                fundamento += " (" + "; ".join(p["fontes"]) + ")"
            linhas.append(
                f"| {p['id']} | {_celula(p.get('afirmacao', ''))} | {_celula(fundamento)} | "
                f"{_celula(r.get('resultado', '— (não medido)'))} | "
                f"{SITUACOES.get(r.get('situacao'), '—')} |"
            )
        linhas.append("")
    else:
        linhas += ["_Sem `predicao.yaml` nesta execução._", ""]

    for titulo, corpo in analise.get("secoes", []):
        linhas += [f"## {titulo}", "", corpo.rstrip(), ""]

    if analise.get("figuras"):
        linhas += ["## Figuras", ""]
        linhas += [f"![{f}](figures/{f}.png)" for f in analise["figuras"]]
        linhas.append("")

    linhas += ["## Análise (a preencher pela equipe)", ""]
    for pergunta in previsao.get("perguntas_guia", []):
        linhas += [f"- {pergunta}", "  - _resposta:_ "]
    linhas.append("")

    arq = dir_execucao / "summary.md"
    arq.write_text("\n".join(linhas))
    return arq


def tabela_md(cabecalho: list[str], linhas: list[list[object]]) -> str:
    saida = ["| " + " | ".join(cabecalho) + " |", "|" + "---|" * len(cabecalho)]
    saida += ["| " + " | ".join(_celula(c) for c in linha) + " |" for linha in linhas]
    return "\n".join(saida)
