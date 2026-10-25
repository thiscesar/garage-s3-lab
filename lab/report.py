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
import shutil
import subprocess
from datetime import datetime
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


# ---------------------------------------------------------------- relatório consolidado

EXPERIMENTOS_RELATORIO = ("E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9")


def _manifest(dir_execucao: Path) -> dict | None:
    try:
        return json.loads((dir_execucao / "manifest.json").read_text())
    except (OSError, ValueError):
        return None


def execucao_valida(dir_execucao: Path) -> bool:
    """Completa (não --quick), terminada, com todas as repetições ok e com análise."""
    m = _manifest(dir_execucao)
    if not m or m.get("quick") or not m.get("fim") or not m.get("analise"):
        return False
    reps = m.get("repeticoes") or []
    return bool(reps) and all(r.get("situacao") == "ok" for r in reps)


def escolher_referencias(base: Path, usar: dict[str, str] | None = None) -> dict[str, Path | None]:
    """Execução de referência de cada experimento: a mais recente válida, salvo escolha explícita em `usar`.

    `usar` = {"E5": "2026-10-25T14-21-46"}; a escolhida também precisa ser válida (nunca uma --quick).
    """
    usar = {k.upper(): v for k, v in (usar or {}).items()}
    escolha: dict[str, Path | None] = {}
    for exp in EXPERIMENTOS_RELATORIO:
        if exp in usar:
            d = base / exp / usar[exp]
            if not execucao_valida(d):
                raise ValueError(f"{exp}: {usar[exp]} não existe ou não é uma execução completa e válida")
            escolha[exp] = d
            continue
        candidatas = sorted((d for d in (base / exp).glob("*") if d.is_dir()), reverse=True)
        escolha[exp] = next((d for d in candidatas if execucao_valida(d)), None)
    return escolha


def rastrear_commit(sha: str | None, raiz: Path | None = None) -> str:
    """Commit da execução como texto: se ele não está no histórico atual (mensagens reescritas),
    aponta o commit do histórico com a mesma árvore de arquivos."""
    if not sha:
        return "?"

    def git(*args: str) -> str | None:
        r = subprocess.run(["git", *args], capture_output=True, text=True, check=False, cwd=raiz)
        return r.stdout.strip() if r.returncode == 0 else None

    curto = sha[:7]
    if git("merge-base", "--is-ancestor", sha, "HEAD") is not None:
        return curto
    arvore = git("rev-parse", f"{sha}^{{tree}}")
    for linha in (git("log", "HEAD", "--format=%h %T") or "").splitlines():
        h, t = linha.split()
        if t == arvore:
            return f"{curto} (= {h} no histórico atual)"
    return f"{curto} (fora do histórico atual)"


def _commit_texto(m: dict, raiz: Path | None = None) -> str:
    g = m.get("git") or {}
    return rastrear_commit(g.get("commit"), raiz) + (" **com alterações locais**" if g.get("alteracoes_locais") else "")


def _placar(m: dict) -> tuple[int, int, list[str]]:
    res = (m.get("analise") or {}).get("resultados") or []
    ok = sum(1 for r in res if r.get("situacao") == "confirmada")
    outras = [f"{r['id']} {r.get('situacao')}" for r in res if r.get("situacao") != "confirmada"]
    return ok, len(res), outras


def _rebaixar_titulos(md: str) -> str:
    """Seções do summary entram um nível abaixo (### → ####) no relatório consolidado."""
    return "\n".join("#" + linha if linha.startswith("#") else linha for linha in md.splitlines())


def gerar_relatorio(base: Path, usar: dict[str, str] | None = None) -> Path:
    """Copia as execuções de referência para `results/final/<E>/` e escreve `results/RELATORIO_DADOS.md`."""
    escolha = escolher_referencias(base, usar)
    final = base / "final"
    final.mkdir(parents=True, exist_ok=True)
    manifests: dict[str, dict] = {}
    for exp, d in escolha.items():
        destino = final / exp
        if destino.exists():
            shutil.rmtree(destino)
        if d is not None:
            shutil.copytree(d, destino)
            manifests[exp] = _manifest(destino)

    linhas = [
        "# Relatório de dados — Laboratório Garage S3 (Tema 3, Ênfase A)",
        "",
        (
            f"> Gerado por `labctl report` em {datetime.now().astimezone().isoformat(timespec='seconds')}. "
            "Cada experimento usa a sua execução de referência (a mais recente completa, sem `--quick`, com todas as "
            "repetições ok, salvo escolha com `--usar`), copiada para `final/<experimento>/`. Fatos sobre o Garage e "
            "a análise de cada resultado ficam nos documentos de apoio; aqui estão só os dados."
        ),
        "",
        "## Placar das previsões",
        "",
    ]
    tab, tot_ok, tot = [], 0, 0
    for exp in EXPERIMENTOS_RELATORIO:
        m = manifests.get(exp)
        if m is None:
            tab.append([exp, "—", "**sem execução válida**", "—", "—", "—"])
            continue
        ok, n, outras = _placar(m)
        tot_ok, tot = tot_ok + ok, tot + n
        reps = m.get("repeticoes") or []
        tab.append(
            [
                exp,
                m.get("descricao", ""),
                f"[`{escolha[exp].name}`](final/{exp}/summary.md)",
                len(reps),
                f"{ok}/{n}",
                ", ".join(outras) or "—",
            ]
        )
    linhas += [
        tabela_md(["exp.", "descrição", "execução", "repetições", "confirmadas", "não confirmadas"], tab),
        "",
        f"**Total: {tot_ok} de {tot} previsões confirmadas.**",
        "",
    ]

    primeiro = next(iter(manifests.values()), None)
    if primeiro:
        v, h, r = primeiro.get("versoes", {}), primeiro.get("host", {}), primeiro.get("redes", {})
        linhas += [
            "## Ambiente (dos manifests)",
            "",
            tabela_md(
                ["item", "valor"],
                [
                    ["Garage", ", ".join(v.get("garage") or ["?"])],
                    ["Docker / Compose", f"{v.get('docker')} / {v.get('compose')}"],
                    ["Python / boto3", f"{v.get('python')} / {v.get('boto3')}"],
                    [
                        "host",
                        f"{h.get('cpu')} · {h.get('nucleos')} núcleos · {(h.get('ram_bytes') or 0) / 2**30:.0f} GiB",
                    ],
                    ["SO / kernel", f"{h.get('so')} / {h.get('kernel')}"],
                    ["redes", f"interna {r.get('interna')} · clientes {r.get('clientes')}"],
                ],
            ),
            "",
            (
                'Commit de cada execução ("com alterações locais" = havia arquivos não commitados quando ela rodou; '
                "o commit sozinho não reproduz exatamente aquele código):"
            ),
            "",
            tabela_md(["exp.", "commit"], [[exp, _commit_texto(m, base)] for exp, m in manifests.items()]),
            "",
        ]

    previsoes_por_exp = {exp: ler_previsao(final / exp) for exp in manifests}
    for exp in EXPERIMENTOS_RELATORIO:
        m = manifests.get(exp)
        if m is None:
            continue
        analise = m.get("analise") or {}
        afirm = {p["id"]: p.get("afirmacao", "") for p in previsoes_por_exp[exp].get("previsoes", [])}
        linhas += [
            f"## {exp} — {m.get('descricao', '')}",
            "",
            (
                f"Execução `{escolha[exp].name}` · início {m.get('inicio')} · fim {m.get('fim')} · "
                f"{len(m.get('repeticoes') or [])} repetições · commit {_commit_texto(m, base)} · "
                f"[summary.md](final/{exp}/summary.md)"
            ),
            "",
            tabela_md(
                ["#", "previsão", "resultado", "situação"],
                [
                    [r["id"], afirm.get(r["id"], ""), r.get("resultado", ""), SITUACOES.get(r.get("situacao"), "—")]
                    for r in analise.get("resultados", [])
                ],
            ),
            "",
        ]
        for titulo, corpo in analise.get("secoes", []):
            linhas += [f"### {titulo}", "", _rebaixar_titulos(corpo.rstrip()), ""]
        if analise.get("figuras"):
            linhas += [f"![{f}](final/{exp}/figures/{f}.png)" for f in analise["figuras"]] + [""]

    arq = base / "RELATORIO_DADOS.md"
    arq.write_text("\n".join(linhas))
    return arq
