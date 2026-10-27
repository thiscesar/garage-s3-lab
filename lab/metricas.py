"""Métricas calculadas a partir dos `ops*.jsonl` já gravados (sem reexecutar nem alterar `results/`).

Tempo sem resposta (B8): a disponibilidade do E6 e do E9 é medida por tentativa, mas cada
cliente espera a resposta antes da próxima operação (`Workload._ritmo`). Um tempo limite de 3 s "ocupa" 3 s e gera
uma tentativa só, enquanto um erro de quórum responde em milissegundos; por tentativa, as duas falhas pesam igual.
Esta métrica complementa a disponibilidade por tentativa (não a substitui): fração da fase coberta pela união dos
intervalos [t_start, t_end] das operações que não tiveram resposta, por papel (operação, nó). Erros rápidos (sem
quórum, depois da detecção) cobrem pouco tempo: a métrica mede a espera do cliente, não a indisponibilidade lógica.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

_ARQ_FASES = re.compile(r"^phases_rep(\d+)(?:_(\w+))?\.json$")


def uniao(intervalos: list[tuple[float, float]], inicio: float, fim: float) -> float:
    """Comprimento da união dos intervalos, recortados a [inicio, fim]."""
    recortados = sorted((max(a, inicio), min(b, fim)) for a, b in intervalos if b > inicio and a < fim)
    total, atual_a, atual_b = 0.0, None, None
    for a, b in recortados:
        if atual_b is None or a > atual_b:
            if atual_b is not None:
                total += atual_b - atual_a
            atual_a, atual_b = a, b
        else:
            atual_b = max(atual_b, b)
    if atual_b is not None:
        total += atual_b - atual_a
    return round(total, 4)


def _respondeu(ev: dict) -> bool:
    """Mesma regra do E9: GET classificado fresh/stale respondeu (NoSuchKey incluído); demais, status ok."""
    if ev["op"] == "GET" and ev.get("class") is not None:
        return ev["class"] in ("fresh", "stale")
    return ev["status"] == "ok"


def tempo_sem_resposta(
    eventos: list[dict], marcos: dict, inicio: str = "partition_at", fim: str = "heal_start"
) -> dict:
    """Por (op, nó): tentativas, janela, segundos sem resposta e fração, na fase [marcos[inicio], marcos[fim]]."""
    a, b = marcos[inicio], marcos[fim]
    grupos: dict[tuple, dict] = {}
    for ev in eventos:
        if ev["t_end"] <= a or ev["t_start"] >= b:
            continue
        g = grupos.setdefault((ev["op"], ev["node"]), {"tentativas": 0, "intervalos": []})
        g["tentativas"] += 1
        if not _respondeu(ev):
            g["intervalos"].append((ev["t_start"], ev["t_end"]))
    janela = round(b - a, 4)
    saida = {}
    for chave, g in grupos.items():
        sem = uniao(g["intervalos"], a, b)
        saida[chave] = {
            "tentativas": g["tentativas"],
            "janela_s": janela,
            "sem_resposta_s": sem,
            "fracao": round(sem / janela, 4) if janela > 0 else None,
        }
    return saida


def de_execucao(dir_execucao: Path) -> list[dict]:
    """Lê `phases_rep<i>[_<modo>].json` e o `ops_rep<i>[_<modo>].jsonl` correspondente (E6, E9) e calcula a métrica
    na fase de partição de cada repetição/rodada. Só lê os arquivos."""
    linhas = []
    for arq in sorted(Path(dir_execucao).glob("phases_rep*.json")):
        m = _ARQ_FASES.match(arq.name)
        if not m:
            continue
        rep, modo = int(m.group(1)), m.group(2)
        ops = arq.with_name(f"ops_rep{rep}{'_' + modo if modo else ''}.jsonl")
        if not ops.exists():
            continue
        marcos = json.loads(arq.read_text())
        eventos = [json.loads(x) for x in ops.read_text().splitlines() if x.strip()]
        for (op, no), v in sorted(tempo_sem_resposta(eventos, marcos).items()):
            linhas.append({"rep": rep, "modo": modo, "op": op, "no": no, **v})
    return linhas


def tabela_por_papel(linhas: list[dict], papeis: dict[tuple, str] | None = None) -> list[list]:
    """Agrega `de_execucao` por (modo, op, nó): média ± desvio da fração sem resposta entre as repetições."""
    from lab import stats

    grupos: dict[tuple, list[dict]] = {}
    for x in linhas:
        grupos.setdefault((x["modo"] or "—", x["op"], x["no"]), []).append(x)
    saida = []
    for (modo, op, no), xs in sorted(grupos.items()):
        s = stats.resumir([x["fracao"] * 100 for x in xs])
        desvio = 0 if s.n < 2 else s.desvio
        papel = (papeis or {}).get((op, no), f"{op} g{no}")
        segundos = sum(x["sem_resposta_s"] for x in xs) / len(xs)
        saida.append([modo, papel, s.n, f"{s.media:.1f} ± {desvio:.1f}%", f"{segundos:.1f}"])
    return saida
