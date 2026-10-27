"""Análise do E4 com dados sintéticos (sem Docker): taxas por nó, listagem e atraso das desatualizadas."""

from lab.config import MODOS
from lab.experiments.e4_consistencia import E4


def _rep(stale_dangerous=2, erros_lista=0, sem_o_novo=0, listagens=10):
    r = {}
    for m in MODOS:
        stale = stale_dangerous if m == "dangerous" else 0
        exemplos = [
            {"no": 3, "confirmada": k + 2, "lida": k + 1, "motivo": None, "lag_ms": 2.0 * (k + 1)} for k in range(stale)
        ]
        r[m] = {
            "sobrescritas": {
                "contagem": {
                    2: {"fresh": 100, "stale": 0, "error": 0, "anomaly": 0},
                    3: {"fresh": 100 - stale, "stale": stale, "error": 0, "anomaly": 0},
                },
                "exemplos_stale": exemplos,
            },
            "listagem": {
                "listagens_por_no": listagens,
                "sem_o_novo": {2: 0, 3: sem_o_novo if m == "dangerous" else 0},
                "faltando_total": {2: 0, 3: sem_o_novo if m == "dangerous" else 0},
                "erros": {2: 0, 3: erros_lista if m == "dangerous" else 0},
                "modo": m,
            },
        }
    return r


def _analisar(tmp_path, reps):
    e = E4(reps=len(reps), base=tmp_path)
    e.dir.mkdir(parents=True)
    e.repeticoes = [{"rep": k, "situacao": "ok", "resultado": r} for k, r in enumerate(reps)]
    return e.analyze()


def test_taxa_de_listagem_usa_so_listagens_respondidas(tmp_path):
    # 10 listagens, 5 com erro, 5 sem o objeto novo: das respondidas, 100% sem o novo
    a = _analisar(tmp_path, [_rep(erros_lista=5, sem_o_novo=5)])
    tabela = dict(a["secoes"])["Listagem logo após cada criação"]
    assert "| dangerous | garage3 | 5 | 5 | 5 | 5 | 100.00% [" in tabela


def test_taxas_rotuladas_por_no_e_atraso_das_desatualizadas(tmp_path):
    a = _analisar(tmp_path, [_rep(stale_dangerous=2), _rep(stale_dangerous=1)])
    secoes = dict(a["secoes"])
    assert "taxa stale por nó [IC95 Wilson]" in secoes["Leituras após cada confirmação (sobrescritas)"]
    atraso = secoes["Atraso das leituras desatualizadas"]
    # lags 2, 4 (rep 0) e 2 (rep 1): média 2,67 ± 1,15 ms, n = 3
    assert "| dangerous | 3 | 2.67 ± 1.15 |" in atraso
