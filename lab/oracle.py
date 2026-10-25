"""Oráculo: classifica cada leitura como fresh, stale, error ou anomaly.

Fundamentação: precedência em tempo real de Malkhi e Reiter (1998, §3.2). Uma operação precede
outra se termina antes de a outra começar; senão, são concorrentes. Uma leitura não concorrente com
escritas deve devolver o último valor escrito; uma leitura concorrente pode devolver o anterior ou o novo.

Para uma leitura no intervalo [ts, te] que devolveu a versão r:
- acked_max(ts)   = maior versão cujo PUT foi confirmado antes de ts;
- started_max(te) = maior versão cujo PUT começou antes de te;
- stale   se existe escrita w confirmada antes de ts que **começou depois de a escrita de r terminar**
          (w sucede a escrita lida em tempo real). lag_versions = nº dessas escritas;
          lag_ms = ts − a 1ª confirmação entre elas;
- fresh   caso contrário (inclui ler uma versão cuja escrita foi concorrente com a última confirmada:
          escritas concorrentes podem ser serializadas em qualquer ordem, ex.: LWW entre dois escritores).
Com um único escritor (escritas sequenciais) isso equivale a "r < acked_max(ts)".
- anomaly se r > started_max(te) ou se r nunca foi escrita (indica bug do laboratório);
- error   se houve timeout ou erro HTTP.
`NoSuchKey` = versão 0: fresh sem escrita confirmada; stale com `motivo: nosuchkey` com escrita confirmada.
Um PUT que falhou (timeout, erro) pode ter sido aplicado: ler a versão dele não é anomalia, pois começou antes de te.

Tempos: relógio monotônico do host, em segundos.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

CLASSES = ("fresh", "stale", "error", "anomaly")


@dataclass
class Escrita:
    versao: int
    t_inicio: float
    t_fim: float | None = None
    ok: bool | None = None  # None = ainda em andamento
    lado: str | None = None  # "maj" / "min" no A2


@dataclass(frozen=True)
class Classificacao:
    classe: str
    motivo: str | None = None
    lag_versions: int | None = None
    lag_ms: float | None = None
    acked_max: int = 0
    started_max: int = 0


class Oraculo:
    """Registro das escritas por chave e classificação das leituras. Seguro para várias threads."""

    def __init__(self) -> None:
        self._escritas: dict[str, dict[int, Escrita]] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------ escritas

    def inicio_escrita(self, chave: str, versao: int, lado: str | None = None, t_inicio: float | None = None) -> float:
        """Registra o início de um PUT e devolve o instante usado.

        Sem `t_inicio`, o instante é lido dentro do lock: assim nenhuma leitura que termine depois dele
        pode ser classificada sem enxergar esta escrita (evita anomalia falsa por corrida entre threads).
        """
        with self._lock:
            t = time.monotonic() if t_inicio is None else t_inicio
            por_versao = self._escritas.setdefault(chave, {})
            if versao in por_versao:
                raise ValueError(f"versão {versao} de {chave!r} registrada duas vezes")
            por_versao[versao] = Escrita(versao, t, lado=lado)
            return t

    def fim_escrita(self, chave: str, versao: int, t_fim: float, ok: bool) -> None:
        with self._lock:
            e = self._escritas[chave][versao]
            e.t_fim, e.ok = t_fim, ok

    def escritas(self, chave: str) -> list[Escrita]:
        with self._lock:
            return sorted(self._escritas.get(chave, {}).values(), key=lambda e: e.versao)

    def pronto(self, chave: str, te: float) -> bool:
        """True se toda escrita que começou antes de te já terminou: só então a classificação é definitiva."""
        with self._lock:
            return all(e.ok is not None for e in self._escritas.get(chave, {}).values() if e.t_inicio < te)

    # ------------------------------------------------------------ leituras

    def classificar(
        self,
        chave: str,
        ts: float,
        te: float,
        versao_lida: int | None,
        erro: str | None = None,
        nosuchkey: bool = False,
    ) -> Classificacao:
        """Classifica uma leitura [ts, te]. `versao_lida` None com `nosuchkey` False = corpo ilegível."""
        escritas = self.escritas(chave)
        confirmadas = [e for e in escritas if e.ok and e.t_fim is not None and e.t_fim < ts]
        acked_max = max((e.versao for e in confirmadas), default=0)
        started_max = max((e.versao for e in escritas if e.t_inicio < te), default=0)

        if erro is not None and not nosuchkey:
            return Classificacao("error", motivo=erro, acked_max=acked_max, started_max=started_max)

        r = 0 if nosuchkey else versao_lida
        motivo = "nosuchkey" if nosuchkey else None
        if r is None:
            return Classificacao("anomaly", "corpo ilegível", acked_max=acked_max, started_max=started_max)
        if r > 0 and r not in {e.versao for e in escritas}:
            return Classificacao("anomaly", "versão nunca escrita", acked_max=acked_max, started_max=started_max)
        if r > started_max:
            return Classificacao(
                "anomaly", "versão começou depois da leitura", acked_max=acked_max, started_max=started_max
            )
        # Escritas confirmadas antes de ts que sucedem (em tempo real) a escrita lida: provam que r está superada.
        if r == 0:
            sucessoras = confirmadas  # nada lido: qualquer escrita confirmada antes de ts supera
        else:
            fim_lida = next(e for e in escritas if e.versao == r).t_fim
            sucessoras = [e for e in confirmadas if fim_lida is not None and e.t_inicio > fim_lida]
        if not sucessoras:
            return Classificacao("fresh", motivo, acked_max=acked_max, started_max=started_max)
        return Classificacao(
            "stale",
            motivo,
            lag_versions=len(sucessoras),
            lag_ms=(ts - min(e.t_fim for e in sucessoras)) * 1000,
            acked_max=acked_max,
            started_max=started_max,
        )
