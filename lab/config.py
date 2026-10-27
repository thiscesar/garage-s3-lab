"""Topologia, carga do .env e renderização da configuração do Garage.

Uso pelos scripts; o `labctl` chama as mesmas funções:

    python -m lab.config init-env              # cria/completa o .env
    python -m lab.config check-redes           # falha se as faixas colidirem com redes do host
    python -m lab.config render --mode <modo>  # gera generated/garage{1,2,3}.toml e generated/MODE
    python -m lab.config definir-pares <id@ip:porta>...  # grava generated/peers (bootstrap_peers)
    python -m lab.config valor <CHAVE>         # imprime um valor do .env
    python -m lab.config definir <CHAVE> <V>   # grava um valor no .env
    python -m lab.config ip-interno <n>        # IP do nó n na rede interna
    python -m lab.config ip-clientes <n>       # IP do nó n na rede clientes
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import re
import secrets
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined

RAIZ = Path(__file__).resolve().parent.parent
ARQ_ENV = RAIZ / ".env"
ARQ_ENV_EXEMPLO = RAIZ / ".env.example"
DIR_GENERATED = RAIZ / "generated"
ARQ_PARES = DIR_GENERATED / "peers"
TEMPLATE = RAIZ / "config" / "garage.toml.j2"

MODOS = ("consistent", "degraded", "dangerous")
# Quóruns (R, W) para replication_factor = 3. Fonte: doc Configuration (consistency_mode) e código v2.3.0
# src/rpc/replication_mode.rs l. 41–55.
N_REPLICAS = 3
QUORUNS = {"consistent": (2, 2), "degraded": (1, 2), "dangerous": (1, 1)}


def propriedades_quorum(modo: str) -> dict:
    """R, W e as duas condições de quórum do modo (N = 3).

    - R + W > N: todo quórum de leitura cruza o de escrita; o Garage escolhe os quóruns de `consistent` "so that
      read-after-write consistency is guaranteed" (doc `reference-manual/configuration.md` l. 241–243). É leitura
      após escrita com semântica de variável regular (o reparo na leitura roda em segundo plano, `src/table/table.rs`
      l. 352), não linearizabilidade.
    - 2W > N: dois quóruns de escrita sempre se cruzam, então lados disjuntos de uma partição não aceitam escritas
      ao mesmo tempo. Condição de quórum de escrita de van Steen e Tanenbaum (Distributed Systems, 3ª ed.);
      não está em Gifford (1979). Aritmeticamente: dois subconjuntos de tamanho W em N se cruzam sse 2W > N.
    """
    r, w = QUORUNS[modo]
    return {"N": N_REPLICAS, "R": r, "W": w, "R+W>N": r + w > N_REPLICAS, "2W>N": 2 * w > N_REPLICAS}


NOS = (1, 2, 3)
REDES_DO_LAB = ("t3_interna", "t3_clientes")
SEGREDOS = ("RPC_SECRET", "ADMIN_TOKEN", "METRICS_TOKEN")

_RE_PREFIXO = re.compile(r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})$")
# Formato de `garage node id -q` e de bootstrap_peers: <chave pública hex>@<ip>:<porta>
_RE_PAR = re.compile(r"^[0-9a-f]{64}@\d{1,3}(\.\d{1,3}){3}:\d+$")


class ErroConfig(Exception):
    """Erro de configuração com mensagem para o usuário."""


# ---------------------------------------------------------------- .env


def carregar_env(caminho: Path = ARQ_ENV) -> dict[str, str]:
    """Lê um arquivo KEY=VALUE (sem interpolação). Linhas vazias e comentários são ignorados."""
    if not caminho.exists():
        raise ErroConfig(f"{caminho.name} não encontrado: rode `make env` primeiro.")
    valores: dict[str, str] = {}
    for linha in caminho.read_text().splitlines():
        linha = linha.strip()
        if not linha or linha.startswith("#") or "=" not in linha:
            continue
        chave, valor = linha.split("=", 1)
        valores[chave.strip()] = valor.strip().strip('"').strip("'")
    return valores


def definir_env(chave: str, valor: str, caminho: Path = ARQ_ENV) -> None:
    """Grava `chave=valor` no .env, substituindo a linha existente ou acrescentando ao final."""
    linhas = caminho.read_text().splitlines() if caminho.exists() else []
    nova = f"{chave}={valor}"
    for i, linha in enumerate(linhas):
        if linha.split("=", 1)[0].strip() == chave and not linha.lstrip().startswith("#"):
            linhas[i] = nova
            break
    else:
        linhas.append(nova)
    caminho.write_text("\n".join(linhas) + "\n")
    caminho.chmod(0o600)


def iniciar_env() -> list[str]:
    """Cria o .env a partir do .env.example (se não existir) e preenche segredos, UID e GID vazios.

    Valores já preenchidos nunca são sobrescritos. Retorna as chaves preenchidas agora.
    """
    if not ARQ_ENV.exists():
        ARQ_ENV.write_text(ARQ_ENV_EXEMPLO.read_text())
        ARQ_ENV.chmod(0o600)
    atual = carregar_env()
    preenchidas = []
    for chave in SEGREDOS:
        if not atual.get(chave):
            definir_env(chave, secrets.token_hex(32))
            preenchidas.append(chave)
    for chave, valor in (("UID", os.getuid()), ("GID", os.getgid())):
        if not atual.get(chave):
            definir_env(chave, str(valor))
            preenchidas.append(chave)
    return preenchidas


# ---------------------------------------------------------------- topologia


def _validar_prefixo(nome: str, prefixo: str) -> str:
    m = _RE_PREFIXO.match(prefixo)
    if not m or any(int(p) > 255 for p in m.groups()):
        raise ErroConfig(f"{nome}={prefixo!r} inválido: use os três primeiros octetos, ex.: 172.30.0")
    return prefixo


@dataclass(frozen=True)
class Topologia:
    """IPs do laboratório a partir dos prefixos do .env. Os finais são fixos."""

    prefixo_interna: str
    prefixo_clientes: str

    @classmethod
    def do_env(cls, env: dict[str, str] | None = None) -> Topologia:
        env = carregar_env() if env is None else env
        return cls(
            _validar_prefixo("REDE_INTERNA_PREFIXO", env.get("REDE_INTERNA_PREFIXO", "")),
            _validar_prefixo("REDE_CLIENTES_PREFIXO", env.get("REDE_CLIENTES_PREFIXO", "")),
        )

    @staticmethod
    def _validar_no(n: int) -> int:
        if n not in NOS:
            raise ErroConfig(f"nó inválido: {n} (use 1, 2 ou 3)")
        return n

    def ip_interno(self, n: int) -> str:
        return f"{self.prefixo_interna}.{20 + self._validar_no(n)}"

    def ip_clientes(self, n: int) -> str:
        return f"{self.prefixo_clientes}.{20 + self._validar_no(n)}"

    @property
    def ip_cliente(self) -> str:
        return f"{self.prefixo_clientes}.100"

    @property
    def subrede_interna(self) -> ipaddress.IPv4Network:
        return ipaddress.ip_network(f"{self.prefixo_interna}.0/24")

    @property
    def subrede_clientes(self) -> ipaddress.IPv4Network:
        return ipaddress.ip_network(f"{self.prefixo_clientes}.0/24")


# ---------------------------------------------------------------- colisão de faixas


def _redes_docker_alheias() -> list[tuple[str, ipaddress.IPv4Network]]:
    """Sub-redes IPv4 das redes Docker do host que não são deste laboratório."""
    nomes = subprocess.run(
        ["docker", "network", "ls", "--format", "{{.Name}}"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    alheias = [n for n in nomes if n not in REDES_DO_LAB]
    if not alheias:
        return []
    saida = subprocess.run(
        ["docker", "network", "inspect", *alheias],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    redes = []
    for rede in json.loads(saida):
        for cfg in (rede.get("IPAM") or {}).get("Config") or []:
            sub = cfg.get("Subnet", "")
            if sub and ":" not in sub:
                redes.append((rede["Name"], ipaddress.ip_network(sub, strict=False)))
    return redes


def _rotas_fora_do_docker() -> list[tuple[str, ipaddress.IPv4Network]]:
    """Rotas IPv4 do host em interfaces que não são pontes do Docker (ex.: Wi-Fi, VPN)."""
    saida = subprocess.run(["ip", "-4", "-o", "route"], capture_output=True, text=True, check=False).stdout
    rotas = []
    for linha in saida.splitlines():
        partes = linha.split()
        if not partes or partes[0] == "default" or "dev" not in partes:
            continue
        dev = partes[partes.index("dev") + 1]
        if dev.startswith(("br-", "docker")):
            continue
        try:
            rotas.append((dev, ipaddress.ip_network(partes[0], strict=False)))
        except ValueError:
            continue
    return rotas


def colisoes_de_rede(topo: Topologia) -> list[str]:
    """Lista as colisões das faixas do laboratório com redes já existentes no host."""
    problemas = []
    vizinhas = [(f"rede Docker {nome}", sub) for nome, sub in _redes_docker_alheias()]
    vizinhas += [(f"rota do host em {dev}", sub) for dev, sub in _rotas_fora_do_docker()]
    for nome_var, nossa in (
        ("REDE_INTERNA_PREFIXO", topo.subrede_interna),
        ("REDE_CLIENTES_PREFIXO", topo.subrede_clientes),
    ):
        for origem, sub in vizinhas:
            if nossa.overlaps(sub):
                problemas.append(f"{nome_var} ({nossa}) colide com {origem} ({sub})")
    if topo.subrede_interna.overlaps(topo.subrede_clientes):
        problemas.append("as redes interna e clientes não podem ter a mesma faixa")
    return problemas


# ---------------------------------------------------------------- renderização


def definir_pares(pares: list[str], arquivo: Path = ARQ_PARES) -> None:
    """Grava os identificadores dos nós (`garage node id -q`) usados em bootstrap_peers."""
    invalidos = [p for p in pares if not _RE_PAR.match(p)]
    if invalidos:
        raise ErroConfig(f"identificador de nó inválido: {', '.join(invalidos)} (esperado <id>@<ip>:<porta>)")
    arquivo.parent.mkdir(parents=True, exist_ok=True)
    arquivo.write_text("\n".join(pares) + "\n")


def ler_pares(arquivo: Path = ARQ_PARES) -> list[str]:
    """Pares gravados pelo iniciar.sh; lista vazia antes da primeira montagem do cluster."""
    if not arquivo.exists():
        return []
    return [linha.strip() for linha in arquivo.read_text().splitlines() if linha.strip()]


def renderizar(modo: str, env: dict[str, str] | None = None, destino: Path = DIR_GENERATED) -> list[Path]:
    """Gera garage{1,2,3}.toml (diferem só em rpc_public_addr) e o arquivo MODE.

    `bootstrap_peers` traz os 3 nós (o mesmo nos 3 arquivos) quando generated/peers existe.
    """
    if modo not in MODOS:
        raise ErroConfig(f"modo inválido: {modo!r} (use {', '.join(MODOS)})")
    env = carregar_env() if env is None else env
    faltando = [k for k in SEGREDOS if not env.get(k)]
    if faltando:
        raise ErroConfig(f"segredos vazios no .env: {', '.join(faltando)}; rode `make env`.")
    topo = Topologia.do_env(env)
    template = Environment(
        loader=FileSystemLoader(TEMPLATE.parent),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
    ).get_template(TEMPLATE.name)

    destino.mkdir(parents=True, exist_ok=True)
    pares = ler_pares(destino / ARQ_PARES.name)
    gerados = []
    for n in NOS:
        arq = destino / f"garage{n}.toml"
        arq.write_text(
            template.render(
                mode=modo,
                rpc_ip=topo.ip_interno(n),
                rpc_secret=env["RPC_SECRET"],
                admin_token=env["ADMIN_TOKEN"],
                metrics_token=env["METRICS_TOKEN"],
                bootstrap_peers=pares,
            )
        )
        gerados.append(arq)
    (destino / "MODE").write_text(modo + "\n")
    return gerados


def modo_atual(destino: Path = DIR_GENERATED) -> str | None:
    arq = destino / "MODE"
    return arq.read_text().strip() if arq.exists() else None


# ---------------------------------------------------------------- CLI mínima


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m lab.config", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init-env", help="cria/completa o .env")
    sub.add_parser("check-redes", help="verifica colisão das faixas com redes do host")
    p = sub.add_parser("render", help="renderiza a configuração dos 3 nós")
    p.add_argument("--mode", required=True, choices=MODOS)
    p = sub.add_parser("definir-pares", help="grava generated/peers (bootstrap_peers)")
    p.add_argument("pares", nargs="+")
    p = sub.add_parser("valor", help="imprime um valor do .env")
    p.add_argument("chave")
    p = sub.add_parser("definir", help="grava um valor no .env")
    p.add_argument("chave")
    p.add_argument("valor")
    for nome in ("ip-interno", "ip-clientes"):
        p = sub.add_parser(nome, help=f"IP do nó na rede {nome.split('-')[1]}")
        p.add_argument("no", type=int)
    args = parser.parse_args(argv)

    try:
        if args.cmd == "init-env":
            preenchidas = iniciar_env()
            print("`.env` pronto" + (f"; preenchido agora: {', '.join(preenchidas)}" if preenchidas else ""))
        elif args.cmd == "check-redes":
            problemas = colisoes_de_rede(Topologia.do_env())
            if problemas:
                for p in problemas:
                    print(f"ERRO: {p}", file=sys.stderr)
                print("Ajuste o prefixo no .env e rode de novo.", file=sys.stderr)
                return 1
            topo = Topologia.do_env()
            print(f"faixas livres: interna {topo.subrede_interna}, clientes {topo.subrede_clientes}")
        elif args.cmd == "render":
            for arq in renderizar(args.mode):
                print(f"gerado {arq.relative_to(RAIZ)} (modo {args.mode})")
        elif args.cmd == "definir-pares":
            definir_pares(args.pares)
            print(f"gravado {ARQ_PARES.relative_to(RAIZ)} ({len(args.pares)} nós)")
        elif args.cmd == "valor":
            env = carregar_env()
            if args.chave not in env:
                raise ErroConfig(f"{args.chave} não existe no .env")
            print(env[args.chave])
        elif args.cmd == "definir":
            definir_env(args.chave, args.valor)
        elif args.cmd == "ip-interno":
            print(Topologia.do_env().ip_interno(args.no))
        elif args.cmd == "ip-clientes":
            print(Topologia.do_env().ip_clientes(args.no))
    except ErroConfig as e:
        print(f"ERRO: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(_main())
