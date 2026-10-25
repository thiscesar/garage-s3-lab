"""CLI do laboratório: `labctl`. Toda a lógica vive em lab/; aqui só há chamadas e exibição."""

from __future__ import annotations

import subprocess
import sys

import typer
from docker.errors import DockerException
from rich.console import Console
from rich.table import Table

from lab import cluster, evidence, garage, journal
from lab import s3 as lab_s3
from lab.config import MODOS, N_REPLICAS, NOS, QUORUNS, RAIZ, ErroConfig, Topologia, modo_atual, renderizar

app = typer.Typer(help="Laboratório Garage S3 (Tema 3, Ênfase A).", no_args_is_help=True, add_completion=False)
config_app = typer.Typer(help="Configuração do Garage.", no_args_is_help=True)
mode_app = typer.Typer(help="consistency_mode do cluster.", no_args_is_help=True)
node_app = typer.Typer(help="Parar/ligar nós.", no_args_is_help=True)
net_app = typer.Typer(help="Partição da rede interna.", no_args_is_help=True)
s3_app = typer.Typer(help="Operações S3 manuais por nó.", no_args_is_help=True)
evidence_app = typer.Typer(help="Evidências.", no_args_is_help=True)
for sub, nome in (
    (config_app, "config"),
    (mode_app, "mode"),
    (node_app, "node"),
    (net_app, "net"),
    (s3_app, "s3"),
    (evidence_app, "evidence"),
):
    app.add_typer(sub, name=nome)

console = Console()
cluster.definir_eco(lambda msg: console.print(f"[green]+ {msg}[/green]", highlight=False))

NO = typer.Argument(..., min=1, max=3, help="nó: 1, 2 ou 3")


def _falhar(msg: str) -> None:
    console.print(f"[bold red]ERRO:[/bold red] {msg}")
    raise typer.Exit(1)


def _make(alvo: str) -> None:
    console.print(f"[green]+ make {alvo}[/green]")
    codigo = subprocess.run(["make", alvo], cwd=RAIZ, check=False).returncode
    journal.registrar(alvo, {}, {"codigo": codigo})
    if codigo != 0:
        raise typer.Exit(codigo)


# ---------------------------------------------------------------- ciclo de vida (mesmos alvos do Makefile)


@app.command()
def up() -> None:
    """Sobe do zero ou retoma o cluster em modo consistent (make up)."""
    _make("up")


@app.command()
def down() -> None:
    """Para e remove os contêineres, mantendo dados/ (make down)."""
    _make("down")


@app.command()
def reset() -> None:
    """Derruba tudo e apaga dados/ e generated/ (make reset)."""
    _make("reset")


# ---------------------------------------------------------------- status


def _rotulo(no: dict) -> str:
    """g1/g2/g3 pelo hostname; sem hostname (nó nunca visto por quem responde), o prefixo do ID."""
    nome = no.get("hostname")
    return nome.replace("garage", "g") if nome else no["id"][:8]


@app.command()
def status() -> None:
    """Nós, rede, saúde, modo, quóruns e layout."""
    modo = modo_atual()
    api = garage.AdminAPI()
    topo = Topologia.do_env()

    visoes: dict[int, set[str] | None] = {}
    layout = None
    zonas: dict[str, str] = {}
    for n in NOS:
        try:
            st = api.cluster_status(n)
            visoes[n] = {_rotulo(x) for x in st["nodes"] if x.get("isUp")}
            layout = layout if layout is not None else st.get("layoutVersion")
            zonas.update(
                {x["hostname"]: (x.get("role") or {}).get("zone", "—") for x in st["nodes"] if x.get("hostname")}
            )
        except garage.ErroGarage:
            visoes[n] = None

    tabela = Table(title="Nós", title_justify="left")
    for col in ("nó", "contêiner", "healthcheck", "/health", "interna", "IP interno", "zona", "vê UP", "mem."):
        tabela.add_column(col, no_wrap=True)
    for n in NOS:
        e = cluster.estado(n)
        h = api.health(n) if e["status"] == "running" else None
        mem = cluster.memoria(f"garage{n}")
        visao = visoes[n]
        tabela.add_row(
            f"g{n}",
            e["status"],
            e["saude"] or "—",
            {200: "[green]200[/green]", 503: "[red]503[/red]", None: "—"}.get(h, str(h)),
            "[green]sim[/green]" if e["na_interna"] else "[red]NÃO[/red]",
            topo.ip_interno(n),
            zonas.get(f"garage{n}", "—"),
            ",".join(sorted(visao)) if visao is not None else "—",
            f"{mem / 2**20:.1f} MiB" if mem else "—",
        )
    console.print(tabela)
    console.print(
        "[dim]vê UP = nós que cada um enxerga como ativos (GetClusterStatus); interna = conectado à rede RPC[/dim]"
    )

    if modo in QUORUNS:
        r, w = QUORUNS[modo]
        garantia = (
            "[green]sim (leitura após escrita garantida)[/green]" if r + w > N_REPLICAS else "[yellow]não[/yellow]"
        )
        console.print(f"modo: [bold]{modo}[/bold]   N={N_REPLICAS}  R={r}  W={w}   R+W>N? {garantia}")
    else:
        console.print("modo: [red]desconhecido[/red] (generated/MODE ausente)")
    console.print(f"layout: versão {layout if layout is not None else '?'}")
    if layout == 0:
        console.print("[red]ALERTA: layout não aplicado; o cluster não armazena nada (rode make up)[/red]")


# ---------------------------------------------------------------- config e modo


@config_app.command("render")
def config_render(mode: str = typer.Option(..., "--mode", help=" | ".join(MODOS))) -> None:
    """Gera generated/garage{1,2,3}.toml (sem reiniciar os nós)."""
    try:
        for arq in renderizar(mode):
            console.print(f"gerado {arq.relative_to(RAIZ)} (modo {mode})")
    except ErroConfig as e:
        _falhar(str(e))


@mode_app.command("set")
def mode_set(modo: str = typer.Argument(..., help=" | ".join(MODOS))) -> None:
    """Troca o modo: renderiza, reinicia os 3 nós e espera /health = 200 (evidência no journal)."""
    if modo not in MODOS:
        _falhar(f"modo inválido: {modo} (use {', '.join(MODOS)})")
    try:
        tempos = garage.definir_modo(modo)
    except garage.ErroGarage as e:
        _falhar(str(e))
    for n, t in tempos.items():
        console.print(f"garage{n}: /health 200 {t:.1f} s após o restart")
    console.print(f"modo atual: [bold]{modo_atual()}[/bold]")


# ---------------------------------------------------------------- nós e rede


@node_app.command("stop")
def node_stop(n: int = NO) -> None:
    """Para o contêiner do nó (docker stop)."""
    cluster.parar(n)


@node_app.command("start")
def node_start(n: int = NO) -> None:
    """Liga o contêiner do nó (docker start); ele se reconecta pelos bootstrap_peers."""
    cluster.ligar(n)


@net_app.command("partition")
def net_partition(n: int = NO) -> None:
    """Isola o nó da rede interna (RPC); ele segue acessível pela rede clientes."""
    if not cluster.particionar(n):
        console.print(f"garage{n} já estava fora da rede interna")


@net_app.command("heal")
def net_heal(
    n: int | None = typer.Argument(None, min=1, max=3, help="nó: 1, 2 ou 3"),
    todos: bool = typer.Option(False, "--all", help="restaura tudo: cura, religa e volta a consistent"),
) -> None:
    """Reconecta o nó à rede interna com o IP fixo (ou restaura o cluster inteiro com --all)."""
    if todos:
        feito = garage.restaurar()
        console.print(
            f"curados: {feito['curados'] or '—'}  religados: {feito['religados'] or '—'}  "
            f"modo: {feito['modo'] or 'já era consistent'}"
        )
        return
    if n is None:
        _falhar("informe o nó ou use --all")
    if not cluster.curar(n):
        console.print(f"garage{n} já estava na rede interna")


@net_app.command("state")
def net_state() -> None:
    """Mostra quais nós estão na rede interna."""
    for n in NOS:
        dentro = cluster.na_rede_interna(n)
        console.print(f"garage{n}: {'na rede interna' if dentro else '[red]FORA da rede interna[/red]'}")


# ---------------------------------------------------------------- S3


def _mostrar(res: lab_s3.Resultado) -> None:
    if res.ok:
        console.print(f"[dim]{res.op} via garage{res.no}: HTTP {res.http} em {res.latencia_ms:.1f} ms[/dim]")
    else:
        _falhar(f"{res.op} via garage{res.no} ({res.latencia_ms:.0f} ms): {res.erro}")


@s3_app.command("put")
def s3_put(n: int = NO, chave: str = typer.Argument(...), valor: str = typer.Argument(...)) -> None:
    res = lab_s3.put(lab_s3.cliente_s3(n), n, chave, valor.encode())
    _mostrar(res)
    console.print(f"PUT {chave} ok")


@s3_app.command("get")
def s3_get(n: int = NO, chave: str = typer.Argument(...)) -> None:
    res = lab_s3.get(lab_s3.cliente_s3(n), n, chave)
    _mostrar(res)
    console.print(res.dados.decode(errors="replace"), highlight=False)


@s3_app.command("ls")
def s3_ls(n: int = NO, prefixo: str = typer.Argument("")) -> None:
    res = lab_s3.listar(lab_s3.cliente_s3(n), n, prefixo)
    _mostrar(res)
    for chave in res.dados:
        console.print(chave, highlight=False)
    console.print(f"({len(res.dados)} objetos)")


@s3_app.command("del")
def s3_del(n: int = NO, chave: str = typer.Argument(...)) -> None:
    res = lab_s3.apagar(lab_s3.cliente_s3(n), n, chave)
    _mostrar(res)
    console.print(f"DELETE {chave} ok")


# ---------------------------------------------------------------- evidências


@evidence_app.command("snapshot")
def evidence_snapshot(rotulo: str = typer.Argument(..., help="nome curto, ex.: antes-particao")) -> None:
    """Retrato do cluster: estado, saúde, layout, estatísticas, workers, logs, disco e memória."""
    destino = evidence.snapshot(rotulo)
    console.print(f"snapshot em {journal.relativo(destino)}")


def main() -> None:
    try:
        app()
    except (ErroConfig, garage.ErroGarage, DockerException, RuntimeError, ValueError) as e:
        console.print(f"[bold red]ERRO:[/bold red] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
