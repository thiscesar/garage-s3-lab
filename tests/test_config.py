"""lab.config: renderização, topologia, .env e colisão de faixas."""

import ipaddress

import pytest

from lab import config
from lab.config import ErroConfig, Topologia, definir_pares, renderizar

ENV = {
    "RPC_SECRET": "a" * 64,
    "ADMIN_TOKEN": "b" * 64,
    "METRICS_TOKEN": "c" * 64,
    "REDE_INTERNA_PREFIXO": "10.30.0",
    "REDE_CLIENTES_PREFIXO": "172.31.0",
}
PARES = [f"{c * 64}@10.30.0.2{n}:3901" for n, c in ((1, "1"), (2, "2"), (3, "3"))]


def _linhas_diferentes(a: str, b: str) -> list[tuple[str, str]]:
    return [(x, y) for x, y in zip(a.splitlines(), b.splitlines(), strict=True) if x != y]


@pytest.mark.parametrize("com_pares", [False, True])
def test_arquivos_diferem_apenas_em_rpc_public_addr(tmp_path, com_pares):
    if com_pares:
        definir_pares(PARES, tmp_path / "peers")
    g1, g2, g3 = (p.read_text() for p in renderizar("consistent", ENV, tmp_path))
    for outro, n in ((g2, 2), (g3, 3)):
        difs = _linhas_diferentes(g1, outro)
        assert difs == [('rpc_public_addr = "10.30.0.21:3901"', f'rpc_public_addr = "10.30.0.2{n}:3901"')]
    assert (tmp_path / "MODE").read_text().strip() == "consistent"


def test_bootstrap_peers_lista_os_tres_nos(tmp_path):
    definir_pares(PARES, tmp_path / "peers")
    texto = renderizar("degraded", ENV, tmp_path)[0].read_text()
    linha = next(x for x in texto.splitlines() if x.startswith("bootstrap_peers"))
    assert all(f'"{p}"' in linha for p in PARES)
    assert 'consistency_mode   = "degraded"' in texto


def test_sem_pares_a_lista_fica_vazia(tmp_path):
    texto = renderizar("consistent", ENV, tmp_path)[0].read_text()
    assert "bootstrap_peers = []" in texto


def test_enderecos_de_escuta_sao_0000(tmp_path):
    texto = renderizar("consistent", ENV, tmp_path)[0].read_text()
    assert "[::]" not in texto  #  (roteiro §7.2 e §7.6)
    assert texto.count('"0.0.0.0:') == 3


def test_modo_invalido_e_segredo_vazio(tmp_path):
    with pytest.raises(ErroConfig):
        renderizar("forte", ENV, tmp_path)
    with pytest.raises(ErroConfig):
        renderizar("consistent", {**ENV, "RPC_SECRET": ""}, tmp_path)


def test_par_invalido_e_rejeitado(tmp_path):
    with pytest.raises(ErroConfig):
        definir_pares(["abc@10.30.0.21:3901"], tmp_path / "peers")


def test_topologia_ips_fixos():
    topo = Topologia.do_env(ENV)
    assert [topo.ip_interno(n) for n in (1, 2, 3)] == ["10.30.0.21", "10.30.0.22", "10.30.0.23"]
    assert topo.ip_clientes(2) == "172.31.0.22"
    assert topo.ip_cliente == "172.31.0.100"
    assert topo.subrede_interna == ipaddress.ip_network("10.30.0.0/24")
    with pytest.raises(ErroConfig):
        topo.ip_interno(4)


@pytest.mark.parametrize("prefixo", ["10.30", "10.30.0.0", "300.1.1", "a.b.c", ""])
def test_prefixo_invalido(prefixo):
    with pytest.raises(ErroConfig):
        Topologia.do_env({**ENV, "REDE_INTERNA_PREFIXO": prefixo})


def test_env_ida_e_volta(tmp_path):
    arq = tmp_path / ".env"
    arq.write_text("# comentário\nA=1\nB='dois'\n")
    config.definir_env("A", "um", arq)
    config.definir_env("C", "3", arq)
    assert config.carregar_env(arq) == {"A": "um", "B": "dois", "C": "3"}
    assert oct(arq.stat().st_mode)[-3:] == "600"


def test_colisao_de_faixas(monkeypatch):
    boom = ("boom_docker_stack_boom_network", ipaddress.ip_network("172.30.0.0/16"))
    monkeypatch.setattr(config, "_redes_docker_alheias", lambda: [boom])
    monkeypatch.setattr(config, "_rotas_fora_do_docker", lambda: [("wlo1", ipaddress.ip_network("192.168.15.0/24"))])
    padrao = Topologia("172.30.0", "172.31.0")
    assert any("REDE_INTERNA_PREFIXO" in p and "boom" in p for p in config.colisoes_de_rede(padrao))
    assert config.colisoes_de_rede(Topologia("10.30.0", "172.31.0")) == []
    assert config.colisoes_de_rede(Topologia("10.30.0", "10.30.0"))  # interna = clientes


def test_quoruns_batem_com_a_tabela_da_spec():
    #  / C6: só consistent tem R + W > N
    garantidos = {m for m, (r, w) in config.QUORUNS.items() if r + w > config.N_REPLICAS}
    assert garantidos == {"consistent"}
    assert config.QUORUNS == {"consistent": (2, 2), "degraded": (1, 2), "dangerous": (1, 1)}


def test_propriedades_quorum_por_modo():
    # 2W > N (van Steen e Tanenbaum): só dangerous aceita escrita nos dois lados de uma partição (E9)
    p = {m: config.propriedades_quorum(m) for m in config.MODOS}
    assert {m for m in p if p[m]["R+W>N"]} == {"consistent"}
    assert {m for m in p if not p[m]["2W>N"]} == {"dangerous"}
    assert p["degraded"] == {"N": 3, "R": 1, "W": 2, "R+W>N": False, "2W>N": True}
