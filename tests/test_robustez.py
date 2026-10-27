"""Robustez do laboratório diante de falhas (sem Docker)."""

import json
import threading

import pytest
from docker.errors import DockerException

from lab import cluster, garage
from lab.experiments import base, e5_falha_quorum, e6_particao
from lab.experiments.e7_recuperacao import E7
from lab.experiments.e8_coordenacao import lastmodified_com_ms


def test_reiniciar_propaga_falha_de_restart(monkeypatch):
    class Falha:
        def restart(self):
            raise DockerException("containerd: task must be stopped")

    class Ok:
        def restart(self):
            pass

    monkeypatch.setattr(cluster, "container", lambda n: Falha() if n == 2 else Ok())
    monkeypatch.setattr(cluster.journal, "registrar", lambda *a, **k: None)
    with pytest.raises(DockerException, match="garage2"):
        cluster.reiniciar((1, 2, 3))


class _ExpFalha(base.Experiment):
    nome = "EX"
    reps_padrao = 1

    def precondition(self):
        pass

    def _montar_manifest(self):
        return {"experimento": "EX"}

    def run_rep(self, i):
        raise ValueError("falha original da repetição")

    def analyze(self):
        return {"resultados": []}


def test_falha_na_restauracao_nao_apaga_o_registro_da_repeticao(tmp_path, monkeypatch):
    def restaurar_quebrado():
        raise garage.ErroGarage("malha incompleta após 120 s")

    monkeypatch.setattr(garage, "restaurar", restaurar_quebrado)
    monkeypatch.setattr(base.journal, "registrar", lambda *a, **k: None)
    monkeypatch.setattr(base.report, "gerar_summary", lambda *a, **k: None)
    e = _ExpFalha(base=tmp_path)
    with pytest.raises(garage.ErroGarage):
        e.run()
    reps = json.loads((e.dir / "manifest.json").read_text())["repeticoes"]
    assert len(reps) == 1
    assert reps[0]["erro"] == "ValueError: falha original da repetição"
    assert "malha incompleta" in reps[0]["erro_restauracao"]


def test_e6_para_o_monitor_mesmo_se_a_carga_falhar_ao_parar(tmp_path, monkeypatch):
    estado = {}

    class CargaFalsa:
        t0 = 0.0
        fase = None

        def __init__(self, *a, **k):
            pass

        def iniciar(self):
            pass

        def agora(self):
            return 0.0

        def parar(self):
            raise RuntimeError("thread da carga morreu")

    def monitor_falso(api, t0, parar, saida, nos=(1, 3)):
        estado["evento"] = parar
        parar.wait(5)

    monkeypatch.setattr(e6_particao, "Workload", CargaFalsa)
    monkeypatch.setattr(e6_particao, "monitorar_visao", monitor_falso)
    monkeypatch.setattr(e6_particao.cluster, "particionar", lambda n: True)
    monkeypatch.setattr(e6_particao.cluster, "curar", lambda n: True)
    monkeypatch.setattr(e6_particao.garage, "esperar_malha", lambda **k: 0.0)
    monkeypatch.setattr(e6_particao.time, "sleep", lambda s: None)
    e = e6_particao.E6(base=tmp_path)
    e.dir.mkdir(parents=True)
    monkeypatch.setattr(e, "evidencia", lambda *a, **k: None)
    antes = set(threading.enumerate())
    with pytest.raises(RuntimeError):
        e.run_rep(0)
    assert estado["evento"].is_set()
    assert not [t for t in threading.enumerate() if t not in antes and t.is_alive()]


def test_e6_primeiro_ok_da_minoria_e_nosuchkey_desatualizado(tmp_path):
    marcos = {"partition_start": 10.0, "partition_at": 10.0, "heal_start": 130.0, "heal_at": 130.0}
    evs = [
        {"op": "GET", "node": 3, "t_start": 134.0, "t_end": 135.0, "status": "ok", "class": "fresh"},
        {"op": "GET", "node": 3, "t_start": 131.0, "t_end": 132.0, "status": "ok", "class": "fresh"},  # menor t_end
        {
            "op": "GET",
            "node": 1,
            "t_start": 20.0,
            "t_end": 20.1,
            "status": "error",
            "class": "stale",
            "error": "NoSuchKey: The specified key does not exist.",
        },
    ]
    arq = tmp_path / "ops.jsonl"
    arq.write_text("\n".join(json.dumps(x) for x in evs) + "\n")
    r = e6_particao.E6._resumir_ops(arq, marcos)
    assert r["primeiro_ok_minoria_apos_cura_s"] == 2.0
    assert r["por_grupo"]["partition|maioria|GET"]["stale"] == 1


def test_e8_p3_exige_milissegundos_diferentes_de_zero():
    assert not lastmodified_com_ms(["2026-10-25T05:57:05.000+00:00", "2026-10-25T05:57:06.000+00:00"])
    assert lastmodified_com_ms(["2026-10-25T05:57:05.000+00:00", "2026-10-25T05:57:05.050+00:00"])
    assert not lastmodified_com_ms(["2026-10-25T05:57:05.050+00:00", None])


def test_e7_tabelas_ausentes_nao_contam_como_convergidas():
    no = {"tabelas": {}, "blocos": {"rc": None, "resync": 0, "erros": 0}}
    estado = {"nos": {f"garage{n}": no for n in (1, 2, 3)}, "du": {f"garage{n}": 100 for n in (1, 2, 3)}}
    ok, motivos = E7.convergido(estado)
    assert not ok and motivos


def test_e5_sonda_para_se_a_acao_falhar(tmp_path, monkeypatch):
    estado = {}

    def sonda_falsa(self, querer_ok, parar, saida, t_ref, limite):
        estado["parar"] = parar
        parar.wait(5)

    monkeypatch.setattr(e5_falha_quorum.E5, "_sonda_escrita", sonda_falsa)
    e = e5_falha_quorum.E5(base=tmp_path)

    def acao():
        raise DockerException("docker stop falhou")

    with pytest.raises(DockerException):
        e._medir_transicao(acao, False, 1.0)
    assert estado["parar"].is_set()


class _ApiLayout:
    def __init__(self):
        self.versao = 3

    def cluster_status(self, n):
        return {
            "layoutVersion": self.versao,
            "nodes": [{"id": "0b63ad6b80851f29" + "0" * 48, "hostname": "garage3", "role": {"capacity": 1}}],
        }


def _e8_com_cli_falso(tmp_path, monkeypatch, falhar_em):
    from lab.experiments import e8_coordenacao

    api = _ApiLayout()
    chamadas = []

    def cli(n, *args):
        chamadas.append(args)
        if args[:2] == ("layout", "apply"):
            versao = int(args[3])
            if versao != api.versao + 1:
                raise garage.ErroGarage("Invalid new layout version")
            if len([c for c in chamadas if c[:2] == ("layout", "apply")]) == falhar_em:
                raise garage.ErroGarage("falha simulada no apply")
            api.versao = versao
        return ""

    monkeypatch.setattr(e8_coordenacao.garage, "cli", cli)
    monkeypatch.setattr(e8_coordenacao.garage, "layout_history", lambda: "")
    monkeypatch.setattr(e8_coordenacao.garage, "esperar_malha", lambda **k: 0.0)
    e = e8_coordenacao.E8(base=tmp_path)
    e.api = api
    return e, chamadas


def test_e8_reverte_mudanca_pendente_se_o_apply_falhar(tmp_path, monkeypatch):
    e, chamadas = _e8_com_cli_falso(tmp_path, monkeypatch, falhar_em=2)  # 1º apply = recusa esperada; 2º falha
    with pytest.raises(garage.ErroGarage, match="falha simulada"):
        e._layout()
    assert ("layout", "revert", "--yes") in chamadas  # 2G pendente descartado


def test_e8_volta_a_1g_se_falhar_depois_de_aplicar_2g(tmp_path, monkeypatch):
    e, chamadas = _e8_com_cli_falso(tmp_path, monkeypatch, falhar_em=3)  # 2G aplicado; falha ao aplicar o 1G
    with pytest.raises(garage.ErroGarage, match="falha simulada"):
        e._layout()
    assert chamadas[-2][:3] == ("layout", "assign", "-c") and chamadas[-2][3] == "1G"
    assert chamadas[-1][:2] == ("layout", "apply")
    assert e.api.versao == 5  # 3 → 4 (2G) → 5 (1G aplicado na reversão)
