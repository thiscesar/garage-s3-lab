"""E8 — Coordenação e ordem (roteiro §4.2 e §7.3).

Partes, por repetição:
1. sem líder: 30 PUT + 30 GET de 4 KiB via cada nó (taxa de sucesso e latência);
2. concorrência: 50 pares de PUT na mesma chave, um via garage1 e outro via garage2, disparados juntos por
   uma barreira; registra t_start/t_end de cada um, o valor final (lido via garage3, consistent) e o carimbo
   da versão final pelo LastModified da listagem (ms; FS4);
3. layout (só na repetição 0): assign -c 2G no garage3 + apply (versão +1), tentativa de apply com versão
   errada (deve ser recusada), reversão para 1G (versão +1); evidências de layout history;
4. E8.4 (só na repetição 0): chave temporária com permissão de criar bucket; 20 pares de CreateBucket com o
   mesmo nome via garage1 e garage2 (barreira); conta buckets duplicados; apaga buckets e chave no fim.
"""

from __future__ import annotations

import threading
import time
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from lab import garage, plots, stats
from lab import s3 as lab_s3
from lab.config import NOS, Topologia
from lab.experiments.base import Experiment
from lab.report import tabela_md


def _cliente_com_chave(n: int, chave: str, segredo: str):
    return boto3.client(
        "s3",
        endpoint_url=f"http://{Topologia.do_env().ip_clientes(n)}:3900",
        region_name="garage",
        aws_access_key_id=chave,
        aws_secret_access_key=segredo,
        config=Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 1, "mode": "standard"},
            connect_timeout=1,
            read_timeout=3,
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


class E8(Experiment):
    nome = "E8"
    descricao = "Coordenação e ordem"

    # ------------------------------------------------------------ 1. sem líder
    def _sem_lider(self, n_ops: int) -> dict:
        corpo = b"x" * 4096
        saida = {}
        for n in NOS:
            c = lab_s3.cliente_s3(n)
            pref = f"e8/lider/{uuid.uuid4().hex[:6]}"
            puts = [lab_s3.put(c, n, f"{pref}/{k}", corpo) for k in range(n_ops)]
            gets = [lab_s3.get(c, n, f"{pref}/{k}") for k in range(n_ops)]
            saida[f"garage{n}"] = {
                "put_ok": sum(r.ok for r in puts),
                "get_ok": sum(r.ok for r in gets),
                "n": n_ops,
                "put_ms": [r.latencia_ms for r in puts if r.ok],
                "get_ms": [r.latencia_ms for r in gets if r.ok],
            }
        return saida

    # ------------------------------------------------------------ 2. concorrência
    def _concorrencia(self, tentativas: int) -> list[dict]:
        clientes = {1: lab_s3.cliente_s3(1), 2: lab_s3.cliente_s3(2)}
        leitor = lab_s3.cliente_s3(3)
        resultados = []
        for k in range(tentativas):
            chave = f"e8/conc/{uuid.uuid4().hex[:8]}"
            lab_s3.put(clientes[1], 1, chave, b"inicial")
            time.sleep(0.2)
            barreira = threading.Barrier(2)
            res: dict[int, lab_s3.Resultado] = {}

            def escrever(n: int, chave=chave, barreira=barreira, res=res) -> None:
                barreira.wait()
                res[n] = lab_s3.put(clientes[n], n, chave, f"via-garage{n}".encode())

            ths = [threading.Thread(target=escrever, args=(n,)) for n in (1, 2)]
            for t in ths:
                t.start()
            for t in ths:
                t.join()
            time.sleep(0.3)
            final = lab_s3.get(leitor, 3, chave)
            lst = leitor.list_objects_v2(Bucket=lab_s3.BUCKET, Prefix=chave).get("Contents", [])
            resultados.append(
                {
                    "tentativa": k,
                    "ok": {n: r.ok for n, r in res.items()},
                    "t_start": {n: r.t_inicio for n, r in res.items()},
                    "t_end": {n: r.t_fim for n, r in res.items()},
                    "final": final.dados.decode() if final.ok else f"erro: {final.erro}",
                    "last_modified": lst[0]["LastModified"].isoformat(timespec="milliseconds") if lst else None,
                }
            )
        return resultados

    # ------------------------------------------------------------ 3. layout
    def _layout(self) -> dict:
        st = self.api.cluster_status(1)
        id3 = next(x["id"] for x in st["nodes"] if x["hostname"] == "garage3")[:16]
        v0 = st["layoutVersion"]
        antes = garage.layout_history()
        garage.cli(1, "layout", "assign", "-c", "2G", id3)
        recusa = None
        try:
            garage.cli(1, "layout", "apply", "--version", str(v0 + 5))
        except garage.ErroGarage as e:
            recusa = str(e)
        v_apos_recusa = self.api.cluster_status(1)["layoutVersion"]
        garage.cli(1, "layout", "apply", "--version", str(v0 + 1))
        v1 = self.api.cluster_status(1)["layoutVersion"]
        cap = next(
            (x.get("role") or {}).get("capacity")
            for x in self.api.cluster_status(1)["nodes"]
            if x["hostname"] == "garage3"
        )
        garage.cli(1, "layout", "assign", "-c", "1G", id3)
        garage.cli(1, "layout", "apply", "--version", str(v1 + 1))
        v2 = self.api.cluster_status(1)["layoutVersion"]
        depois = garage.layout_history()
        self.evidencia("layout_history_antes.txt", "garage layout history (antes)", lambda: antes)
        self.evidencia("layout_history_depois.txt", "garage layout history (depois)", lambda: depois)
        garage.esperar_malha(limite_s=120, api=self.api)
        return {"v0": v0, "v_apos_recusa": v_apos_recusa, "recusa": recusa, "v1": v1, "cap_garage3_v1": cap, "v2": v2}

    # ------------------------------------------------------------ 4. E8.4
    def _create_bucket_concorrente(self, tentativas: int) -> dict:
        nome_chave = f"e8-tmp-{uuid.uuid4().hex[:6]}"
        info = self.api._json("POST", 1, "CreateKey", json={"name": nome_chave, "allow": {"createBucket": True}})
        id_chave, segredo = info["accessKeyId"], info["secretAccessKey"]
        clientes = {n: _cliente_com_chave(n, id_chave, segredo) for n in (1, 2)}
        tentativas_res = []
        try:
            for k in range(tentativas):
                nome = f"e8dup-{uuid.uuid4().hex[:6]}-{k}"
                barreira = threading.Barrier(2)
                resp: dict[int, str] = {}

                def criar(n: int, nome=nome, barreira=barreira, resp=resp) -> None:
                    barreira.wait()
                    try:
                        clientes[n].create_bucket(Bucket=nome)
                        resp[n] = "ok"
                    except ClientError as e:
                        resp[n] = e.response.get("Error", {}).get("Code", "erro")
                    except BotoCoreError as e:
                        resp[n] = type(e).__name__

                ths = [threading.Thread(target=criar, args=(n,)) for n in (1, 2)]
                for t in ths:
                    t.start()
                for t in ths:
                    t.join()
                tentativas_res.append({"nome": nome, "respostas": resp})
            time.sleep(1.0)
            buckets = self.api._json("GET", 1, "ListBuckets")
            meus = [b for b in buckets if any(a["accessKeyId"] == id_chave for a in b.get("localAliases", []))]
            por_nome: dict[str, list[str]] = {}
            for b in meus:
                for a in b["localAliases"]:
                    if a["accessKeyId"] == id_chave:
                        por_nome.setdefault(a["alias"], []).append(b["id"])
            # buckets da chave sem alias (perderam a disputa pelo nome) também contam
            ids_com_alias = {i for v in por_nome.values() for i in v}
            info_chave = self.api._json("GET", 1, "GetKeyInfo", params={"id": id_chave})
            ids_chave = {b["id"] for b in info_chave.get("buckets", [])}
            self.evidencia(
                "e84_buckets.txt",
                "ListBuckets + GetKeyInfo da chave temporária",
                lambda: {"por_nome": por_nome, "buckets_da_chave": sorted(ids_chave)},
            )
            for t in tentativas_res:
                t["buckets_com_o_nome"] = len(por_nome.get(t["nome"], []))
            orfaos = sorted(ids_chave - ids_com_alias)
            return {"tentativas": tentativas_res, "orfaos": orfaos, "total_buckets_chave": len(ids_chave)}
        finally:
            self._limpar_e84(id_chave)

    def _limpar_e84(self, id_chave: str) -> None:
        try:
            info = self.api._json("GET", 1, "GetKeyInfo", params={"id": id_chave})
            for b in info.get("buckets", []):
                try:
                    for alias in b.get("localAliases", []):
                        self.api._json(
                            "POST",
                            1,
                            "RemoveBucketAlias",
                            json={"bucketId": b["id"], "accessKeyId": id_chave, "localAlias": alias},
                        )
                except garage.ErroGarage:
                    pass
                self.api._json("POST", 1, "DeleteBucket", params={"id": b["id"]})
        finally:
            self.api._json("POST", 1, "DeleteKey", params={"id": id_chave})

    # ------------------------------------------------------------ execução
    def run_rep(self, i: int) -> dict:
        r = {
            "sem_lider": self._sem_lider(10 if self.quick else 30),
            "concorrencia": self._concorrencia(10 if self.quick else 50),
        }
        if i == 0:
            r["layout"] = self._layout()
            r["e84"] = self._create_bucket_concorrente(5 if self.quick else 20)
        return r

    # ------------------------------------------------------------ análise
    def analyze(self) -> dict:
        reps = [r["resultado"] for r in self.repeticoes if r.get("situacao") == "ok"]
        if not reps:
            return {"resultados": []}
        nomes = [f"garage{n}" for n in NOS]
        # 1. sem líder
        sl = {
            no: {
                "put_ok": sum(r["sem_lider"][no]["put_ok"] for r in reps),
                "get_ok": sum(r["sem_lider"][no]["get_ok"] for r in reps),
                "n": sum(r["sem_lider"][no]["n"] for r in reps),
                "put": stats.resumir([stats.resumir(r["sem_lider"][no]["put_ms"]).media for r in reps]),
                "get": stats.resumir([stats.resumir(r["sem_lider"][no]["get_ms"]).media for r in reps]),
            }
            for no in nomes
        }
        medias = [sl[no]["put"].media for no in nomes]
        p1 = all(sl[no]["put_ok"] == sl[no]["n"] == sl[no]["get_ok"] for no in nomes) and max(medias) <= 2 * min(medias)

        # 2. concorrência
        tent = [t for r in reps for t in r["concorrencia"] if all(t["ok"].values())]
        linhas_csv, venceu_ultimo, empates, lm = [], [], [], []
        for t in tent:
            d_ms = (t["t_start"][2] - t["t_start"][1]) * 1000  # > 0: garage2 começou depois
            vencedor = 1 if t["final"] == "via-garage1" else 2 if t["final"] == "via-garage2" else None
            ultimo = 2 if d_ms > 0 else 1
            linhas_csv.append(
                {"delta_start_ms": round(d_ms, 3), "vencedor": vencedor, "last_modified": t["last_modified"]}
            )
            if vencedor is None:
                continue
            (empates if abs(d_ms) < 1 else venceu_ultimo).append(vencedor == ultimo)
            lm.append(t["last_modified"])
        self.gravar_csv("metrics.csv", linhas_csv)
        frac_ultimo = sum(venceu_ultimo) / len(venceu_ultimo) if venceu_ultimo else float("nan")
        frac_emp = sum(empates) / len(empates) if empates else float("nan")
        unico = all(t["final"] in ("via-garage1", "via-garage2") for t in tent)
        p2 = unico and (not venceu_ultimo or frac_ultimo > 0.5)
        lm_ok = [x for x in lm if x]
        p3 = len(lm_ok) == len(lm) and all(x.count(".") == 1 for x in lm_ok)
        deltas = [abs(x["delta_start_ms"]) for x in linhas_csv]
        dist = stats.resumir(deltas) if deltas else None
        fig_x = [x["delta_start_ms"] for x in linhas_csv if x["vencedor"]]
        fig_y = [x["vencedor"] for x in linhas_csv if x["vencedor"]]
        fig, ax = plots.plt.subplots(figsize=(6.5, 3.4))
        ax.scatter(fig_x, fig_y, s=14, alpha=0.6, color="#2b6cb0")
        ax.axvline(0, color="#c53030", linewidth=1, linestyle="--")
        ax.set_yticks([1, 2], ["venceu garage1", "venceu garage2"])
        ax.set_xlabel("t_start(garage2) − t_start(garage1) (ms)  → garage2 começou depois")
        ax.set_title("PUTs concorrentes: vencedor × diferença de disparo")
        plots.salvar(fig, self.dir_figuras, "vencedor_vs_disparo")

        # 3. layout e 4. E8.4
        lay = reps[0].get("layout", {})
        p4 = (
            lay.get("v_apos_recusa") == lay.get("v0")
            and lay.get("recusa") is not None
            and lay.get("v1") == lay.get("v0", -9) + 1
            and lay.get("v2") == lay.get("v0", -9) + 2
        )
        e84 = reps[0].get("e84", {})
        dup = [t for t in e84.get("tentativas", []) if t.get("buckets_com_o_nome", 0) > 1]
        ambos_ok = [t for t in e84.get("tentativas", []) if list(t["respostas"].values()).count("ok") == 2]
        n84 = len(e84.get("tentativas", []))
        p5_sit = "confirmada" if (dup or e84.get("orfaos")) else "inconclusiva"
        resultados = [
            {
                "id": "P1",
                "situacao": "confirmada" if p1 else "refutada",
                "resultado": "; ".join(
                    f"{no}: PUT {sl[no]['put_ok']}/{sl[no]['n']} ({sl[no]['put'].media:.1f} ms), "
                    f"GET {sl[no]['get_ok']}/{sl[no]['n']} ({sl[no]['get'].media:.1f} ms)"
                    for no in nomes
                ),
            },
            {
                "id": "P2",
                "situacao": "confirmada" if p2 else "refutada",
                "resultado": f"{len(tent)} pares; valor final único em todos: {unico}; com |Δ| ≥ 1 ms venceu o que começou "
                f"depois em {sum(venceu_ultimo)}/{len(venceu_ultimo)} ({frac_ultimo * 100:.0f}%); com |Δ| < 1 ms: "
                f"{sum(empates)}/{len(empates)} ({frac_emp * 100:.0f}%) para o que começou depois; |Δ| disparo: "
                + (f"mediana {dist.p50:.2f} ms, p95 {dist.p95:.2f} ms" if dist else "—"),
            },
            {
                "id": "P3",
                "situacao": "confirmada" if p3 else "refutada",
                "resultado": f"LastModified com ms em {len(lm_ok)}/{len(lm)} (ex.: {lm_ok[0] if lm_ok else '—'})",
            },
            {
                "id": "P4",
                "situacao": "confirmada" if p4 else "refutada",
                "resultado": f"v{lay.get('v0')} → apply com versão {lay.get('v0', 0) + 5} recusado "
                f"({'sim' if lay.get('recusa') else 'não'}; versão continuou {lay.get('v_apos_recusa')}) → "
                f"v{lay.get('v1')} (garage3 {(lay.get('cap_garage3_v1') or 0) / 1e9:g} GB) → v{lay.get('v2')} (revertido)",
            },
            {
                "id": "P5",
                "situacao": p5_sit,
                "resultado": f"{n84} pares de CreateBucket: ambos responderam ok em {len(ambos_ok)}; nomes com 2+ buckets: "
                f"{len(dup)}; buckets da chave sem alias (órfãos): {len(e84.get('orfaos', []))}"
                + ("" if dup or e84.get("orfaos") else f" — duplicata não observada em {n84} tentativas"),
            },
        ]
        return {
            "resultados": resultados,
            "secoes": [
                (
                    "Sem líder",
                    tabela_md(
                        ["nó", "PUT ok", "PUT média (ms)", "GET ok", "GET média (ms)"],
                        [
                            [
                                no,
                                f"{sl[no]['put_ok']}/{sl[no]['n']}",
                                f"{sl[no]['put'].media:.1f}",
                                f"{sl[no]['get_ok']}/{sl[no]['n']}",
                                f"{sl[no]['get'].media:.1f}",
                            ]
                            for no in nomes
                        ],
                    ),
                ),
                (
                    "E8.4 — respostas por tentativa",
                    tabela_md(
                        ["bucket", "garage1", "garage2", "buckets com o nome"],
                        [
                            [t["nome"], t["respostas"].get(1), t["respostas"].get(2), t.get("buckets_com_o_nome")]
                            for t in e84.get("tentativas", [])
                        ],
                    )
                    + "\n\nChave temporária e buckets apagados no fim.",
                ),
                ("Recusa do layout apply com versão errada", f"```\n{(lay.get('recusa') or '—')[:600]}\n```"),
            ],
            "figuras": ["vencedor_vs_disparo"],
        }
