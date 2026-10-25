"""Cliente S3 do laboratório: uma única tentativa por operação."""

import socket
import threading
import time

import boto3

from lab import s3 as lab_s3


def _servidor_mudo():
    """Aceita conexões e nunca responde (simula um nó que não devolve a resposta a tempo)."""
    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen()
    conexoes = []

    def aceitar():
        while True:
            try:
                conexoes.append(srv.accept()[0])
            except OSError:
                return

    threading.Thread(target=aceitar, daemon=True).start()
    return srv, conexoes


def test_config_sem_novas_tentativas():
    retries = lab_s3.config_cliente().retries
    assert retries["total_max_attempts"] == 1
    assert "max_attempts" not in retries  # no botocore, max_attempts = nº de NOVAS tentativas


def test_timeout_faz_um_unico_envio():
    srv, conexoes = _servidor_mudo()
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://127.0.0.1:{srv.getsockname()[1]}",
            region_name="garage",
            aws_access_key_id="GKteste",
            aws_secret_access_key="segredo",
            config=lab_s3.config_cliente(connect_timeout=1, read_timeout=0.3),
        )
        envios = []
        s3.meta.events.register("before-send.s3", lambda **_: envios.append(time.monotonic()))
        res = lab_s3.get(s3, 1, "qualquer")
        assert not res.ok and "timeout" in res.erro.lower()
        assert len(envios) == 1
        assert res.latencia_ms < 1000  # um timeout de 0,3 s, não dois
        assert len(conexoes) == 1
    finally:
        srv.close()
