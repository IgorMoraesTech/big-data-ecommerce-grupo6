import shutil
import time
from pathlib import Path


INCOMING = Path("/stream/incoming")
READY = Path("/stream/ready")
CHECK_INTERVAL = 2


def main():
    INCOMING.mkdir(parents=True, exist_ok=True)
    READY.mkdir(parents=True, exist_ok=True)

    print("[promoter] iniciado", flush=True)
    print(f"[promoter] incoming={INCOMING}", flush=True)
    print(f"[promoter] ready={READY}", flush=True)

    while True:
        files = [
            f for f in INCOMING.iterdir()
            if f.is_file()
        ]

        # O arquivo mais novo pode ainda estar aberto pelo FileRoll.
        # Portanto movemos somente os anteriores.
        files.sort(
            key=lambda f: f.stat().st_mtime,
            reverse=True
        )

        if len(files) > 1:
            for file in files[1:]:
                try:
                    if file.stat().st_size == 0:
                        # Arquivos vazios antigos não interessam ao Flink.
                        file.unlink(missing_ok=True)
                        continue

                    destination = READY / file.name

                    shutil.move(
                        str(file),
                        str(destination)
                    )

                    print(
                        f"[promoter] pronto: {file.name}",
                        flush=True
                    )

                except FileNotFoundError:
                    # O arquivo pode ter mudado entre a listagem e a ação.
                    pass

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()