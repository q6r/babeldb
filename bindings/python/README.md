# babeldb para Python

Binding nativo e embarcado do babeldb, no estilo de `sqlite3`, `lmdb`, `polars` ou `lancedb`: o
banco roda dentro do processo Python, sem servidor. Por baixo é a receita da seção "Usar em outro
projeto (Rust)" do README principal: fjall + WAL write-through (`Db::open_fjall_wal`), escritas
por um group committer, leituras direto no banco. Módulo nativo `babeldb._native` (PyO3, abi3),
pacote Python `babeldb` com tipos (`py.typed` + `.pyi`).

## Instalação

### A partir do wheel já construído

```powershell
pip install babeldb-0.1.0-cp38-abi3-win_amd64.whl   # o wheel gerado (veja abaixo); depois da publicação: pip install babeldb
```

O wheel é `abi3`: o mesmo arquivo serve para CPython 3.8 ou mais novo (Windows x64).

### Construir o wheel

Requisitos: Rust (o `rust-toolchain.toml` da raiz fixa a versão; o rustup a instala sozinho),
Python 3.8+ e maturin. De dentro de `bindings/python`:

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install maturin pytest
.venv\Scripts\maturin build --release -i .venv\Scripts\python.exe
.venv\Scripts\python -m pip install --force-reinstall target\wheels\babeldb-0.1.0-cp38-abi3-win_amd64.whl
```

O wheel sai em `target\wheels\`. Para desenvolver, `.venv\Scripts\maturin develop --release`
compila e instala direto no venv.

## Exemplo rápido

```python
import babeldb

with babeldb.open("dados/app") as db:              # cria o diretório se não existir
    rev = db.put("usuario:1", b'{"nome": "Ana"}')   # durável quando retorna
    db.get("usuario:1")                              # b'{"nome": "Ana"}'
    db.put("usuario:1", b'{"nome": "Bia"}', if_revision=rev)  # compare-and-set
    db.put("usuario:2", "x", if_absent=True)         # só cria se não existir
    db.scan("usuario:")                              # [(b'usuario:1', ...), (b'usuario:2', b'x')]
    db.keys("usuario:", reverse=True, limit=1)       # [b'usuario:2']
    db.batch([("put", "a", "1"), ("delete", "usuario:2")])  # atômico, um commit
    db.delete("a")                                   # True

    # Chat: chave = canal ‖ id da mensagem (big-endian), mensagens de um canal em ordem.
    canal = 42
    db.put(babeldb.message_key(canal, 1001), "oi")
    ultimas = db.scan(babeldb.channel_prefix(canal), reverse=True, limit=50)
    canal, msg_id = babeldb.parse_message_key(ultimas[0][0])
```

## Referência da API

Chaves e valores aceitam `bytes`, `bytearray`, `memoryview` ou `str` (`str` vira UTF-8); valores
voltam sempre como `bytes`. Chave: 1 a 4096 bytes. `help(babeldb.Db)` mostra a mesma referência.

| Chamada | Retorno | O que faz |
| --- | --- | --- |
| `babeldb.open(path, durability="immediate")` | `Db` | Abre (ou cria) o banco no diretório `path`. `babeldb.Db(path, durability=...)` é o mesmo. |
| `db.put(key, value, *, if_absent=False, if_revision=None)` | `int` | Grava; devolve a nova revisão. `if_absent=True`: só se a chave não existir. `if_revision=r`: só se a revisão atual for `r` (compare-and-set). Expectativa falhou: `ConflictError`. |
| `db.get(key)` | `bytes \| None` | Valor atual, ou `None`. |
| `db.delete(key, *, if_revision=None)` | `bool` | Apaga; `False` se a chave não existia. Com `if_revision=r`, `ConflictError` se a revisão atual não for `r` (inclusive chave inexistente). |
| `db.scan(prefix=None, *, start=None, end=None, reverse=False, limit=0)` | `list[tuple[bytes, bytes]]` | Registros em ordem de bytes da chave. `prefix`: chaves que começam com ele. `start` inclusivo, `end` exclusivo; `prefix` não combina com `start`/`end`. `reverse=True`: decrescente. `limit=0`: sem limite (com `reverse`, os últimos N). |
| `db.keys(...)` | `list[bytes]` | Os mesmos argumentos de `scan`, só as chaves. |
| `db.batch(ops)` | `list[int \| None]` | `ops`: iterável de `("put", k, v)` / `("delete", k)`. Atômico e durável; ver abaixo. |
| `db.sync()` | `None` | Espera tudo o que foi confirmado antes ficar durável (modo buffered; no immediate já está). |
| `db.close()` | `None` | Espera as chamadas em andamento, torna tudo durável e libera o diretório. Idempotente. |
| `db.closed`, `db.path`, `db.durability` | | Propriedades só de leitura. |
| `with babeldb.open(p) as db:` | | Fecha ao sair do bloco (também com exceção). |
| `babeldb.message_key(channel, message_id)` | `bytes` | 16 bytes: `channel` (u64 big-endian) ‖ `message_id` (u64 big-endian). |
| `babeldb.parse_message_key(key)` | `tuple[int, int] \| None` | Inverso de `message_key`; `None` se a chave não tiver 16 bytes. |
| `babeldb.channel_prefix(channel)` | `bytes` | Os 8 bytes iniciais das chaves do canal (para `scan`). |

**Revisões**: um `int` (u64) que cresce a cada escrita do banco (não por chave). Guarde a de
`put` para usar em `if_revision`.

**`batch` (tudo ou nada)**: as operações são aplicadas em ordem (uma operação vê o efeito das
anteriores na mesma chave) num único commit atômico e durável (`Db::write_batch` do núcleo). Se
qualquer operação for malformada ou inválida (por exemplo, chave vazia), nada é escrito e o erro
sobe. Retorno por operação: `put` → nova revisão; `delete` → revisão do registro apagado, ou
`None` se a chave não existia. As operações do batch não têm condições (último escritor vence) e
o batch é durável ao retornar nos dois modos de durabilidade. (O primitivo do group commit,
`write_batch_each`, aplicaria as operações válidas e pularia as inválidas; por isso `batch` não
usa ele.)

**Exceções** (todas derivam de `babeldb.BabelError`):

| Exceção | Quando |
| --- | --- |
| `ConflictError` | `if_absent=True` com a chave existente; `if_revision` diferente da revisão atual. Nada foi escrito. |
| `ClosedError` | Qualquer método (exceto `close`) depois de `close()`, e `with` num `Db` fechado. |
| `InvalidArgumentError` | Chave vazia ou com mais de 4096 bytes; valor grande demais; `durability` desconhecida; `prefix` junto com `start`/`end`; `limit` negativo; `if_absent` junto com `if_revision`; operação de batch malformada. |
| `BabelError` | Erros de E/S e do armazenamento; abrir um diretório que já está aberto neste processo. |

Tipos errados de chave/valor levantam `TypeError`; inteiros fora de u64 (revisões, helpers),
`OverflowError`.

## Modos de durabilidade

- **`"immediate"`** (padrão): `put`, `delete` e `batch` são duráveis quando a chamada retorna.
  Escritas simultâneas de várias threads entram no mesmo commit (group commit): N threads pagam
  uma escrita no WAL, não N.
- **`"buffered"`**: `put` e `delete` retornam quando a escrita está commitada e visível (leituras
  já a veem), mas ainda não durável. Ela fica durável em até ~100 ms, antes de 64 MiB pendentes,
  em `sync()` e em `close()` (`WriteDurability::Buffered { flush_interval: 100 ms,
  max_pending_bytes: 64 MiB }`). Num crash (queda de energia, processo morto) podem se perder as
  escritas confirmadas nos últimos ~100 ms. `batch` continua durável ao retornar.

O WAL grava em modo write-through (`FILE_FLAG_WRITE_THROUGH`): a garantia padrão do PostgreSQL no
Windows (ver `docs/` e o módulo `store::wal` do núcleo). Ao sair do interpretador, os bancos ainda
abertos são fechados (`atexit`), o que torna durável o que estiver pendente; um processo morto à
força não passa por isso.

## Threads

- Um `Db` pode ser usado por muitas threads ao mesmo tempo: compartilhe a mesma instância.
- Toda chamada ao banco roda sem o GIL (`py.detach`, o antigo `allow_threads` do PyO3): leituras
  rodam em paralelo e as escritas de threads diferentes compartilham commits.
- **Um `Db` por diretório por processo**: abrir de novo um diretório já aberto no mesmo processo
  levanta `BabelError` ("already open") na hora, sem travar. Outro processo que abra o mesmo
  diretório recebe o erro da trava do fjall.
- `close()` espera as chamadas em andamento terminarem; as chamadas seguintes levantam
  `ClosedError`. Um `Db` coletado pelo garbage collector sem `close()` é fechado ao ser destruído.
- Com `asyncio`, as chamadas bloqueiam: use `await asyncio.to_thread(db.get, chave)`.

## Limites

- Testado só no Windows 11 x64 com CPython 3.12. O wheel é abi3 (CPython 3.8+), mas as outras
  versões não foram testadas; Linux e macOS ainda não foram compilados.
- Chave de 1 a 4096 bytes. Chaves e valores são copiados a cada chamada: valores enormes custam
  memória (o limite do núcleo é 4 GiB por valor).
- `scan` e `keys` devolvem a lista inteira (sem cursor): pagine com `start`/`end` + `limit`.
- Transações: só `if_absent`/`if_revision` (por operação) e `batch` (atômico, sem condições).
- Configuração fixa: `Config::adaptive()` + `WalConfig::default()`. O banco ocupa o diretório
  `path` e o WAL é o arquivo `<path>.wal` ao lado dele (pré-alocado, 16 MiB).
- PyPy e o Python sem GIL (3.13t) não são suportados.

## Testes

De dentro de `bindings/python`, com o wheel instalado no venv (ver "Construir o wheel"):

```powershell
.venv\Scripts\python -m pytest -q
```

O pytest roda contra o pacote instalado: o código-fonte fica em `python/babeldb`, fora do
`sys.path`, então `import babeldb` carrega o wheel.

## Benchmark

```powershell
.venv\Scripts\python bench.py                        # durability="immediate"
.venv\Scripts\python bench.py --durability buffered
```

Imprime puts/s e gets/s com valores de 512 B (aleatórios, incompressíveis), 1 e 8 threads.

Medido nesta máquina de desenvolvimento (Windows 11, NVMe; CPython 3.12, wheel `--release`),
20 000 operações por fase, um banco novo por fase:

| durability | threads | puts/s | gets/s |
| --- | ---: | ---: | ---: |
| `immediate` | 1 | 8 472 | 320 000 |
| `immediate` | 8 | 34 834 | 129 870 |
| `buffered` | 1 | 29 452 | 299 472 |
| `buffered` | 8 | 113 013 | 151 644 |

Com 8 threads as escritas ganham com o group commit (várias escritas por commit). As leituras
com 8 threads rendem menos que com 1: cada `get` leva poucos microssegundos e o laço do benchmark
roda em Python, então a troca do GIL entre as threads (que toda chamada solta) custa mais do que
o paralelismo ganha. Leituras paralelas compensam quando cada chamada faz mais trabalho (valores
grandes, `scan`).
