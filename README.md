# babeldb

Armazenamento chave-valor embutido, em Rust, inspirado na *Biblioteca de Babel*
de Borges: a biblioteca que contém todos os livros possíveis, onde o difícil não
é guardar, e sim saber onde está o livro que interessa.

O babeldb leva essa ideia a sério em duas variantes explicitamente separadas e
mede o que cada uma custa de verdade: todo byte necessário para recuperar os
dados fica no arquivo local e entra na contabilidade.

## O que é, e o que não é

É:

- um banco chave-valor embutido (biblioteca + CLI), com reconstrução **exata**
  dos bytes, verificação por BLAKE3, commits atômicos e duráveis, leitura de
  intervalos, histórico opcional de revisões e importação de arquivos com
  memória limitada;
- um laboratório honesto para comparar representações: endereçamento
  reversível (BabelPure) contra a menor representação exata disponível
  (Adaptive), com os resultados sempre reportados separadamente.

Não é:

- **armazenamento zero.** Uma seed ou receita só é curta quando o dado tem uma
  estrutura que o sistema conhece (repetição, sequência aritmética, gerador
  registrado, duplicata, texto compressível). Para dados arbitrários a
  descrição tem, no pior caso, pelo menos o tamanho dos dados (argumento de
  contagem);
- compressão mágica: o modo **BabelPure não economiza espaço** — a seed tem
  exatamente o tamanho do valor, e cada unidade ainda paga 64 bytes de
  envelope;
- o algoritmo do site libraryofbabel.info, nem criptografia;
- um índice que "encontra" dados por hash: o BLAKE3 identifica e verifica, mas
  não reconstrói. A leitura vai de chave → manifesto → objetos, sem busca;
- dependente de rede: tudo funciona offline, e dicionários, templates e
  geradores necessários para ler um valor fazem parte do banco ou do binário
  (e da contabilidade).

## Modos

O modo é escolhido na criação do banco e gravado nele; misturar modos num mesmo
banco não é permitido, para não contaminar a comparação.

| modo | o que é gravado por unidade | para que serve | espaço |
|---|---|---|---|
| `BabelPure` | a seed da bijeção afim `seed = ((x − 1) · 5⁻¹) mod 2^(8n)`, com `x = (5 · seed + 1) mod 2^(8n)` na leitura (n bytes, big-endian) | demonstrar endereçamento reversível sem busca | igual a "motor + Raw" — nunca menor que os dados |
| `Adaptive` | a menor representação exata entre receitas (`RepeatV1`, `ArithmeticU64V1`), `RawV1`, LZ4, Zstd (com dicionário opcional) e templates, mais deduplicação de blocos com verificação byte a byte | economia real com custo de leitura controlado | medido em bytes alocados, metadados incluídos |

Valores até `inline_max` (padrão 16 KiB, o tamanho padrão de bloco) ficam
dentro do manifesto; valores maiores são divididos em blocos de `block_size`
bytes. Registros descritos por um gerador
registrado (`put_generated`, comando `gen`) guardam só o id do gerador, os
parâmetros e um digest.

## Começo rápido

Requer Rust 1.97.1 (fixado em `rust-toolchain.toml`; as dependências estão
travadas no `Cargo.lock`).

```sh
cargo build --release
cargo test
```

CLI (`babeldb --db <arquivo> [opções globais] <comando> [args]`; `data/` está
no `.gitignore`):

```sh
# gravar e ler (o banco é criado no primeiro put; modo Adaptive por padrão)
cargo run --bin babeldb -- --db data/demo.redb put k --value hello
cargo run --bin babeldb -- --db data/demo.redb get k
cargo run --bin babeldb -- --db data/demo.redb inspect k    # codec, envelope, refcount, tamanhos

# importar um arquivo byte a byte (memória limitada) e ler um intervalo
cargo run --bin babeldb -- --db data/demo.redb import lock Cargo.lock
cargo run --bin babeldb -- --db data/demo.redb range lock 0 64

# registro gerado: 1000 u64 LE 0, 1, 2, ... guardados como (gerador, params, digest)
cargo run --bin babeldb -- --db data/demo.redb gen seq arith 0 1 1000

# listar, contabilizar, verificar
cargo run --bin babeldb -- --db data/demo.redb scan --prefix k
cargo run --bin babeldb -- --db data/demo.redb stats
cargo run --bin babeldb -- --db data/demo.redb verify --deep

# outro banco, no modo BabelPure (--mode, --block-size e --inline-max valem na criação)
cargo run --bin babeldb -- --db data/babel.redb --mode babel-pure put k --value hello

# histórico de revisões (--history), sessão interativa e ajuda
cargo run --bin babeldb -- --db data/demo.redb --history put k --value v2
cargo run --bin babeldb -- --db data/demo.redb history k
cargo run --bin babeldb -- --db data/demo.redb repl
cargo run --bin babeldb -- help
```

Como biblioteca:

```rust
use babeldb::{Config, Db, Expect};

let db = Db::open("data/demo.redb", Config::adaptive())?;
let rev = db.put(b"usuario/42", b"hello", Expect::Any)?; // durável ao retornar
assert_eq!(db.get(b"usuario/42")?.as_deref(), Some(&b"hello"[..]));
db.put(b"usuario/42", b"outro", Expect::Revision(rev))?; // concorrência otimista
```

## Usar em outro projeto (Rust)

Versão estável: `0.1.1` no [crates.io](https://crates.io/crates/babeldb), no
[PyPI](https://pypi.org/project/babeldb/) e no [npm](https://www.npmjs.com/package/babeldb).
O CI testa Windows, Linux e macOS; as medições de desempenho são de Windows 11 (NVMe).

`Cargo.toml` do outro projeto:

```toml
[dependencies]
babeldb = { version = "0.1.1", features = ["fjall"] }
```

Embarcado no mesmo processo, a configuração que venceu PostgreSQL/MongoDB na comparação
(fjall + WAL write-through; `docs/comparison.md`). Exemplo completo que compila e roda:
`cargo run --release --example embedded --features fjall`.

```rust
use std::sync::Arc;
use babeldb::config::WalConfig;
use babeldb::scale::chat::{channel_prefix, message_key};
use babeldb::scale::group_commit::{GroupCommitConfig, GroupCommitter};
use babeldb::{Config, Db, Expect, ScanOptions};

let db = Arc::new(Db::open_fjall_wal("dados/babel", Config::adaptive(), WalConfig::default())?);
// Escritas de várias threads: group commit (um commit durável por lote).
let writer = GroupCommitter::new(db.clone(), GroupCommitConfig::default())?;

let key = message_key(canal, id); // canal BE ‖ id BE: as mensagens de um canal ficam em ordem
writer.put(key.to_vec(), payload.to_vec(), Expect::Any)?; // durável quando retorna
let valor: Option<Vec<u8>> = db.get(&key)?;
// as 50 mensagens mais novas do canal
let ultimas = db.scan(
    &ScanOptions::prefix(&channel_prefix(canal)).reverse(true).limit(50).with_values(true),
)?;
writer.delete(key.to_vec(), Expect::Any)?;
```

- As chamadas bloqueiam: num servidor tokio/axum, chame-as dentro de
  `tokio::task::spawn_blocking` e compartilhe `db` e `writer` com `Arc` (são `Send + Sync`).
- Um diretório é aberto por um processo por vez (trava exclusiva). Para vários processos ou
  outras linguagens há o servidor TCP (`babeldb --db <arquivo> serve --addr 127.0.0.1:7878`,
  cliente em `babeldb::cli::server::Client`), hoje com o backend redb na CLI.
- `Expect::Absent` falha se a chave existir; `Expect::Revision(r)` faz compare-and-set.

## Usar em Python e Node.js

Bibliotecas nativas embarcadas (o banco roda dentro do processo, como o SQLite), com a mesma
API nas duas linguagens: `open`, `put` (com `if_absent`/`ifAbsent` e compare-and-set por
revisão), `get`, `delete`, `scan`/`keys` por prefixo ou intervalo, `batch` atômico (tudo ou
nada), `sync`, `close`, e helpers de chave de mensagem (canal, id). Por enquanto só há binários
para **Windows x64**.

| linguagem | instalar | documentação |
|---|---|---|
| Python ≥ 3.8 | `pip install babeldb` | [`bindings/python/README.md`](bindings/python/README.md) |
| Node.js | `npm install babeldb` | [`bindings/node/README.md`](bindings/node/README.md) |

`dist/` não é versionado: gere os pacotes com `maturin build --release` (em `bindings/python`)
e `npx napi build --platform --release` + `npm pack` (em `bindings/node`); os READMEs das
bibliotecas explicam.

## Layout do repositório

| caminho | conteúdo |
|---|---|
| `src/format.rs` | formato persistente v1: envelope, manifesto, varint, chaves, params, fontes |
| `src/engine/` | `Db`: escrita, leitura, histórico, inspeção, contabilidade |
| `src/codec/` | codecs (`RawV1`, `RepeatV1`, `ArithmeticU64V1`, `Lz4V1`, `ZstdV1`, `BabelAffineV1`, `TemplatePatchV1`) |
| `src/planner.rs` | escolha da representação e treino de dicionários/templates |
| `src/generator/` | geradores registrados e versionados (`ARITH_U64`, `BLAKE3_XOF`, `REPEAT`) |
| `src/store/` | contrato transacional e backends (redb; LMDB com a feature `lmdb`) |
| `src/ingest/`, `src/source.rs` | importação em streaming e proveniência |
| `src/maintenance.rs` | `verify`, `gc`, `compact`, treino |
| `src/cache.rs`, `src/chunk.rs`, `src/hash.rs` | cache de blocos, divisão em blocos, BLAKE3 |
| `src/stats.rs`, `src/sys.rs` | contabilidade de espaço e métricas do processo |
| `src/cli/`, `src/bin/babeldb.rs` | CLI, REPL e protocolo TCP |
| `src/datasets.rs`, `benches/` | cenários sintéticos e benchmarks |
| `tests/` | testes de integração; `tests/format_compat.rs` e `tests/data/` congelam o formato v1 |
| `docs/` | especificação do formato e resultados de benchmark |

## Status das etapas

Etapas da especificação de projeto (§10), verificadas em 27/09/2026 pela suíte
`cargo test --features lmdb,fjall` e pelos benchmarks de `docs/benchmarks.md`.

| # | entrega | status |
|---|---|---|
| 0 | cenários sintéticos documentados (S1–S6) | feito (`benchdata/manifest.json`) |
| 1 | formato, `RawV1`, store redb, put/get/delete | feito |
| 2 | `BabelAffineV1` O(n) + oráculo `num-bigint` | feito (exaustivo em 1–2 bytes + proptest até 4 KiB) |
| 3 | benchmark Raw × BabelPure | feito — BabelPure ocupa o mesmo que Raw (medido) |
| 4 | Repeat, ArithmeticU64, LZ4, Zstd, seletor | feito |
| 5 | deduplicação + refcounts | feito (colisão forçada testada) |
| 6 | intervalos, block size, cache limitado, `put_generated` | feito |
| 7 | dicionários/templates, manutenção | feito (`verify`, `gc`, `compact`, treino com validação) |
| 8 | backends LMDB (heed) e fjall (LSM) | feito; passam na suíte de conformidade |
| 9 | recuperação, dados maiores que a RAM, compatibilidade de formato | recuperação (processo morto) e fixtures v1 feitos; benchmark > RAM disponível no harness, ainda não executado |
| 10 | CLI/REPL/servidor TCP | feito |
| + | escala tipo Discord: group commit, sharding, coalescing, chat | feito; comparação com PostgreSQL/MongoDB em `docs/comparison.md` |

## Documentação

- [`docs/format.md`](docs/format.md) — especificação do formato persistente v1
  (tabelas, envelope, manifesto, codecs, geradores, regras de compatibilidade e
  de contabilidade de espaço).
- [`docs/benchmarks.md`](docs/benchmarks.md) — metodologia e resultados dos
  benchmarks.
- [`docs/comparison.md`](docs/comparison.md) — babeldb × PostgreSQL × MongoDB locais
  (mesmos dados e operações, durabilidade equivalente) e o plano de otimização.
- [`docs/design.md`](docs/design.md) — especificação de projeto (Babel puro × adaptativo);
  [`docs/discord-scale-research.md`](docs/discord-scale-research.md) — pesquisa de escala tipo Discord.

## Estabilidade

Versão `0.x`: a API e o formato em disco podem mudar entre versões menores (o formato v1 é
verificado pelos testes de compatibilidade em `tests/format_compat.rs`, e mudanças de formato
são anotadas em `docs/format.md`). Os números de desempenho foram medidos numa única máquina
(Ryzen 7 5700, Windows 11, NVMe) e estão rotulados assim em `docs/comparison.md`.

## Licença

Licenciado sob a sua escolha de

- Apache License, Version 2.0 ([`LICENSE-APACHE`](LICENSE-APACHE)), ou
- MIT license ([`LICENSE-MIT`](LICENSE-MIT)).

Salvo declaração explícita em contrário, qualquer contribuição enviada para inclusão no
babeldb, conforme definido na licença Apache-2.0, é licenciada da mesma forma dupla, sem
termos ou condições adicionais.

## Créditos

A organização em módulos, os metadados de origem e a ideia da linha de comando foram
inspirados conceitualmente no projeto [margostino/babeldb](https://github.com/margostino/babeldb)
(Go, Apache-2.0). Nenhum código dele foi copiado.
