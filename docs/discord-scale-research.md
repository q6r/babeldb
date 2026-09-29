# Pesquisa: leitura e escrita rápidas em escala tipo Discord

Pesquisa feita em 27/09/2026 com fontes primárias (blogs de engenharia do Discord, documentação e código do redb/RocksDB/fjall/LMDB, artigos). [F] = afirmação da fonte (números citados literalmente); [G] = conhecimento geral; [I] = inferência nossa. Nenhum número abaixo foi medido no babeldb — as medições próprias ficam em especificação §11 e docs/benchmarks.md.

## 1. O que o Discord fez [F]

| data | fonte | fatos relevantes |
| --- | --- | --- |
| 2017 | *How Discord Stores Billions of Messages* | MongoDB degradou a ~100 M mensagens (índice fora da RAM). Cassandra com chave `((channel_id, bucket), message_id)`, message_id = Snowflake, bucket ≈ 10 dias (< 100 MB). "Últimas mensagens" = varrer buckets do mais recente para trás. Leitura/escrita ~50/50, 120 M msgs/dia, 12 nós RF=3; "Writes were sub-millisecond and reads were under 5 milliseconds". Incidente: milhões de tombstones num canal → pausas de GC de 10–20 s. |
| 2020 | *Why Discord is switching from Go to Rust* | Serviço Read States: LRU com dezenas de milhões de entradas, centenas de milhares de updates/s, **write-behind** (commit agendado 30 s depois). Pausas do GC do Go a cada 2 min; Rust eliminou os picos. |
| 2022 | *Supercharges Network Disks* | ~2 M req/s; 1,25–1,5 M leituras/s vs ~0,1 M escritas/s (≈93 % leitura, [I]). Disco em rede ≈ ms vs 0,5 ms local; "super-disk" = RAID0 de SSD local + RAID1 com disco persistente. |
| 2023 | *How Discord Stores Trillions of Messages* | 177 nós Cassandra → 72 ScyllaDB. **Hot partitions** (um canal/bucket muito lido). **Data services em Rust** com **request coalescing** ("we'll only query the database once") e **roteamento por hash consistente por canal**. p99 de leitura histórica 40–125 ms → 15 ms; inserção 5–70 ms → 5 ms (cluster distribuído, não comparável a embarcado). |
| 2025 | *Indexes Trillions of Messages* | Lotes agrupados **por destino** (um nó lento não derruba o lote inteiro). |

Snowflake: 42 bits de ms desde 1420070400000, 5 bits worker, 5 bits process, 12 bits de incremento.

**O que transfere para um banco embarcado [I]:** os mecanismos, não os números de cluster — chave canal + tempo ordenável, varredura reversa para "últimas N", roteamento por canal, coalescing, lotes por destino, write-behind com janela explícita, ausência de pausas de GC, disco local de baixa latência.

## 2. Técnicas de engine e trade-offs

| técnica | ganho | custo | evidência |
| --- | --- | --- | --- |
| B+tree copy-on-write (redb, LMDB) | leitura e range scan baratos; redb faz 1 fsync por commit | um escritor; cada commit reescreve o caminho de páginas; fragmentação | redb design.md [F] |
| LSM (RocksDB, fjall, ScyllaDB) | escrita sequencial, escritores concorrentes | amplificação de leitura, compactação, tombstones | RUM conjecture, EDBT 2016 [F] |
| **Group commit** | 1 fsync para N escritas | janela de espera soma latência | RocksDB/PostgreSQL [F] |
| Shard-per-core / N arquivos | N escritores independentes | atomicidade só dentro do shard | ScyllaDB [F] |
| Coalescing (singleflight) | uma leitura por chave quente concorrente | nada retido; sem staleness | Discord 2023 [F] |
| LZ4 vs Zstd por bloco | LZ4 descompressão ~2,5× mais rápida (README zstd, Silesia) | Zstd comprime mais | [F] |

**Custo de durabilidade [F]:** fsync em SSD de consumo sem proteção contra queda de energia leva ~0,9–3 ms (Small Datum, jan/2026); SSD enterprise com PLP ~1,6–12 µs. No Windows, `File::sync_data()` → `FlushFileBuffers`; redb `Immediate` = um `FlushFileBuffers` por commit. **Não há medição rigorosa de `FlushFileBuffers` nesta máquina — deve ser medida.**

## 3. Engines Rust (versões em 27/09/2026) [F]

| engine | versão | notas |
| --- | --- | --- |
| redb | 4.3.0 | B+tree CoW, 1 escritor; `Durability::{None, Immediate}` — `None` persiste com o próximo `Immediate`; 4.2: escritas em tabelas distintas da mesma transação escalam com threads; 3.1.3/4.2/4.3 corrigiram bugs de perda/corrupção |
| fjall | 3.1.10 | LSM, LZ4, separação chave/valor, transações single-writer ou otimistas; `PersistMode::{Buffer, SyncData, SyncAll}` |
| heed/LMDB | 0.22.1 | mmap, escrita serializada; `NOSYNC` perde D |
| rocksdb | 0.25.0 (RocksDB 11.8.1) | LSM C++, group commit nativo |
| sled | 0.34.7 / 1.0 alpha | "sled is beta" |

Benchmark **do próprio redb** (alegação do projeto, Ryzen 9950X3D, 5 M pares): 1.000 txns de 1 insert: redb 920 ms, lmdb 1598, rocksdb 2432, fjall 3488. 100 txns de 1.000: redb 1595, lmdb 942, rocksdb 451, fjall 353. Leituras 32 threads: redb 410, lmdb 125. Remoções: redb 23297, fjall 6004. [I] em lote, o redb insere ~58× mais por segundo do que com 1 insert por commit — o fsync domina.

## 4. Recomendações adotadas no babeldb

| # | recomendação | onde está sendo implementada | medir |
| --- | --- | --- | --- |
| 1 | **Group commit**: thread escritora dedicada, fila limitada, lote = 1 commit/fsync; níveis `Immediate` e `Buffered` (janela de perda explícita) | `src/scale/group_commit.rs` sobre `Db::write_batch_each` | ops/s e p99 por nº de escritores |
| 2 | **Chave de chat** `channel_id BE ‖ message_id BE` + varredura reversa "últimas N" | `src/scale/chat.rs`, `ScanOptions::prefix().reverse().limit()` | p99 de `latest(50)` |
| 3 | **Coalescing** de leituras quentes + cache de página recente por canal (write-through) | `src/scale/coalesce.rs` | hit rate, p99 sob Zipf |
| 4 | Codec por tamanho: mensagens pequenas inline/raw/LZ4; Zstd para frio/grande; dedupe só vale acima de um limiar | planner + benchmark S3 | CPU por escrita, bytes |
| 5 | Orçamento de cache em camadas (cache do SO + redb + babeldb) | `Config::cache_bytes`/`backend_cache_bytes` | RSS e acerto |
| 6 | **Sharding**: N bancos roteados por canal = N escritores concorrentes | `src/scale/sharded.rs` | throughput com 1/2/4/8 shards |
| 7 | Backends comparados no mesmo harness: redb, LMDB, **fjall (LSM)** | `src/store/{redb,heed,fjall}.rs` | mesma carga e durabilidade |
| 8 | Especulativo (só se as medições pedirem): WAL próprio, write-through no Windows, io_uring | — | — |
| 9 | Robustez: versão fixada, testes de queda | `Cargo.lock`, `tests/recovery.rs` | — |

**Medir, não presumir:** latência de `FlushFileBuffers` neste disco; p99 de commit por janela de grupo e nº de escritores; leituras com dataset > cache; hit rate; amplificação de escrita/espaço; custo de BLAKE3 e codecs; cauda durante compactação.

## 5. Fontes (acesso 27/09/2026)

- discord.com/blog: how-discord-stores-billions-of-messages; how-discord-indexes-billions-of-messages; why-discord-is-switching-from-go-to-rust; how-discord-supercharges-network-disks-for-extreme-low-latency; how-discord-stores-trillions-of-messages; how-discord-indexes-trillions-of-messages; how-discord-automates-scylladb-clusters-at-scale; docs.discord.com/developers/reference (Snowflakes)
- openproceedings.org/2016/conf/edbt/paper-12.pdf (RUM); usenix.org FAST'16 WiscKey
- github.com/facebook/rocksdb/wiki (Tuning Guide, WAL-Performance, Block-Cache, Asynchronous-IO); postgresql.org/docs/current/wal-configuration.html
- scylladb.com (shard-per-core; internal cache 2024-01-08); github.com/facebook/zstd; github.com/lz4/lz4
- smalldatum.blogspot.com/2026/01/ssds-power-loss-protection-and-fsync.html; percona.com/blog/fsync-performance-storage-devices
- learn.microsoft.com FlushFileBuffers e ioringapi; rust-lang/rust library/std/src/sys/fs/windows.rs
- github.com/cberner/redb (README, design.md, CHANGELOG, benches, issue #1507); docs.rs/redb
- fjall-rs.github.io (fjall 3); github.com/fjall-rs/fjall; lmdb.h; github.com/spacejam/sled; crates.io API
- Inacessíveis: PDF do RUM em stratos.seas.harvard.edu (403), Phoronix (403), rust-storage-bench (JS)
