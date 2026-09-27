# babeldb × PostgreSQL × MongoDB — comparação local

Harness: `benches/compare.rs` (`cargo bench --bench compare --features compare`). Credenciais
só por variável de ambiente (`BABEL_PG_URL`); cada execução recria apenas o banco isolado
`babeldb_bench` em cada servidor.

## Condições da rodada 1 (27/09/2026)

- Máquina: AMD Ryzen 7 5700 (8C/16T), 31,8 GB RAM, Windows 11 Pro 10.0.26200, NVMe KINGSTON
  SNV3S1000G (NTFS, cluster 4 KiB), rustc 1.97.1, commit `c6a92f5` + harness.
- PostgreSQL 17.10 (padrões: `synchronous_commit=on`, `fsync=on`,
  `wal_sync_method=open_datasync`, `shared_buffers=128MB`); MongoDB 8.3 (WiredTiger padrão,
  snappy); ambos em localhost, sem outras cargas.
- Dados: 100 000 mensagens de chat S3 (`datasets`), ~512 B cada, chave (canal, snowflake).
  Tabela PG `messages(channel_id bigint, id bigint, payload bytea, PK(channel_id, id))`;
  coleção Mongo com `_id = {c, m}` e `p = BinData`; babeldb com chave `canal BE ‖ id BE`.
- Durabilidade equivalente ("durable"): babeldb `Immediate`; PG `synchronous_commit=on`;
  Mongo `{w: 1, j: true}`. 1 repetição; cache do SO quente.
- `babel-raw`/`babel-adaptive` rodam **dentro do processo** (sem rede) — vantagem natural de
  um banco embarcado; `babel-tcp` é o mesmo motor atrás do servidor TCP do babeldb, a
  comparação justa com os servidores PG/Mongo.

## Resultados (ops/s; maior é melhor)

| operação | babel-raw (embarcado) | babel-adaptive (embarcado) | babel-tcp | PostgreSQL | MongoDB |
|---|---|---|---|---|---|
| carga em lote (registros/s) | 16 168 | 14 840 | 13 988 | **67 847** | 31 742 |
| put durável, 1 thread | 502 | 434 | 313 | **3 988** | 370 |
| put durável, 4 threads | 810 | 749 | 324 | **10 102** | 1 321 |
| put durável, 16 threads | 2 537 | 2 828 | 332 | **23 007** | 2 806 |
| put durável, 64 threads | 5 067 | 4 364 | 380 | **24 838** | 8 982 |
| get por chave, 1 thread | **117 898** | 70 737 | 14 875 | 7 287 | 2 822 |
| get, 4 threads | **199 560** | 183 217 | 48 163 | 26 674 | 13 294 |
| get, 16 threads | 75 212 | **85 274** | 40 898 | 63 751 | 21 232 |
| últimas 50 do canal, 1 thread | **11 915** | 5 234 | 3 510 | 2 745 | 404 |
| últimas 50, 4 threads | **49 291** | 17 958 | 12 590 | 9 842 | 1 204 |
| últimas 50, 16 threads | **107 443** | 37 535 | 28 223 | 23 412 | 3 156 |
| espaço em disco (MB, menor é melhor) | 134,75 (payload 61,3) | 134,75 (payload 46,7) | 134,75 | 62,62 | **32,10** |

Latências p50/p99 de cada linha: `bench-results/compare-round1.txt` e `compare.jsonl`.

## Diagnóstico

1. **Commit durável 8× mais lento que o PostgreSQL.** PG usa `open_datasync` (WAL gravado com
   *write-through*, `FILE_FLAG_WRITE_THROUGH`/FUA no Windows): ~0,24 ms por commit. O redb faz
   um `FlushFileBuffers` (flush do cache inteiro do disco) por commit: ~1,9 ms. Mesma garantia
   de durabilidade com um WAL próprio em write-through → alvo principal da rodada 2.
2. **Servidor TCP sem group commit**: cada PUT paga seu próprio commit; 16–64 clientes
   enfileiram (332–380 ops/s).
3. **Leituras com 16 threads caem** (embarcado: 199 k/s com 4 threads → 75 k/s com 16): cada
   `begin_read` do redb 4.3 trava o mutex global do `TransactionTracker` (lido no código-fonte).
4. **Carga em lote 4× mais lenta que o COPY do PG** (~50 µs por registro).
5. **Espaço**: o arquivo do redb cresce em regiões de potência de 2 (134,75 MB para 47–61 MB
   de payload); Mongo comprime páginas (snappy). Dicionário zstd + compactação são os
   candidatos.
6. **Onde já vencemos**: leituras por chave e "últimas 50" — embarcado 4–30× à frente; via TCP
   ainda 2× à frente do PG com 1 thread.

## Rodada 2 (em andamento)

WAL write-through + commits `Deferred` no redb com checkpoint; group commit no servidor;
reuso de snapshots de leitura; redução de CPU por operação; dicionário zstd e compactação.
Os resultados entram aqui, sempre com as mesmas condições.
