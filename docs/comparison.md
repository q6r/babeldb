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

## Rodada 2 (27/09/2026, noite)

Mudanças integradas: WAL próprio com write-through na frente do redb (`Db::open_wal`),
reuso de snapshots de leitura + pool de handles de leitura no redb, CPU do motor menor e
dicionário zstd automático, group commit no servidor TCP. Mesmas condições da rodada 1
(100 000 × 512 B, durabilidade equivalente, 1 repetição, mesma máquina; commit `c9…` do
harness com as variantes `babel-wal*`). Saída bruta: `bench-results/compare-round2.txt`.
Nesta rodada os sistemas babeldb rodam as fases de leitura/escrita **depois** de
`Db::compact` (o harness compacta após a carga; `--no-compact` volta às condições da rodada 1).

Garantia de durabilidade do WAL padrão (`WalSync::WriteThrough`): a mesma do padrão do
PostgreSQL no Windows (`open_datasync`, `FILE_FLAG_WRITE_THROUGH`) — que, segundo a própria
documentação do PostgreSQL e o `CreateFile` da Microsoft, **não garante** atravessar o cache
volátil do disco numa queda de energia. Os modos mais fortes também foram medidos:
`wal-strict` (`WRITE_THROUGH | NO_BUFFERING`, FUA pedido ao disco) e `wal-flush`
(`FlushFileBuffers`, a garantia do redb sozinho). Nenhum teste de queda de energia foi feito.

### Comparação justa (cliente/servidor, localhost TCP) — ops/s

| operação | babeldb TCP + WAL (raw) | babeldb TCP + WAL (adaptive) | PostgreSQL | MongoDB |
|---|---|---|---|---|
| carga em lote (registros/s) | 46 000 | 55 000 | **87 000** | 40 000 |
| put durável, 1 cliente | **3 908** | 3 311 | 3 590 | 244 |
| put durável, 4 clientes | 9 336 | 8 461 | **9 822** | 571 |
| put durável, 16 clientes | 15 323 | 13 750 | **25 192** | 2 028 |
| put durável, 64 clientes | **24 910** | 14 930 | 24 628 | 11 494 |
| get, 1 cliente | **15 429** | 15 038 | 7 828 | 3 745 |
| get, 4 clientes | **59 778** | 57 377 | 27 915 | 11 410 |
| get, 16 clientes | 120 497 | **120 449** (dict 127 301) | 54 994 | 21 980 |
| últimas 50, 1 cliente | 5 599 | **6 816** | 2 860 | 1 678 |
| últimas 50, 4 clientes | 24 175 | 26 106 (dict 27 389) | 7 366 | 6 566 |
| últimas 50, 16 clientes | 52 904 | 50 809 (dict **65 281**) | 17 819 | 13 592 |

### Embarcado (dentro do processo, sem rede) — ops/s

| operação | WAL raw | WAL adaptive | WAL estrito (FUA) | WAL flush | redb sem WAL |
|---|---|---|---|---|---|
| carga em lote (registros/s) | 58 000 | 69 000 | 53 000 | 50 000 | 26 000 |
| put durável, 1 thread | 3 915 | 4 987 | 5 618 | 729 | 444 |
| put durável, 64 threads | 31 070 | 25 625 | **35 516** | 20 695 | 5 382 |
| get, 16 threads | 1 736 650 | 2 228 670 | 1 375 316 | 1 994 876 | 1 834 271 |
| últimas 50, 16 threads | 182 999 | 286 273 | 157 609 | 167 945 | 171 542 |

### Espaço — só arquivos de dados (logs excluídos para todos: WAL do babeldb, WAL do PG, journal do Mongo)

| sistema | recém-carregado | após compactação |
|---|---|---|
| babeldb + dicionário zstd | 67,4 MB | **31,5 MB** |
| babeldb adaptive | 67,4 MB | 34,8 MB |
| babeldb raw | 134,7 MB | 69,6 MB |
| PostgreSQL (heap + índice) | 62,6 MB | — |
| MongoDB (storage + índice, snappy) | **32,1 MB** | — |

O WAL do babeldb é um arquivo pré-alocado de 16 MiB (fixo). Os números de espaço desta tabela
foram obtidos da execução da rodada 2 subtraindo esse arquivo (a partir desta versão o
harness já reporta dados e WAL separados).

### Placar após a rodada 2

- **Vencemos**: todas as leituras (2–4× o PostgreSQL via TCP; 15–40× embarcado), put durável
  com 1 e 64 clientes via TCP, todas as escritas embarcadas, espaço compactado (empate com o
  Mongo, metade do PG).
- **Perdemos**: carga em lote (55 k/s vs 87 k/s do PG), put durável via TCP com 4 e 16
  clientes (9,3 k e 15,3 k vs 9,8 k e 25,2 k do PG), escrita concorrente com codec adaptive
  (a codificação ainda roda na thread única do committer), espaço **sem** compactação.

## Rodada 3 (planejada)

1. Codificar os valores nas threads de quem escreve (TCP workers / threads da aplicação), não
   na thread do group committer; usar `put_many` no motor; acelerar a carga em lote.
2. Perfilar o caminho de escrita concorrente do servidor TCP (16 clientes: 15 k/s vs 30 k/s
   embarcado).
3. Espaço sem compactação manual: envelope inline compacto (os 64 B por registro pesam em
   mensagens de ~185 B comprimidas) e compactação/reescrita incremental.
