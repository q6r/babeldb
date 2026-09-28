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
(100 000 × 512 B, durabilidade equivalente, 1 repetição, mesma máquina; commit `a1ee6b9` do
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
  com 1 e 64 clientes via TCP, todas as escritas embarcadas unitárias e concorrentes (a carga
  em lote embarcada, 69 k/s, ainda perde), espaço compactado (empate com o
  Mongo, metade do PG).
- **Perdemos**: carga em lote (55 k/s vs 87 k/s do PG), put durável via TCP com 4 e 16
  clientes (9,3 k e 15,3 k vs 9,8 k e 25,2 k do PG), escrita concorrente com codec adaptive
  (a codificação ainda roda na thread única do committer), espaço **sem** compactação.

## Rodada 3 (28/09/2026)

Mudanças integradas: valores pré-codificados nas threads de quem escreve (`Db::prepare`,
`write_prepared_each`, group commit com preparo no chamador), carga em lote com pipeline e
`put_many`/`get_many`; backend **fjall (LSM) + WAL** (`Db::open_fjall_wal`, blocos LZ4);
prioridade elevada da thread de commit no servidor TCP. Mesmas condições (100 000 × 512 B,
durabilidade equivalente, 1 repetição; o PostgreSQL também rendeu mais nesta execução do que
na rodada 2). Saída bruta: `bench-results/compare-round3.txt`. Espaço: só arquivos de dados
(WAL do babeldb, journal do fjall, WAL do PG e journal do Mongo excluídos).

### Comparação justa (localhost TCP) — ops/s

| operação | babeldb fjall+WAL | babeldb fjall+WAL+dict | babeldb redb+WAL raw | PostgreSQL | MongoDB |
|---|---|---|---|---|---|
| carga em lote (registros/s) | **147 000** | 138 000 | 81 000 | 121 000 | 78 000 |
| put durável, 1 cliente | 4 405 | **4 748** | 3 627 | 4 250 | 640 |
| put durável, 4 clientes | 12 681 | **13 005** | 10 590 | 11 715 | 1 479 |
| put durável, 16 clientes | 26 626 | 23 990 | 22 584 | **29 537** | 6 114 |
| put durável, 64 clientes | 29 642 | 29 739 | **35 975** | 35 480 | 16 404 |
| get, 1 cliente | 14 996 | 16 411 | 16 562 (dict redb 18 621) | 9 415 | 4 085 |
| get, 16 clientes | 145 697 | 142 923 | 133 382 (dict redb **150 819**) | 79 883 | 25 209 |
| últimas 50, 1 cliente | 5 667 | 5 590 | 7 055 (adaptive redb **7 693**) | 4 174 | 1 498 |
| últimas 50, 16 clientes | 65 880 | 56 748 | 62 977 (dict redb **78 766**) | 31 474 | 10 832 |
| espaço dos dados, recém-carregado | 29,0 MB | **25,8 MB** | 134,7 MB | 62,6 MB | 32,1 MB |

### Embarcado (dentro do processo) — destaques

| operação | fjall+WAL | redb+WAL (adaptive) |
|---|---|---|
| carga em lote (registros/s) | 191 000 | 132 000 |
| put durável, 1 / 16 / 64 threads | 6 661 / 48 094 / **99 726** | 5 138 / 30 206 / 51 701 |
| get, 16 threads | 1 848 587 | **3 146 287** |
| últimas 50, 16 threads | 157 476 | **347 873** |

### Placar após a rodada 3

- Uma única configuração (**fjall + WAL, via TCP**) vence o PostgreSQL e o MongoDB em 10 de 12
  métricas: carga em lote, put durável com 1 e 4 clientes, todas as leituras (1,6–2,1× o PG),
  "últimas 50" e espaço sem compactação manual (29 MB vs 32 MB do Mongo e 63 MB do PG).
- **Perde**: put durável via TCP com 16 clientes (26,6 k vs 29,5 k do PG) e com 64 clientes
  (29,6 k vs 35,5 k; o redb+WAL raw empata: 36,0 k).
- Embarcado, vence tudo: até 99,7 k escritas duráveis/s e 3,1 M leituras/s.
- O placar vale para esta carga (mensagens de chat de 512 B, chave → valor, "últimas N"). O
  PostgreSQL e o MongoDB seguem tendo SQL/consultas, índices secundários e replicação.

## Rodada 4 (28/09/2026)

Mudanças integradas: o sink de commit do servidor TCP passa a pré-codificar os valores nas
threads das conexões (falha de integração da rodada 3: tudo era codificado na thread única de
commit); notificação dos clientes em árvore (a thread de commit acorda 2 waiters, cada um acorda
mais 2); requisições grandes (≥ 64 ops ou ≥ 1 MiB) seguem o caminho antigo para não perder
carga em lote. Harness com fases opcionais: `update`, `delete`, `range`, `mixed95`, `mixed50`,
`--scenario` (s3 texto, s5 incompressível), valores de até 1 MiB. Sistemas:
`babel-fjall-wal-tcp` (a comparação justa), `babel-fjall-wal` (embarcado; o único que roda
`range`, porque o protocolo TCP ainda não tem varredura por intervalo de chaves),
`babel-wal-tcp-raw`, PostgreSQL 17, MongoDB 8.3. 1 repetição por execução; saídas brutas em
`bench-results/compare-r4-*.txt`.

### Placar por carga (babeldb via TCP vs o melhor entre PostgreSQL e MongoDB)

| carga | fases vencidas | onde perde ou empata |
|---|---|---|
| chat 100 k × 512 B, durável | **26 de 31** | carga em lote 119 k vs 120 k/s e delete 1 cliente 4 270 vs 4 333/s (empates no ruído); `range` sem suporte TCP |
| chat 100 k × 512 B, relaxado | **28 de 31** | só `range` (sem suporte TCP) |
| 50 k × 4 KiB (texto) | 23 de 31 | update/delete concorrentes (0,68–0,97 do PG) |
| 10 k × 64 KiB (incompressível) | 17 de 31 | `latest` (0,63–0,82 do PG), update/delete concorrentes (até 0,35 do Mongo), put com 4 clientes |
| 2 k × 1 MiB (texto) | 10 de 25 (+2 empates) | put, get, update e delete (0,22–0,92); `range` sem suporte TCP |
| 2 k × 1 MiB (incompressível) | 2 de 25 | quase tudo; delete concorrente até 0,05 do Mongo |

### Destaques da carga de chat (ops/s, durável, localhost TCP)

| operação | babeldb fjall+WAL | PostgreSQL | MongoDB |
|---|---|---|---|
| put durável, 16 / 64 clientes | **44 770 / 72 462** | 30 379 / 31 445 | 6 470 / 17 140 |
| update, 16 / 64 clientes | **33 719 / 44 220** | 16 693 / 19 574 | 6 396 / 15 061 |
| delete, 16 / 64 clientes | **36 957 / 51 842** | 34 111 / 32 467 | 6 475 / 17 151 |
| get, 16 clientes | **128 339** | 66 555 | 24 148 |
| últimas 50, 16 clientes | **60 929** | 23 331 | 10 877 |
| mistura 95/5, 16 clientes | **74 754** | 44 419 | 18 953 |
| mistura 50/50, 16 clientes | **55 344** | 39 284 | 10 049 |
| intervalo de 200 msgs, 16 threads (babeldb embarcado) | 28 755 | 12 232 | 4 777 |
| espaço dos dados | **28,5 MB** | 62,6 MB | 32,1 MB |

### Diagnóstico dos valores grandes

1. `delete`/`update` liberam os objetos do valor antigo lendo o envelope inteiro de cada bloco
   só para achar o digest do índice de dedupe (1 MiB lido para apagar 1 MiB).
2. Valores grandes são escritos no WAL, no journal do fjall e nas SSTables; a transferência TCP
   de valores grandes fica em ~330 MB/s (medido na rodada 4).
3. O protocolo TCP não tem varredura por intervalo de chaves (só prefixo + últimas N).
4. Espaço de valores incompressíveis grandes no redb: ~2× (blocos de 16 KiB + cabeçalho não cabem
   numa página de 16 KiB); no fjall compactado empata com PG/Mongo, mas antes de compactar fica
   acima.

## Rodada 5 (28/09/2026)

Mudanças integradas:

- **Motor:** liberar um objeto remove só as linhas do objeto e do refcount, sem ler o corpo
  (ids obsoletos na lista de candidatos de dedupe são tolerados e varridos pelo `gc`); ids de
  objetos alocados uma vez por transação; refcount de objeto novo escrito sem leitura;
  `inline_max` padrão 1 KiB → 16 KiB (bancos existentes mantêm o seu).
- **Protocolo:** operação `SCAN_RANGE` (intervalo de chaves com limite, ordem reversa e
  paginação); buffer de recepção de 4 MiB nos sockets (Windows), valores ≥ 4 KiB escritos
  direto dos seus buffers (escrita vetorizada) e lidos direto no destino.
- **fjall:** separação chave/valor (valores ≥ 1 KiB em blob files, padrão de
  `FjallOptions::for_wal`); `compact` recolhe todo o lixo dos blobs.
- **Vigia de compactação:** o fjall 3.1.10 pausa os commits com 30 execuções no nível 0 e só
  agenda compactações depois de descarregar memtables; com os commits pausados, nada mais
  agendava compactação e os escritores dormiam para sempre (reproduzido: 16–64 escritores de
  valores de 1 MiB sem separação, 0% de CPU). Uma thread agora compacta todo keyspace com 20+
  execuções no nível 0 (detalhes em `src/store/fjall.rs`, *Compaction watchdog*).

Condições: a primeira bateria desta rodada foi descartada, porque o emulador MEmu e o Spotify
consumiam ~9 núcleos durante as medições (o `get` embarcado do fjall mediu 49 k/s em vez de
~200 k/s). Um A/B com a máquina quieta (binário da rodada 4 × atual × atual sem separação
chave/valor, alternados, 2 vezes cada) não mostrou regressão do fjall em valores pequenos. A
bateria abaixo foi repetida com a máquina quieta; 1 repetição por carga; saídas em
`bench-results/compare-r5-*.txt`.

### Placar (babeldb via TCP vs o melhor entre PostgreSQL e MongoDB)

| carga | rodada 4 | rodada 5 | onde ainda perde |
|---|---|---|---|
| chat 100 k × 512 B, durável | 26 de 31 | **28 de 31** | update 1 cliente (0,91), update 16 clientes (0,98), delete 16 clientes (0,86) |
| chat, relaxado | 28 de 31 | **28 de 31** | put 1 cliente (0,74–0,81 do PG); delete 64 clientes empata |
| 50 k × 4 KiB (texto) | 23 de 31 | **29 de 31** | delete 4 e 16 clientes (0,89–0,92 do PG) |
| 10 k × 64 KiB (incompressível) | 17 de 31 | **26 de 31** | put 4/16 clientes (0,85–0,87), update concorrente (0,73–0,87 do PG) |
| 2 k × 1 MiB (texto) | 10 de 25 (+2 empates) | **17 de 25 (+2 empates)** | put (0,64–0,77 do Mongo), update 16 clientes (0,78), delete 16 clientes (0,51) |
| 2 k × 1 MiB (incompressível) | 2 de 25 | **8 de 25 (+2 empates)** | put, latest, range, misturas, update (0,62–0,98), delete 16 clientes (0,28 do Mongo) |

Total: 106 → **136 de 170 fases** vencidas.

### Valores grandes: babeldb fjall+WAL via TCP, rodada 4 → rodada 5 (ops/s)

| carga e operação | rodada 4 | rodada 5 | PostgreSQL | MongoDB |
|---|---|---|---|---|
| 4 KiB, update 16 clientes | 10 844 | **18 088** | 10 768 | 5 969 |
| 4 KiB, put 16 clientes | 21 023 | **33 449** | 14 574 | 5 992 |
| 64 KiB, delete 16 clientes | 1 992 | **13 877** | 3 340 | 3 418 |
| 64 KiB, get 1 cliente | 3 134 | **5 135** | 2 786 | 2 519 |
| 64 KiB, últimas 50 | 88 | **239** | 161 | 150 |
| 1 MiB texto, get 1 cliente | 90 | **165** | 124 | 134 |
| 1 MiB texto, delete 16 clientes | 238 | 815 | 509 | **1 607** |
| 1 MiB incompr., update 16 clientes | 24 | 76 | 40 | **120** |
| 1 MiB incompr., delete 16 clientes | 34 | 488 | 117 | **1 732** |

Espaço dos dados (MB): chat 29,2 (PG 62,6, Mongo 32,1); 4 KiB 91,2 (144,3 / 124,1); 64 KiB
662,8 (684,7 / 696,8); 1 MiB texto 853 (957 / 1 107); 1 MiB incompressível 2 121,7 — acima do
Mongo (2 111,8) por 0,5%: o envelope de 64 B por bloco de 16 KiB custa 0,4%.

### Pontos a investigar

- Update e delete de valores pequenos via TCP mediram 0,59–0,78 da rodada 4 (ex.: update 16
  clientes 33 719 → 19 844/s), enquanto put, get e misturas ficaram iguais ou melhores; o PG
  variou ±20–30% entre as duas rodadas. Ainda não se sabe se é regressão ou ruído: precisa de
  A/B repetido.
- Modo relaxado: put com 1 cliente 8 777 → 6 600/s e latest 1 cliente 5 819 → 3 738/s.

## Rodada 6 (planejada)

1. A/B repetido de update/delete de valores pequenos e de operações de 1 cliente (rodada 4 ×
   rodada 5); corrigir a regressão se ela se confirmar.
2. Valores de 1 MiB: delete e update concorrentes (Mongo 2–3,5× à frente), put com 1–16 clientes.
3. Valores de 64 KiB: put e update concorrentes.
4. Espaço de valores grandes incompressíveis: blocos maiores ou envelope compacto.
