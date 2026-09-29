# Log de valores: gravar valores grandes uma vez só — design

> **Estado:** proposta para a rodada 7. Nada implementado. Base: `master` `b217bfe` (rodada 5).
>
> **Rótulos usados no texto**
>
> | rótulo | significado |
> |---|---|
> | **[código]** | fato lido no código; o arquivo e o símbolo são citados |
> | **[medido]** | número de `bench-results/compare-r5-*.txt` ou de `docs/comparison.md` |
> | **[hipótese]** | estimativa não medida |
> | **[doc PG]**, **[doc Mongo]** | afirmação da documentação oficial |
> | **[inferência]** | dedução nossa, que a documentação não afirma |
>
> Termos técnicos ficam em inglês, como no código.

## 0. Resumo

**Recomendação.** A ideia candidata é sólida: os segmentos do WAL passam a ser o lugar
definitivo dos valores grandes, e o inner store guarda só ponteiros. Com os ajustes das §§ 2.6–2.9,
ela é o desenho certo **a longo prazo**. É a única opção avaliada que grava o valor **uma vez**
(PostgreSQL e MongoDB gravam duas). Ela também deixa os checkpoints baratos e, de quebra,
resolve o espaço 2× do redb com valores grandes.

O custo é alto:

- muda o formato físico;
- exige um GC próprio;
- o inner store deixa de ser autossuficiente.

Além disso, o ganho nas fases que hoje perdemos **não está demonstrado**, porque o tempo do caminho
de 1 MiB nunca foi atribuído (§ 1.4).

**Rodada 7: não implementar o vlog inteiro.**

1. Fazer a Etapa 0, que mede onde vai o tempo e quantos bytes o processo grava por byte de
   usuário.
2. Fazer as alternativas baratas C, D e E (§ 5): blocos maiores, remoções sem lookup e preparo dos
   pedidos de ≥ 1 MiB fora da thread de commit. Elas atacam o custo **por chunk**, que é, pelos
   dados da rodada 5, o que domina o pior placar: delete de 1 MiB com 16 clientes, 0,28 do Mongo.
3. A Etapa 1 (WAL segmentado, ainda sem ponteiros) só começa no fim da rodada 7 se o critério de
   decisão da § 7 for atendido. Ponteiros e GC ficam para a rodada 8.

**Achados da leitura do código que mudam o quadro** (detalhes na § 1):

1. **Com 16 clientes e valores de 1 MiB, o WAL não é usado.** O registro de um lote de 4 ou mais
   valores passa de `record_limit()` (4 MiB). O commit vira então um checkpoint, isto é, um commit
   Immediate do fjall. O valor é gravado **2** vezes (journal com `FlushFileBuffers`, depois o blob
   file), e não 3.
2. **Um valor de 1 MiB são 64 objetos de 16 KiB.** Um delete custa cerca de 194 operações de
   índice, das quais 129 são remoções. O vlog não reduz isso; com 16 KiB por bloco, ele até soma um
   lookup por objeto.
3. **O fjall 3.1.10 não permite desligar o journal.** Todo commit de lote chama
   `journal_writer.write_batch`.
4. **`FjallWrite::remove` faz um lookup só para dizer se a chave existia**, e o motor descarta essa
   resposta ao liberar objetos.
5. **Pedidos de ≥ 1 MiB não são preparados na thread de quem chama** (`PREPARE_MAX_BYTES`). A
   codificação deles roda dentro do `apply` do escritor único.

## 1. Situação atual

### 1.1 Escrita de um valor de 1 MiB

Este é o sistema `babel-fjall-wal-tcp`: Adaptive, `WalConfig::default()` com `WriteThrough`, e
`FjallOptions::for_wal()`. O caminho tem três camadas.

**Motor** [código `config.rs`, `engine/ops.rs::store_unit_with`]

- `block_size` e `inline_max` valem 16 KiB. Um valor de 1 MiB vira 64 chunks.
- Cada objeto novo custa 1 leitura (a lista de candidatos de dedupe) e 3 puts: `objects` (16 KiB
  mais 64 B de envelope), `refcounts` e `hash_candidates`.
- Somando o manifesto e `next_object_id`, são cerca de 194 puts e 64 gets por valor.

**WalStore** [código `store/wal.rs::WalWrite::commit`, `log_op`; `config.rs::WalConfig`]

- Cada put também é copiado para o registro de redo em memória.
- Um commit Immediate faz **uma** escrita write-through do registro e depois o commit Deferred do
  inner store.
- O checkpoint (um commit Immediate do inner store) acontece em dois casos:
  - o registro não cabe no resto do arquivo de 16 MiB;
  - o registro passa de `record_limit()` = min(`max_record_bytes` = 4 MiB, capacidade − 4096). Nesse
    caso o registro é descartado e o commit inteiro vira checkpoint (*oversized*).

**fjall** [código `store/fjall.rs`; fontes fjall 3.1.10 `batch/mod.rs`, `journal/writer.rs`]

- O commit do lote escreve cada item no journal através de um `BufWriter` de 8 KiB
  (`JOURNAL_BUFFER_BYTES`), com LZ4 nos valores de ≥ 4 KiB, e depois aplica o lote às memtables.
- Com `DeferredPersist::JournalBuffer` não há chamada de persist.
- No flush da memtable de 64 MiB, os valores de ≥ 1 KiB vão para blob files (com `sync_all`), e as
  tabelas guardam ponteiros.
- O journal é rotacionado num flush quando passa de 64 000 000 bytes (`worker_pool.rs`).

Bytes gravados por valor de 1 MiB:

| caminho | quando ocorre | WAL | journal do fjall | blob file | total |
|---|---|---|---|---|---|
| registrado | lote ≤ ~3 MiB (1 cliente; parte dos lotes com 4 clientes; 64 KiB com 16 clientes) | ~1 MiB, write-through, no commit | ~1 MiB no page cache; `FlushFileBuffers` no checkpoint, a cada ~15 MiB | ~1 MiB no flush (em segundo plano) | **~3 MiB** |
| oversized | lote de 4 ou mais valores de 1 MiB (16 clientes; carga em lote de 16) | 0 | ~1 MiB, com `FlushFileBuffers` **por lote**, no commit | ~1 MiB no flush (em segundo plano) | **~2 MiB** |

Notas sobre a tabela:

- **Quantos lotes são oversized.** O limite de 4 MiB é **[código]**. Que os lotes com 16 clientes
  tenham até ~15 valores (`GroupCommitConfig::max_batch_bytes` = 16 MiB) é **[inferência]**: a
  rodada 5 não registrou `WalStats::unlogged_commits`.
- **Se o journal chega ao disco.** Os bytes do journal chegam ao disco pelo flush do checkpoint ou
  pelo lazy writer do SO antes da rotação. Isso é **[inferência]**, não medido.

### 1.2 Delete e update

**Liberar um objeto** [código `engine/ops.rs::decref`, `remove_object`]:

- 1 get em `refcounts`, depois 2 removes (`objects` e `refcounts`).
- `FjallWrite::remove` faz `contains_key` e só então grava o tombstone [código `store/fjall.rs`].
- O `contains_key` do `BlobTree` não lê o blob [código lsm-tree 3.1.10 `blob_tree/mod.rs`].
- `remove_object` descarta o `bool` que o remove devolve.

**Delete de um valor de 1 MiB:**

- 1 get em `records` e 64 × (1 get + 2 lookups + 2 tombstones), cerca de 194 operações de índice;
- 129 ops no registro do WAL, cerca de 3 KiB, e nenhum byte de valor.

**Update:** um put novo mais o release do valor antigo.

### 1.3 Leitura

Para cada chunk, `fetch_chunks` [código `engine/read.rs`]:

1. consulta o block cache do motor, que guarda blocos já decodificados e verificados;
2. se o bloco não está lá, faz `get(objects)`, que no fjall é um pread do blob, com o block cache do
   próprio fjall no caminho;
3. decodifica o bloco e verifica comprimento e BLAKE3.

### 1.4 O que a rodada 5 mostra

Todas as linhas abaixo são **[medido]**: localhost TCP, durável, ops/s.

| carga e fase | babeldb | PG | Mongo | razão vs. o melhor |
|---|---|---|---|---|
| 1 MiB s5, put ×1 | 77 | 83 | 75 | 0,93 |
| 1 MiB s5, put ×16 | 103 | 106 | 132 | 0,78 |
| 1 MiB s5, update ×4 | 63 | 49 | 102 | 0,62 |
| 1 MiB s5, update ×16 | 76 | 40 | 120 | 0,63 |
| 1 MiB s5, delete ×16 | 488 | 117 | 1 732 | 0,28 |
| 1 MiB s3 (texto), put ×1 | 56 | 33 | 104 | 0,54 |
| 1 MiB s3, put ×16 | 150 | 163 | 194 | 0,77 |
| 1 MiB s3, delete ×16 | 815 | 509 | 1 607 | 0,51 |
| 64 KiB s5, put ×16 | 3 040 | 3 513 | 1 932 | 0,87 |
| 64 KiB s5, update ×4 | 675 | 1 466 | 968 | 0,46 |
| 64 KiB s5, update ×16 | 1 639 | 1 891 | 1 390 | 0,87 |

O que dá para ler desses números:

- **O custo do delete é por chunk, não por byte** **[hipótese forte]**. O delete ×16 de 64 KiB (4
  chunks por valor) mede **13 877/s**; o de 1 MiB (64 chunks por valor) mede 488/s. É cerca de 28×
  menos vazão para cerca de 15× mais operações de índice por delete. A ressalva: a conclusão vem de
  duas cargas diferentes.
- **Update ≈ put + delete** **[hipótese]**. Com 16 clientes, 1/103 s + 1/488 s ≈ 11,8 ms por op,
  isto é, cerca de 85/s. O medido é 76/s.
- **O tempo do put ×16 de 1 MiB não está atribuído.** São cerca de 15 valores por lote, o que dá
  ~146 ms por lote de ~15 MiB. O que se conhece explica menos da metade disso **[hipótese]**:
  - a codificação de dados incompressíveis custa cerca de 1 ms/MiB e é paralelizada em até 8 threads
    [código `planner.rs`: o zstd é pulado para dados uniformes; `engine/write.rs::map_units`];
  - o journal mais `FlushFileBuffers` de 15 MiB fica por volta de 15–30 ms.
- **A anomalia do update de 64 KiB** **[hipótese]**. O update ×4 (675/s) é mais lento que o ×1
  (839/s). A causa não é conhecida. Candidatos: compactações e GC de blobs do fjall disputando o
  disco, ou checkpoints.
- **O put ×1 de 1 MiB de texto** **[hipótese]**. Perde por quase 2×. O candidato é o zstd rodando na
  thread de commit, já que pedidos de 1 MiB não são preparados fora dela. O vlog não mexe nisso.

## 2. Proposta: o WAL segmentado vira o log de valores

### 2.1 Visão geral

```text
commit ─► registro BWR1 (ops pequenas + valores grandes inteiros) ─► segmento N (1 escrita write-through)
             │                                                            ▲
             └─► inner store: ops pequenas + PONTEIROS (commit Deferred)  │ ponteiro = (segmento, offset, len, lsn, 64 B iniciais)
leitura:  inner.get(objects) → ponteiro → pread(segmento, offset, len) → o motor verifica BLAKE3
GC:       segmento com muito lixo → re-put dos valores vivos (registro novo) → checkpoint → segmento livre ou reciclado
```

Princípios:

- **Tudo acontece no `WalStore`.** O motor não muda. O conteúdo lógico das 9 tabelas, lido através
  do `WalStore`, é idêntico ao de hoje (invariante I5).
- **Só a tabela `objects` usa ponteiros.** Os valores dela são envelopes imutáveis, e os ids nunca
  são reutilizados.
- **O registro do WAL é a casa definitiva do valor.** Por isso o valor é gravado uma vez.

É a ideia do WiscKey (Lu et al., FAST 2016), em que o vLog também serve de log de recuperação
**[literatura]**. A variante do WiscKey com um vLog separado e o ponteiro no WAL foi descartada: ela
exige duas escritas duráveis por commit, ou escritas paralelas com checksum por quadro.

### 2.2 Segmentos

**Diretório.** `<arquivo do banco>.vlog/`; com `WalConfig::dir`, `<nome>.<16 hex>.vlog/`, como hoje.

- Um arquivo `LOCK` fica aberto de forma exclusiva, como o `.wal` hoje.
- Cada segmento é `<número, 16 hex>.seg`, com tamanho fixo `segment_bytes`. Proposta: 64 MiB, o
  tamanho dos blob files do fjall.

**Bloco 0 (cabeçalho, 4 KiB):**

| campo | conteúdo |
|---|---|
| magic | `BABELVLG` |
| versão | 1 |
| `data_start` | 4096 |
| capacidade | tamanho do arquivo |
| salt | 32 B, **o mesmo em todos os segmentos do banco** (o id e a chave de checksum derivam dele, como na § 19 de `format.md`) |
| número do segmento | u64, crescente, nunca reutilizado |
| `first_lsn` | u64 |
| checksum | 16 B |

**Registros.** A partir de 4096, no mesmo formato `BWR1` da § 19.2, sem mudança de bytes.

- Um registro nunca atravessa segmentos.
- A cadeia continua no segmento N+1 se, e somente se, o `first_lsn` do cabeçalho dele for o último
  LSN + 1.

**Alocação de um segmento novo.**

1. Primeiro reutiliza um segmento do pool de livres: o arquivo é renomeado e o cabeçalho é regravado
   com uma escrita write-through.
2. Sem livres, cria um arquivo novo. O custo de pré-alocar é uma questão em aberto (§ 8):
   - zerar o arquivo, como `create_wal_file` faz hoje, dobra os bytes enquanto o conjunto vivo
     cresce;
   - `set_len` sem zeros paga uma atualização da valid data length do NTFS a cada escrita
     write-through.

**Após cada abertura,** o escritor começa num segmento novo. Ele nunca escreve depois de uma cadeia
cortada.

### 2.3 Registros: a op 03 (put por referência)

`03, índice da tabela, klen u32 LE, vlen u32 LE, chave, valor`. São os mesmos bytes do put (`01`).
A diferença está no replay: ele grava no inner store um **ponteiro** para os bytes do valor dentro
do registro, e não o valor.

Um put vira op 03 só quando:

- a tabela é `objects`; e
- vale uma destas condições:
  - `vlen ≥ vlog_min_bytes` (proposta: 4 KiB, a medir), para que os últimos chunks pequenos
    continuem inline;
  - o valor começa com o magic `BVP1`. Assim um valor bruto nunca é confundido com um ponteiro, sem
    recusar nada.

### 2.4 Ponteiro: valor de `objects[id]` no inner store, 96 bytes

| offset | bytes | campo |
|---|---|---|
| 0 | 4 | magic `BVP1` (um envelope sempre começa com `BBO1`) |
| 4 | 4 | número do segmento, u32 LE |
| 8 | 8 | offset do primeiro byte do valor no arquivo, u64 LE |
| 16 | 4 | `len` do valor, u32 LE |
| 20 | 4 | reservado, 0 |
| 24 | 8 | LSN do registro, u64 LE (diagnóstico e verificação cruzada no GC) |
| 32 | 64 | os 64 primeiros bytes do valor (num envelope: o cabeçalho inteiro, com `raw_len`, codec e **digest BLAKE3**) |

Para que servem os 64 bytes copiados:

- a leitura detecta, sem hashing, um pread no lugar errado;
- `stats` e `inspect` conseguem ler codec e tamanhos sem ler o valor.

### 2.5 Metadados ocultos no inner store

Hoje o `WalStore` já esconde de quem usa o store as entradas `wal_lsn`, `wal_id` e `wal_clean`
[código `store/wal.rs::is_reserved`]. A proposta acrescenta quatro:

| entrada | conteúdo | registrada no WAL? |
|---|---|---|
| `wal_layout` | u8; 2 = segmentado; ausente = 1 (o arquivo único de hoje) | — |
| `wal_replay` | segmento u64 + offset u64: início da cadeia depois do último checkpoint | não |
| `vlog_seg.<16 hex>` | `live_count` u64 LE, `live_bytes` u64 LE; ausente = 0 | **sim**, como puts comuns com valor absoluto; por isso o replay as reproduz |
| `format_version` **físico** = 2 | o `WalStore` apresenta 1 ao motor (§ 2.11) | — |

Consequência: a regra de reserva deixa de ser por nome exato e passa a incluir o prefixo
`vlog_seg.`.

### 2.6 Caminho de escrita e durabilidade: a ordem exata

**Dentro da transação:**

- **`put(objects, k, v)`, quando elegível:**
  1. `inner.get(objects, k)`. Se o valor antigo for um ponteiro, desconta 1 valor e `len` bytes do
     segmento dele. As contas ficam em memória, na transação.
  2. Acrescenta a op 03 ao registro em `out` e anota (k, posição do valor no registro).
  3. Grava `inner.put(objects, k, ponteiro provisório)`. O provisório usa um segmento sentinela e
     um offset relativo ao registro. Um `get` dentro da mesma transação o resolve a partir de `out`.
     Isso cobre, por exemplo, a comparação de dedupe com um bloco gravado na mesma transação.
- **`remove(objects, k)`:** lê o ponteiro antigo para descontar, remove e registra a op 02, como
  hoje.

**No commit** (Immediate, ou Deferred com pelo menos uma op 03):

1. **Posiciona o registro.** Se `tail + out.len()` couber no segmento atual, fica nele. Senão:
   - grava os registros pendentes no segmento atual;
   - sela esse segmento;
   - abre o seguinte (com o cabeçalho durável antes do primeiro registro);
   - posiciona o registro em `data_start`.
2. **Finaliza os ponteiros.** Para cada op 03 cuja chave ainda aponta para o provisório (a última op
   da chave na transação), grava `inner.put(objects, k, ponteiro final)`. Esse put vai só ao inner
   store: o replay deriva o ponteiro da op 03.
3. **Contadores.** Grava `vlog_seg.<n>` com o valor absoluto de cada segmento tocado, no inner store
   **e** no registro.
4. **Sela e escreve.** Sela o registro (LSN e checksum) e faz **uma** escrita write-through de `out`
   (pendentes + este registro). É o ponto de durabilidade: o valor e tudo de que o replay precisa
   para refazer o ponteiro estão no mesmo registro, e o checksum decide tudo ou nada.
5. **Publica.** Só então faz `inner.commit(Deferred)`.

**Casos especiais:**

- **Deferred sem op 03:** igual a hoje; o registro fica em memória.
- **Deferred com op 03** (importações, `WriteDurability::Buffered`): segue os passos acima, ou seja,
  é tratado como Immediate **no nível do WAL** (regra I2). Sem isso, o fjall poderia recuperar do
  journal um commit Deferred com ponteiro cujo registro ainda estava em memória: o modo
  `JournalBuffer` manda o excedente de 8 KiB ao SO. Nesse caso sobraria um ponteiro para bytes que
  nunca foram gravados. Custo: uma escrita write-through por commit desse tipo.
- **Registro maior que um segmento** (substitui o oversized de hoje):
  - os valores das ops 03 da transação voltam a ir inline, `inner.put(objects, k, v)` com os bytes
    ainda em `out`;
  - nada é registrado;
  - o commit é um checkpoint.

  A leitura distingue valor inline de ponteiro pelo magic. `max_record_bytes` passa a limitar só os
  bytes que não são vlog. Com o vlog ligado, as importações devem ter lotes de no máximo
  `segment_bytes / 2`.
- **Checkpoint.**
  - *Gatilho:* uma destas condições:
    - payload fora do vlog desde o último checkpoint ≥ `checkpoint_bytes` (16 MiB, o comportamento
      de hoje para valores pequenos);
    - todos os bytes de registro desde o último checkpoint ≥ `replay_max_bytes` (proposta: 256 MiB,
      para limitar o replay);
    - pedido explícito, `compact` ou fechamento.
  - *Procedimento:*
    1. `sync_data` dos segmentos escritos desde o último checkpoint. Com `WriteThrough` no Windows
       isso garante que a cache do disco foi esvaziada; é barato, porque os dados já foram
       escritos.
    2. Commit Immediate do inner store com `wal_lsn` e `wal_replay` = (segmento atual, `tail`).
  - *O log não recomeça.* Os segmentos selados sem valores vivos viram livres (§ 2.9).

**Invariantes:**

| id | nome | enunciado |
|---|---|---|
| I1 | WAL primeiro | como hoje: nada fica visível antes de o registro ser durável (Immediate) ou estar na fila (Deferred) |
| I2 | valor antes do ponteiro | um ponteiro só entra num commit do inner store, mesmo Deferred, depois que o registro com o seu valor é durável. Logo, nenhum estado recuperado do inner store contém ponteiro para bytes que possam não existir |
| I3 | nenhum ponteiro durável pendurado | um segmento só é liberado se: (a) todos os seus registros têm LSN ≤ `wal_lsn` durável; (b) o seu contador no estado durável é 0; (c) nenhuma transação de leitura iniciada antes do commit que zerou o contador ainda está viva |
| I4 | determinismo | ponteiro = f(posição do registro, posição da op). O replay reproduz exatamente os ponteiros do commit original e continua idempotente (puts e removes cegos) |
| I5 | transparência lógica | através do `WalStore`, as 9 tabelas se leem como sem o vlog |

### 2.7 Caminho de leitura

**`WalRead::get(objects, k)`:**

1. Lê o valor no inner store.
2. Se não for ponteiro (96 B com `BVP1`), devolve o valor como está.
3. Se for, pega o segmento no `Arc<SegmentSet>`. Esse `Arc` é capturado em `begin_read`, **antes**
   do snapshot do inner store (a razão está na § 2.9).
4. Faz o pread de `len` bytes no `offset` e confere o comprimento e os 64 primeiros bytes.

**Verificação.** O motor já verifica comprimento e BLAKE3 de cada unidade decodificada
(`verify_on_read`, ligado por padrão) [código `engine/read.rs`, `engine/ops.rs`]. Isso é a
integridade fim a fim. Os 64 bytes pegam leituras no lugar errado (zeros, segmento reciclado, offset
errado) sem hashing.

Com `verify_on_read = false`, o corpo não é conferido na leitura. É uma perda pequena em relação ao
fjall, cujo blob traz um checksum xxh3-128 no cabeçalho **[código `store/fjall.rs`, doc do módulo]**. Mesmo assim, o GC
e o `verify --deep` conferem o checksum dos registros.

**`get_many`.** Ordena por (segmento, offset) e junta vizinhos num só pread. Os 64 chunks de um valor
de 1 MiB estão quase contíguos no mesmo registro, separados por 50–100 B de ops pequenas. Para o
motor se beneficiar, `fetch_chunks` precisa usar `get_many`, uma mudança pequena.

**`scan(objects)`.** Resolve cada valor. Ponto de atenção: `stats` varre hoje os valores de
`objects` só para somar tamanhos e codecs [código `engine/inspect.rs`]. Com o vlog, isso leria todos
os valores, ~2 GiB na carga de 1 MiB. É preciso um caminho que leia só o cabeçalho, o que é possível
porque o ponteiro carrega os 64 bytes iniciais.

**Caches.**

- O block cache do motor não muda: os ids são imutáveis, e a realocação não muda bytes.
- O block cache do fjall deixa de guardar blocos de valor; guarda só ponteiros.
- Os segmentos são lidos pelo page cache do SO. Um valor recém-escrito já está nele, porque a
  escrita write-through passa por um handle com cache **[inferência sobre o Windows]**.
- Com `WriteThroughUnbuffered`, a coerência entre o handle de escrita sem cache e os leitores com
  cache precisa de teste.

### 2.8 Recuperação após queda

**Abertura, passo a passo:**

1. Lê do inner store `wal_lsn` (L), `wal_id`, `wal_layout`, `wal_replay` (S, O) e os contadores.
2. Trava o diretório, lista os segmentos, valida os cabeçalhos (id do banco) e monta um mapa
   número → arquivo.
3. Lê a cadeia a partir de (S, O), atravessando segmentos (§ 2.2), até o primeiro registro rasgado
   ou corrompido, ou até uma lacuna de LSN.
4. Aplica os registros com LSN > L:
   - ops 01 e 02, como hoje;
   - op 03, gravando o ponteiro derivado da posição;
   - os contadores voltam como puts comuns.
5. Calcula o salto de LSN, como hoje (§ 19.3, ponto 6), usando a soma das capacidades dos segmentos.
6. Faz um checkpoint Immediate com `wal_lsn` = salto e `wal_replay` = início de um segmento **novo**.
7. Libera ou recicla os segmentos que não têm contador vivo e estão inteiros antes do novo
   `wal_replay`.
8. Se um segmento com contador > 0 estiver ausente ou tiver cabeçalho inválido, o resultado é um
   erro de integridade, como o WAL ausente hoje. Nada se perde em silêncio.

**Casos:**

- **Escrita rasgada no fim.** A cadeia termina ali. O commit não foi confirmado e, por I2, nenhum
  estado do inner store tem ponteiro para ele.
- **Registros íntegros depois do rasgado, no mesmo segmento.** Nunca foram confirmados e o segmento
  não recebe mais escritas. Nenhum ponteiro os referencia.
- **Ponteiro para dado ainda não durável.** Impossível por I2, inclusive quando o fjall recupera do
  journal commits Deferred posteriores ao checkpoint. Isso pode acontecer (`store/fjall.rs`,
  "Durability") e é exatamente o que torna necessária a regra dos Deferred com op 03.
- **Segmento reciclado com registros velhos.** O cabeçalho novo e a regra de LSN consecutivo
  impedem que eles continuem uma cadeia, porque os LSNs antigos são menores.
- **Queda de energia com `WriteThrough` no Windows.**
  - Para os commits recentes, a garantia é a de hoje, a do `open_datasync` do PostgreSQL.
  - Para os valores alcançáveis do estado durável do inner store, vale o `sync_data` explícito de
    cada checkpoint. O que `FlushFileBuffers` garante no disco não foi verificado, como já diz
    `wal.rs`.

### 2.9 GC e recuperação de espaço

**Contabilidade.** É exata e transacional: os contadores `vlog_seg.*` mudam no mesmo commit que
cria ou remove o ponteiro. Um valor morre no remove do motor (refcount 0), não quando uma
compactação o encontra.

**Segmento elegível.** Está selado e todos os seus LSNs são ≤ `wal_lsn` durável.

**Política proposta** (limiares a medir):

- Segmento com 0 vivos: liberado no checkpoint seguinte, **sem cópia**. Com carga de valores
  pequenos, o vlog se comporta como o arquivo circular de hoje.
- Realocação: roda quando os bytes mortos passam de max(`gc_min_bytes` = 256 MiB,
  `gc_space_ratio` = 0,5 × vivos). Escolhe os segmentos com menor fração viva (guloso) e para abaixo
  do limite. O custo-benefício do LFS (Rosenblum & Ousterhout, 1992) fica para depois.
- `compact`: realoca todo segmento com ≥ 10% de lixo e devolve ao sistema de arquivos o pool de
  livres acima de 2 segmentos.

**Realocação.** Roda como uma transação de escrita comum, em pedaços de até 16 MiB:

1. Lê o segmento vítima em sequência, **conferindo os checksums dos registros**.
2. Para cada op 03, se `inner.get(objects, k)` ainda aponta para (vítima, offset), faz o re-put do
   valor, que vira uma op 03 no segmento atual.
3. O motor não vê nada: bytes e ids são os mesmos.
4. Depois vem um checkpoint, e o arquivo é liberado quando I3 vale.

**Leitores.** Cada `WalRead` segura o `Arc<SegmentSet>` capturado antes do snapshot. O GC só publica
um conjunto sem a vítima depois do commit da realocação, e o arquivo só é apagado ou reciclado no
`Drop` do último `Arc`. Assim um snapshot antigo sempre encontra o segmento para os ponteiros que
ele vê. No Windows, os handles de leitura precisam de `FILE_SHARE_DELETE`, o padrão do Rust.

**Queda no meio do GC:**

| momento da queda | estado após reabrir |
|---|---|
| antes da escrita da realocação | nada mudou |
| depois do commit, antes do checkpoint | o replay reaponta; a vítima ainda existe |
| depois do checkpoint, antes de apagar | a vítima tem 0 vivos e é liberada na abertura (§ 2.8, passo 7) |

**Quem roda o GC.**

- Etapa 2: offline, dentro de `compact`.
- Etapa 3: uma thread em segundo plano que disputa o lock de escrita entre os lotes do primeiro
  plano.

**Amplificação: modelo.** Realocar um segmento com fração viva *u* copia *u* para liberar 1 − *u*,
isto é, *u*/(1 − *u*) byte copiado por byte recuperado: 0,33 com *u* = 0,25 e 1,0 com *u* = 0,5.

| | bytes do valor gravados | quando o lixo é descoberto | espaço até recuperar |
|---|---|---|---|
| hoje, fjall + WAL | 3 (ou 2 oversized), mais as realocações de blob | quando uma compactação junta o tombstone com o ponteiro (no leveled, quando chegam ao mesmo nível) [código, doc de `store/fjall.rs`] | lixo até a compactação chegar lá; blob file copiado com ≥ 25% de lixo e só se nenhuma tabela fora da entrada aponta para ele; no máximo os 25% mais antigos por compactação; `compact` limpa tudo |
| vlog | 1, mais *u*/(1 − *u*) por byte recuperado | no remove (exato) | ≤ vivos × (1 + `gc_space_ratio`), mais o pool, mais as ops pequenas mortas dos segmentos retidos |
| PostgreSQL | 2 (WAL + heap/TOAST), mais FPW [§ 3] | VACUUM | tuplas mortas até o VACUUM; o espaço é reutilizado dentro do arquivo |
| MongoDB | 2 (journal + checkpoint) [§ 3] | no commit (em memória) | blocos livres reutilizados dentro do arquivo [inferência] |

### 2.10 Interação com o motor

- **Objetos imutáveis e ids nunca reutilizados.** Por isso a realocação é invisível ao motor e ao
  block cache. `refcounts`, `hash_candidates`, `pending_imports` e `history` não mudam: o `WalStore`
  só vê puts e removes em `objects`.
- **Dedupe.** Um candidato é lido por pread, como hoje do blob do fjall. Um candidato gravado na
  mesma transação é lido de `out`.
- **Release: um lookup a mais por objeto removido**, para achar o segmento a descontar. São +64
  lookups por delete de 1 MiB com blocos de 16 KiB e +1 com blocos de 1 MiB. **Sem a alternativa C,
  o vlog é neutro a negativo para deletes.**
- **Put: um lookup a mais por objeto** (o `get` do valor antigo). Pode ser evitado com uma dica
  `put_new` do motor, que sabe quando o id é novo.
- **Importações.** Seguem a regra dos Deferred com op 03 e têm lotes limitados a `segment_bytes / 2`.
- **Blocos maiores (C) potencializam tudo.** Com blocos de 1 MiB há 1 ponteiro, 1 contador e 1
  lookup por valor, e a leitura é um pread só.

### 2.11 Formato, versionamento e compatibilidade

**Texto de `docs/format.md`:**

- a § 19 ganha o layout 2 (segmentos, cadeia entre segmentos, `wal_layout`, `wal_replay`,
  `vlog_seg.*`) e a op 03;
- uma § 20 nova descreve o ponteiro.

**Formato lógico: não muda.** `FORMAT_VERSION` lógico continua 1, e as 9 tabelas lidas pelo
`WalStore` são as mesmas.

**Marca física.** O inner store com layout 2 grava `format_version` = 2, e o `WalStore` traduz de
volta para 1 ao falar com o motor. O motivo:

- um binário antigo, ou `Db::open` sem o `WalStore`, vê 2 e recusa com `Unsupported`, em vez de ler
  ponteiros como envelopes quebrados;
- copiar tabelas entre backends byte a byte (§ 2 de `format.md`) copiaria ponteiros em silêncio.

A alternativa, uma entrada nova em `meta`, protege só os binários novos. A § 16 de `format.md` pede
versão nova quando um leitor antigo pode ler os bytes novos de outro jeito.

**Mudança de garantia.** Hoje, "depois de um fechamento limpo, o arquivo do backend sozinho é
completo" (§ 19.3). Com o vlog, isso deixa de valer: o backup é o inner store **mais** o diretório
`.vlog`.

**Bancos existentes.**

- O padrão não muda: layout 1.
- Ligar o vlog na abertura:
  1. recupera o WAL de layout 1 e faz o checkpoint;
  2. cria o diretório;
  3. grava as marcas num commit Immediate;
  4. apaga o `.wal` antigo.
- Os valores que já estão inline continuam inline; a leitura distingue pelo magic.
- É uma mão só. Uma ferramenta que regrave os ponteiros inline (uma "desvlogação") seria opcional.

**`tests/format_compat.rs`:**

- `open_v1_fixtures` fica intacto e tem de passar;
- vetores golden de ponteiro, op 03 e cabeçalho de segmento;
- uma fixture congelada nova, `v1_vlog.redb` + `.vlog/`, gerada uma vez;
- um teste de que `Db::open` num banco com vlog dá `Unsupported`;
- `validate_tables` através do `WalStore` num banco com vlog (I5).

### 2.12 Backends

| backend | aplica? | por quê |
|---|---|---|
| redb (padrão) | **sim, com ganho grande [hipótese]** | o commit Deferred é `Durability::None`, que escreve páginas sem fsync [código `store/redb.rs`]. Com só ponteiros nas páginas, o checkpoint fica barato, e o espaço 2× do redb com blocos de 16 KiB (4 312 MB vs. 2 112 MB do Mongo) [medido] deve sumir |
| fjall | **sim** | a separação chave-valor continua para os valores inline de `records` de ≥ 1 KiB; os ponteiros de 96 B ficam nas tabelas |
| LMDB (heed) | não | Deferred é Immediate [código `store/heed.rs`], então o próprio WAL não serve de nada nele |
| fjall `KeyspacePerTable` | não | também trata Deferred como Immediate |
| mem | só testes | — |

## 3. Como PostgreSQL e MongoDB gravam valores grandes

### PostgreSQL 17

**[doc PG]**

- **TOAST**
  - A página tem tamanho fixo (em geral 8 kB), e uma tupla não atravessa páginas.
  - Acima de cerca de 2 kB (`TOAST_TUPLE_THRESHOLD`), o valor é comprimido e/ou movido para fora da
    linha.
  - Fora da linha, o valor é cortado em chunks de cerca de 2000 bytes (`TOAST_MAX_CHUNK_SIZE`, para
    caber 4 por página), cada um uma linha da tabela TOAST.
  - `EXTENDED` (compressão e depois fora da linha) é o padrão da maioria dos tipos.
  - Um UPDATE que não muda o valor fora da linha não tem custo de TOAST.
- **WAL**
  - As mudanças nos arquivos de dados só são escritas depois que o WAL que as descreve foi para o
    disco; as páginas de dados não precisam ir ao disco a cada commit.
  - `full_page_writes` (ligado por padrão) grava a página inteira no WAL na primeira modificação
    depois de cada checkpoint.
- **Checkpoints**
  - `checkpoint_timeout` = 5 min, `max_wal_size` = 1 GB, `checkpoint_completion_target` = 0,9: as
    páginas são escritas em segundo plano, espalhadas no tempo.
  - `wal_recycle` e `wal_init_zero` reciclam e zeram segmentos de WAL. É o mesmo problema de
    pré-alocação da § 2.2.

**[inferência]**

- **Insert.** O registro de WAL do insert de cada chunk leva os dados do chunk. Um valor de 1 MiB
  vai ao disco duas vezes: WAL no commit, e heap/TOAST no checkpoint ou pelo background writer.
- **DELETE.** Marca cada tupla-chunk (cerca de 500 por MiB), com um registro de WAL por tupla, e
  pode gravar a imagem inteira de cada página tocada pela primeira vez desde o checkpoint. Isso é
  coerente com o delete ×16 do PG cair de 3 340/s (64 KiB) para 117/s (1 MiB) [medido].
- **Dados incompressíveis.** O pglz desiste de comprimi-los.
- **Espaço.** O VACUUM recupera o espaço depois, reescrevendo páginas.

### MongoDB 8.3 (WiredTiger)

**[doc Mongo]**

- **Durabilidade.**
  - O WiredTiger usa um write-ahead log (journal) junto com checkpoints.
  - O MongoDB cria um checkpoint a cada 60 s.
  - Com `j: true`, a escrita espera o sync do journal; sem isso, o sync ocorre a cada 100 ms
    (`commitIntervalMs`).
  - Cada escrita de cliente vira **um** registro de journal, que inclui as modificações internas
    (índices).
- **Compressão.**
  - O journal é comprimido com snappy (registros de ≤ 128 B não são comprimidos).
  - As coleções usam compressão de bloco snappy.
- **Tamanho.** Um documento tem no máximo 16 MiB.

**[inferência]**

- **O que vai no journal.** O registro de um insert ou replace leva o documento inteiro, porque é
  redo.
- **Arquivo de dados.** Só é escrito no checkpoint ou na evicção, fora do caminho do commit.
- **Delete.** No commit, o delete é só um registro pequeno de journal mais uma mudança em memória.
  Isso é coerente com 1 732 deletes/s de 1 MiB com 16 clientes [medido].
- **Espaço.** O espaço liberado é reutilizado dentro do arquivo pelo block manager, sem GC que copie
  valores.
- **Valores grandes.** Viram overflow items (`leaf_value_max` na documentação do WiredTiger). Não
  verificamos como o MongoDB configura isso.

### Comparação: valor de 1 MiB

| | no caminho do commit | depois, em segundo plano | total de bytes do valor | delete no commit |
|---|---|---|---|---|
| PostgreSQL | WAL (fsync por `open_datasync`) | heap/TOAST no checkpoint | 2 (+ FPW) | ~500 tuplas marcadas + FPW [inferência] |
| MongoDB | journal (sync com `j:true`) | arquivo de dados no checkpoint de 60 s | 2 | 1 registro pequeno [inferência] |
| babeldb hoje | WAL write-through + journal no page cache + checkpoint síncrono a cada ~15 MiB sob o lock de escrita | blob file no flush da memtable | 3 (2 oversized) | ~194 operações de índice, 129 tombstones |
| babeldb com vlog | 1 escrita write-through + commit pequeno do inner store | GC só onde houver lixo | 1 (+ GC) | igual a hoje, +64 lookups (C reduz a ~5 ops) |

## 4. Ganhos esperados — **todos [hipótese]**

As faixas vêm do mecanismo e das contas da § 1.4, não de medições. Elas assumem que a parte do
tempo que o vlog remove existe, e a Etapa 0 precisa confirmar isso.

| fase (TCP, rodada 5) | hoje | melhor rival | C + D + E (rodada 7) | + vlog | mecanismo |
|---|---|---|---|---|---|
| 1 MiB s5, delete ×16 | 488 | 1 732 | 3 000–10 000 | ≈ C | ~194 → ~5 operações por delete; 64 KiB (4 chunks) já faz 13,9 k/s |
| 1 MiB s5, update ×16 | 76 | 120 | 95–115 | 115–150 | update ≈ put + delete; C tira quase todo o delete; o vlog tira journal, blob e checkpoint do put |
| 1 MiB s5, update ×4 | 63 | 102 | 75–95 | 95–120 | idem |
| 1 MiB s5, put ×16 | 103 | 132 | 105–120 | 110–140 | lote oversized: o vlog troca journal + `FlushFileBuffers` + blob por 1 escrita. Mais da metade dos ~146 ms por lote não está atribuída: é a maior incerteza |
| 1 MiB s5, put ×1 | 77 | 83 (PG) | 78–85 | 85–100 | some o checkpoint a cada ~15 puts (p95 16 ms / p99 21 ms no embarcado, contra p50 6,6 ms) e 1 MiB de journal por put |
| 1 MiB s3, put ×1 | 56 | 104 | 60–90 (E) | ≈ C | o candidato é CPU de zstd na thread de commit; o vlog não ajuda |
| 64 KiB s5, put ×16 | 3 040 | 3 513 | 3 000–3 300 | 3 500–4 500 | 3 gravações → 1; checkpoint (flush de ~16 MiB de journal) a cada ~16 lotes de 1 MiB |
| 64 KiB s5, update ×4 / ×16 | 675 / 1 639 | 1 466 / 1 891 | ? | ? | a anomalia do ×4 não está explicada. Se for GC de blobs do fjall, o GC preguiçoso do vlog ajuda. Medir antes |
| 1 MiB, get ×1 | 184 | 165 | ≈ | ≈ ou melhor | o pread existe nos dois; `get_many` junta os preads. Risco de piora se o page cache for pior que o block cache do fjall |
| espaço 1 MiB s5, após a carga (MB) | 2 121,7 | 2 111,8 | ~2 113 | ~2 113–2 125 | C tira os 0,4% de envelope por bloco de 16 KiB; o vlog soma cabeçalhos de registro e a cauda do segmento |
| espaço redb 1 MiB (MB) | 4 311,8 | — | ? | ~2 120 | as páginas do redb passam a ter só ponteiros |

Nenhuma linha acima conta o GC. Nas fases do benchmark, cerca de 700 updates espalhados por cerca
de 2 700 MiB de log deixam cada segmento com cerca de 34% de lixo. Isso fica abaixo do limite de
realocação proposto, então o GC não rodaria durante a medição. Mas o espaço ficaria em torno de
1,34× os vivos até o próximo `compact` **[hipótese]**.

## 5. Alternativas

**A. Flush das memtables do fjall no checkpoint, em vez de fsync do journal.**

- O journal continua sendo gravado em todo commit (ver B), então **nenhum byte é poupado**.
- O checkpoint passa a gravar os blobs e as tabelas de forma síncrona, sob o lock de escrita.
- As memtables efetivas passam de 64 MiB para 16 MiB: mais runs no nível 0, mais compactação.
- Para o flush ser assíncrono, o WAL precisa reter segmentos até o flush acabar, ou seja, a Etapa 1.
- **Sozinha, não.**

**B. Desligar o journal do fjall.**

- **Impossível por configuração no 3.1.10** [código fjall `batch/mod.rs`: `commit` sempre chama
  `journal_writer.write_batch`]:
  - `manual_journal_persist` (`builder.rs`) só desliga o `persist(Buffer)` automático;
  - a `Ingestion` (`ingestion.rs`) grava tabelas direto, mas exige chaves em ordem crescente
    (lsm-tree `tree/ingest.rs`, `assert!`), então não serve para transações.
- **Caminho possível:** um fork vendorizado do fjall com uma flag que pula o `write_batch`, mais o
  checkpoint feito por flush de memtable, assíncrono sobre o WAL segmentado (Etapa 1).
  - Dá **2 gravações**, paridade com PG e Mongo.
  - Não precisa de GC próprio: o GC de blobs do fjall continua.
- **Riscos:**
  - manter o fork;
  - premissas internas do fjall sobre journals (o `journal_manager` usa os journals para saber o que
    já foi descarregado);
  - o `persist` do fjall segura o mutex do journal durante o sync (`journal/mod.rs`);
  - não ajuda o redb, que já grava 2×.
- **Plano B se o vlog for julgado arriscado demais.** A Etapa 1 serve às duas opções.

**C. Blocos maiores.**

- `block_size` já aceita até 1 MiB (`MAX_BLOCK_SIZE`) [código `config.rs`]. É um parâmetro de
  criação, então os bancos existentes mantêm o seu e **não há mudança de formato**.
- Com 1 MiB, um valor de 1 MiB vira 1 objeto:
  - o delete cai de ~194 para ~5 operações;
  - o put cai de ~256 para ~6 operações de índice;
  - somem os 0,4% de envelopes.
- Custos:
  - leitura parcial decodifica e verifica o bloco inteiro;
  - dedupe e block cache ficam mais grossos (o cache de 64 MiB tem 16 shards de 4 MiB e aceita
    blocos de 1 MiB, [código `cache.rs`]);
  - não reduz os bytes gravados.
- Os leitores só usam `logical_end` (`format.md` § 6), então uma política de chunk variável por
  tamanho de valor também seria compatível, com uma mudança só de texto na § 6.
- **Recomendada primeiro:** medir 256 KiB e 1 MiB.

**D. Remoções sem lookup e leituras em lote no release.**

- Um `WriteTxn::remove_unchecked` teria como padrão o `remove`. No fjall, gravaria o tombstone sem
  `contains_key`. No `WalStore`, sempre registraria a op 02: o replay de um remove de chave ausente
  não muda nada.
- `remove_object` passaria a usá-lo, e `release_manifest` passaria a ler os refcounts com
  `get_many`.
- Economiza 128 lookups por delete de 1 MiB com blocos de 16 KiB. **Barata.**

**E. Preparar pedidos de ≥ 1 MiB fora da thread de commit.**

- Hoje `prepare` devolve `Owned` quando `bytes ≥ PREPARE_MAX_BYTES` (1 MiB) [código
  `scale/mod.rs`], então todo put de 1 MiB é codificado dentro do `apply`.
- Proposta: preparar quando houver menos de 64 ops e até, por exemplo, 16 MiB.
- Deve pesar mais no texto (zstd) do que no incompressível. **Barata; medir.**

**F. Só ajustar parâmetros.**

- Um `segment_bytes` maior só espaça os checkpoints: o total de bytes do journal a esvaziar é o
  mesmo.
- Um `max_record_bytes` maior levaria os lotes grandes do caminho oversized (2 gravações) para o
  registrado (3). **Não.**

**Resumo das opções:**

| opção | bytes do valor | formato | complexidade | ataca |
|---|---|---|---|---|
| hoje | 3 (2 oversized) | — | — | — |
| A | 3 | não | baixa | nada |
| B (fork) | 2 | não para os dados; WAL segmentado | média-alta | bytes do journal |
| C | 3 | não | baixa | operações por valor (delete e update) |
| D, E | 3 | não | baixa | lookups; CPU na thread de commit |
| vlog | 1 (+ GC) | sim, físico 2 | alta | bytes e checkpoints (com C: tudo) |

## 6. Riscos, do mais grave ao menos grave

1. **O ganho não está demonstrado.** As piores fases (delete e update ×16 de 1 MiB) são dominadas
   por custo por chunk, que o vlog não toca, e mais da metade do tempo do put ×16 não está
   atribuída. *Mitigação:* Etapa 0 e o critério de decisão da § 7.
2. **Perda de dados por bug.** As fontes possíveis:
   - ponteiro antes do valor (Deferred, fallback de registro grande, ponteiros provisórios, put
     seguido de remove na mesma transação ressuscitando o provisório);
   - GC liberando um segmento ainda referenciado por um snapshot ou pelo estado durável;
   - cadeia entre segmentos e arquivos reciclados.

   *Mitigação:* invariantes I1–I5 com `debug_assert`, testes de queda com processo filho, teste de
   modelo e failpoints no GC.
3. **Mudança de formato físico.**
   - O inner store deixa de ser completo sozinho; o backup inclui o `.vlog`.
   - A migração é de mão única.
   - `stats`, `inspect` e `verify` passam a ler ponteiros.
4. **Espaço.**
   - Até (1 + `gc_space_ratio`) × os vivos antes do GC;
   - ops pequenas mortas presas em segmentos retidos;
   - o pool de livres.
5. **O GC disputa o lock de escrita.** Isso causa picos de latência e amplificação de até
   1 + *u*/(1 − *u*).
6. **Windows.**
   - Pré-alocação de segmentos novos: zerar dobra os bytes durante o crescimento (na carga, o vlog
     empataria com hoje); `set_len` com write-through paga a atualização da valid data length.
   - `FILE_SHARE_DELETE` nos handles de leitura.
   - Coerência de cache com `WriteThroughUnbuffered`.
7. **Modo relaxado (`Buffered`) com valores grandes.** Passa a ter uma escrita durável por lote
   (regra I2).
8. **Leituras.** Um pread por bloco em vez de um acerto no block cache do fjall, e o page cache do
   SO fica fora do orçamento de RAM.
9. **Complexidade.** `wal.rs` cerca de dobra, e passa a haver dois layouts a manter.

## 7. Plano em etapas

**Etapa 0 — rodada 7: medir, sem mudar formato.**

Instrumentar `benches/compare.rs` para imprimir, por fase:

- o delta de `WalStats`: `wal_bytes`, `logged_commits`, `unlogged_commits`, `checkpoints`,
  `write_nanos`, `inner_commit_nanos`, `checkpoint_nanos`;
- os bytes escritos pelo processo divididos pelos bytes de usuário (Windows
  `GetProcessIoCounters().WriteTransferCount`; Linux `/proc/self/io` `wchar`), que é a amplificação
  lógica, com as threads do fjall incluídas;
- `write_pressure()`, `blob_file_count()` e `stale_blob_bytes()` do fjall, antes e depois;
- o tempo por lote em `prepare`, `apply` do motor e commit do store.

Depois, rodar só os alvos babeldb em 1m-s5, 1m-s3 e 64k-s5, com o C, o D e o E ligados um por vez:
`block_size` por variável de ambiente; D e E atrás de flags.

Testes:

- unitários do D, com o conformance do store e do WAL: remove de chave ausente e replay;
- `tests/format_compat.rs` sem mudança;
- com C, `check_fixture` continua passando (as fixtures mantêm 16 KiB).

**Critério de decisão para o vlog.** Seguir para a Etapa 1 somente se, **já com o C aplicado**, em
put ×16 e update ×16 de 1 MiB s5 via TCP, valerem as três condições:

- (a) a fase ainda perde para o melhor rival;
- (b) os bytes escritos por byte de usuário são ≥ 2;
- (c) commit do inner store + checkpoints + esperas por backpressure do fjall somam ≥ 25% do tempo
  da thread de commit.

Senão, o problema está em CPU ou no TCP, e o vlog não é a próxima alavanca.

**Etapa 1 — WAL segmentado, sem ponteiros.** Implementa, tudo testável sem os ponteiros:

- layout 2;
- cadeia entre segmentos;
- `wal_replay`;
- reciclagem;
- segmento novo a cada abertura;
- checkpoint sem reinício do log;
- migração a partir do layout 1.

Serve tanto ao vlog quanto à alternativa B.

Testes:

- `tests/wal.rs` inteiro parametrizado pelos dois layouts;
- `tests/wal_recovery.rs` com segmentos minúsculos (256 KiB), para forçar trocas de segmento durante
  as mortes de processo;
- escrita rasgada no fim de um segmento do meio da cadeia;
- registro velho válido num segmento reciclado nunca é reaplicado;
- segmento ausente dá erro;
- microbenchmark de alocação: zerado vs. `set_len` vs. append, medindo latência por escrita
  write-through de 1 e 16 MiB.

**Etapa 2 — ponteiros e GC offline.** Implementa:

- op 03, ponteiro e ponteiros provisórios;
- a regra dos Deferred com op 03;
- o fallback de registro grande;
- contadores;
- `format_version` físico 2;
- leitura (`get`, `get_many`, `scan`) e o caminho de `stats` só com cabeçalho;
- GC dentro de `compact` e liberação de segmentos mortos;
- `docs/format.md` §§ 19–20.

Testes:

- vetores golden em `format_compat.rs` e a fixture congelada do vlog;
- `store::conformance` sobre `WalStore<Mem|Redb|Fjall>` com vlog, com limiar e segmentos pequenos
  (I5);
- teste de modelo aleatório contra um mapa em memória, com reabertura e `compact`;
- mortes de processo com vlog, conferindo o BLAKE3 de todo valor confirmado e que nenhum ponteiro
  fica pendurado (varrer `objects` pelo `WalRead`);
- o caso do fjall recuperando do journal um commit Deferred (como
  `a_killed_writer_loses_the_journal_buffer_and_the_wal_restores_it`), para I2;
- dedupe de blocos gravados na mesma transação;
- put seguido de remove do mesmo objeto numa transação;
- `Db::open` num banco com vlog dá `Unsupported`;
- `open_v1_fixtures` continua passando;
- `files()` e o `SpaceProbe` contando os segmentos como dados: hoje o bench exclui só `.wal` e
  `.jnl`, o que já conta `.seg`.

Bench completo, com PG e Mongo feitos pelo agente de benchmark, incluindo o chat, para garantir que
os valores pequenos não regridem.

**Etapa 3 — GC online.** Implementa a thread de GC, o epoch `Arc<SegmentSet>` e a política da § 2.9.

Testes:

- um leitor segurando o snapshot durante a realocação e a liberação continua lendo os valores
  antigos;
- queda em cada passo do GC (failpoints por variável de ambiente no processo filho);
- limite de espaço sob um laço de sobrescritas;
- contadores de amplificação do GC.

**Etapa 4 — otimizações.** Implementa:

- `fetch_chunks` via `get_many` com preads juntados;
- a dica `put_new`;
- criação de segmentos zerados fora do caminho do commit;
- separar quente e frio no GC.

## 8. Perguntas em aberto

- `segment_bytes`: 64 MiB? `vlog_min_bytes`: 4 KiB ou o limiar de 1 KiB do fjall? Os limiares
  `gc_space_ratio` e `gc_min_bytes`?
- Pré-alocação no NTFS: quanto custa, de fato, estender a valid data length com escritas
  write-through de 1–16 MiB, comparado com zerar antes? E no ext4/XFS com `O_DSYNC`?
- Vale estender os ponteiros aos valores inline de `records` (as cargas de 4 KiB)? A proposta é não:
  `records` é lido em toda escrita (expectativas, release), e cada leitura viraria um pread.
- Com C a 1 MiB, quanto sobra do ganho do vlog? A Etapa 0 responde antes de qualquer linha de
  código do vlog.
