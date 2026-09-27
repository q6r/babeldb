# Formato persistente do babeldb — versão 1

Este documento é a especificação normativa do formato de dados da versão 1
(`FORMAT_VERSION = 1`). Ele descreve os bytes, não uma implementação: com ele é
possível escrever um leitor em outra linguagem. A implementação de referência é
[`src/format.rs`](../src/format.rs); os vetores de teste de
[`tests/format_compat.rs`](../tests/format_compat.rs) foram montados à mão a
partir deste texto. Se o texto, o código e os vetores divergirem, isso é um bug
a ser corrigido (sem mudar os bytes de arquivos já gravados).

Termos técnicos (envelope, manifest, codec, refcount, varint, ...) ficam em
inglês, como no código.

## 1. Princípios

- **Recuperação local.** Todo byte necessário para reconstruir os valores está
  no banco local (arquivo redb ou diretório LMDB) mais o código do binário
  (codecs e geradores versionados). Nada depende de rede, relógio ou estado
  externo.
- **Reconstrução exata ou erro.** Um leitor nunca devolve bytes diferentes dos
  gravados: comprimento e BLAKE3 são verificáveis por unidade, e tudo o que o
  leitor não conhece (versão, flag, kind, codec, gerador) é erro.
- **Serialização explícita.** Cada campo é escrito individualmente; nenhuma
  estrutura de memória é gravada como está.
- **Ordem dos bytes.** Inteiros dentro de *valores* são little-endian (LE).
  Inteiros usados como *chave* de tabela são big-endian (BE), para que a ordem
  lexicográfica dos bytes seja a ordem numérica. Única exceção: o `raw_len` da
  chave de `hash_candidates` é LE (a chave só é usada em busca exata).
- **Codificação canônica.** Cada valor lógico tem exatamente uma codificação:
  varints não canônicos, bytes sobrando no fim e campos reservados diferentes de
  zero são rejeitados. Consequência testada: se `decode(b) = v` então
  `encode(v) = b`.
- **Digest.** `digest` é sempre o BLAKE3 padrão (sem chave, saída de 32 bytes)
  dos bytes originais. Ele identifica e verifica; nunca prova igualdade (a
  deduplicação compara bytes, §14).
- **Identificadores.** Ids de objetos, params, fontes, importações e revisões
  são `u64` alocados de contadores em `meta` que começam em 1. O valor 0 é
  reservado ("nenhum"). Ids nunca são reutilizados; esgotar um contador é erro,
  nunca volta a 0.

## 2. Backends

O conteúdo lógico (9 tabelas de pares chave → valor, ambos sequências de bytes)
é idêntico em todos os backends. O formato físico do arquivo pertence ao
backend e segue o versionamento dele.

| backend | disponibilidade | armazenamento físico |
|---|---|---|
| redb 4.3 | padrão | um arquivo; cada tabela lógica é uma `TableDefinition<&[u8], &[u8]>` com o nome da §3 |
| LMDB (heed 0.22) | feature `lmdb` | um diretório com `data.mdb` e `lock.mdb`; cada tabela é um *named database* com o nome da §3 |
| outros (ex.: fjall) | experimentais, por feature | layout físico documentado no módulo do backend; mesmo conteúdo lógico |

Contrato exigido de qualquer backend:

- chaves ordenadas por comparação lexicográfica de bytes sem sinal;
- uma transação de escrita cobre todas as tabelas e é publicada atomicamente;
  leitores veem snapshots consistentes; no máximo um escritor por vez;
- `Immediate`: durável quando o commit retorna (fsync ou equivalente);
  `Deferred`: visível às transações seguintes, podendo ser perdido numa queda
  até o próximo commit `Immediate` (backends sem equivalente tratam como
  `Immediate`). Só as transações intermediárias de importação usam `Deferred`.

Converter um banco entre backends é copiar as 9 tabelas byte a byte.

## 3. Tabelas lógicas

| tabela | chave | valor |
|---|---|---|
| `meta` | nome UTF-8 (§4) | depende da entrada (§4) |
| `records` | chave do usuário (bytes arbitrários, não vazia) | `Manifest` (§6) da revisão atual |
| `objects` | `object_id` u64 BE | envelope (§5) |
| `hash_candidates` | `digest[32] ++ raw_len` (u32 **LE**), 36 bytes | lista de ids (§3.1) |
| `refcounts` | `object_id` u64 BE | u64 LE ≥ 1 |
| `params` | `param_id` u64 BE | `Param` (§9) |
| `history` | chave de histórico (§11) | `Manifest` de uma revisão anterior |
| `sources` | `source_id` u64 BE | `SourceDescriptor` (§12) |
| `pending_imports` | `import_id` u64 BE | lista de ids (§3.1) |

O motor limita chaves de usuário a `max_key_len` (configuração de execução,
padrão 4 KiB, máximo 64 KiB) e rejeita a chave vazia; o formato não fixa outro
limite.

### 3.1 Listas de ids

Concatenação de ids `u64` LE; o comprimento é múltiplo de 8 (senão, erro).
Exemplo: `[1, 0x0102030405060708]` →
`01 00 00 00 00 00 00 00  08 07 06 05 04 03 02 01`.

- Em `hash_candidates`: não vazia (a chave é removida quando esvazia), sem
  repetição, e cada id existe em `objects` com o mesmo `digest` e `raw_len` da
  chave.
- Em `pending_imports`: ids na ordem em que os blocos foram gravados; um id
  pode repetir (bloco repetido e deduplicado) e **cada ocorrência vale uma
  referência** (§14).

## 4. Tabela `meta`

| nome | tipo | regra |
|---|---|---|
| `format_version` | u32 LE | obrigatória; `1`. Qualquer outro valor: o banco não é aberto (erro `Unsupported`) |
| `mode` | u8 | `1` = BabelPure, `2` = Adaptive |
| `block_size` | u32 LE | 512 ≤ `block_size` ≤ 1 MiB |
| `inline_max` | u32 LE | ≤ `block_size` |
| `next_object_id` | u64 LE | próximo id de objeto; começa em 1 |
| `next_revision` | u64 LE | próxima revisão; começa em 1 |
| `next_param_id` | u64 LE | próximo id de param; começa em 1 |
| `next_source_id` | u64 LE | próximo id de fonte; começa em 1 |
| `next_import_id` | u64 LE | próximo id de importação; começa em 1 |
| `active_zstd_dict` | u64 LE (opcional) | param `ZSTD_DICT` usado em novas escritas; ausente = nenhum |
| `active_template` | u64 LE (opcional) | param `TEMPLATE` usado em novas escritas; ausente = nenhum |
| `created_by` | UTF-8 | informativo, ex. `babeldb 0.1.0` |

`mode`, `block_size` e `inline_max` são parâmetros de criação: gravados quando
o banco é criado, eles prevalecem sobre a configuração passada em aberturas
posteriores. Todo id presente numa tabela é menor que o contador
correspondente; toda revisão gravada é menor que `next_revision`. Um contador
em `u64::MAX` está esgotado. Escritores v1 não gravam outras entradas.

## 5. Envelope de objeto

Toda unidade armazenada (um bloco em `objects` ou um valor inline dentro de um
manifesto) é um envelope: cabeçalho fixo de 64 bytes seguido do corpo do codec.

| offset | bytes | campo | regra |
|---|---|---|---|
| 0 | 4 | `magic` | ASCII `BBO1` (`42 42 4f 31`) |
| 4 | 1 | `codec_id` | tabela permanente (§8) |
| 5 | 1 | `codec_version` | versão do codec |
| 6 | 2 | `flags` | u16 LE; deve ser 0 |
| 8 | 4 | `raw_len` | u32 LE; comprimento decodificado; ≤ `MAX_UNIT_LEN` = 2^24 (16 MiB) |
| 12 | 4 | `body_len` | u32 LE; = comprimento total − 64 |
| 16 | 8 | `aux_id` | u64 LE; param de que o corpo depende, ou 0 |
| 24 | 32 | `digest` | BLAKE3 dos `raw_len` bytes originais |
| 56 | 8 | `reserved` | deve ser 0 |
| 64 | `body_len` | `body` | corpo do codec |

Validação, nesta ordem, antes de qualquer alocação proporcional a `raw_len`:

1. pelo menos 64 bytes;
2. `magic` correto;
3. `flags == 0` (qualquer bit desconhecido é erro);
4. `reserved == 0`;
5. `raw_len ≤ MAX_UNIT_LEN`;
6. `body_len` igual ao número de bytes que seguem o cabeçalho.

O par `(codec_id, codec_version)` não é validado nessa camada, para que
inspeção e verificação consigam relatar um codec desconhecido; ele é validado
na decodificação (§8). A decodificação precisa produzir exatamente `raw_len`
bytes, e com verificação ligada (padrão na leitura e sempre em `verify --deep`)
`BLAKE3(saída) == digest`.

Exemplo — `RawV1` de `"abc"` (67 bytes):

```text
off  bytes                     campo
  0  42 42 4f 31               magic "BBO1"
  4  01                        codec_id 0x01 (RawV1)
  5  01                        codec_version 1
  6  00 00                     flags
  8  03 00 00 00               raw_len = 3
 12  03 00 00 00               body_len = 3
 16  00 00 00 00 00 00 00 00   aux_id = 0
 24  64 37 b3 ac 38 46 51 33   digest = BLAKE3("abc")
     ff b6 3b 75 27 3a 8d b5
     48 c5 58 46 5d 79 db 03
     fd 35 9c 6c d5 bd 9d 85
 56  00 00 00 00 00 00 00 00   reserved
 64  61 62 63                  body
```

O valor vazio é um envelope de 64 bytes sem corpo (`raw_len = body_len = 0`,
`digest` = BLAKE3 de zero bytes = `af1349b9…e41f3262`).

## 6. Manifesto

Valor de `records` e de `history`. Descreve uma revisão de uma chave.

| offset | bytes | campo | regra |
|---|---|---|---|
| 0 | 1 | `manifest_version` | = 1 |
| 1 | 8 | `revision` | u64 LE |
| 9 | 8 | `logical_len` | u64 LE; comprimento do valor |
| 17 | 1 | `kind` | 0 Inline, 1 Chunks, 2 Generated, 3 Tombstone |
| 18 | 1 | `flags` | bit 0 = `has_source`; demais bits devem ser 0 |
| 19 | 8 | `source_id` | u64 LE; presente somente se `has_source` |
| 19 ou 27 | … | corpo do kind | abaixo |

Corpos:

| kind | corpo |
|---|---|
| 0 Inline | `varint len` + envelope completo de `len` bytes |
| 1 Chunks | `varint count` + `count` × (`logical_end` u64 LE, `object_id` u64 LE) — 16 bytes por referência |
| 2 Generated | `generator_id` u16 LE, `generator_version` u16 LE, `varint params_len`, `params`, `digest[32]` |
| 3 Tombstone | vazio |

Regras de validação (qualquer violação é erro de formato):

- `manifest_version`, `kind` e `flags` conhecidos;
- nenhum byte sobrando depois do corpo; nenhum campo truncado;
- **Inline:** o envelope embutido passa pela §5 e `raw_len == logical_len`;
- **Chunks:** `count ≥ 1`; `count × 16` não excede os bytes restantes
  (verificado antes de alocar); `logical_end` estritamente crescente (logo o
  primeiro é > 0 e nenhum chunk é vazio); o último `logical_end` é igual a
  `logical_len`; `object_id ≠ 0`;
- **Generated:** `params_len ≤ MAX_PARAMS_LEN` (65 536);
- **Tombstone:** `logical_len == 0`.

Semântica:

- **Inline** guarda o envelope dentro do manifesto (sem ir a `objects`, sem
  deduplicação). É usado quando `logical_len ≤ inline_max`. O valor vazio é
  sempre Inline, porque Chunks exige pelo menos um chunk.
- **Chunks**: o chunk *i* cobre `[logical_end(i−1), logical_end(i))`, com
  `logical_end(−1) = 0`. O comprimento do chunk tem de ser igual ao `raw_len`
  do objeto referenciado (verificado na leitura). O escritor corta o valor em
  blocos de `block_size` (o último pode ser menor), mas leitores usam apenas os
  `logical_end`. O mesmo objeto pode aparecer várias vezes (deduplicação). Uma
  leitura de intervalo localiza os chunks por busca binária em `logical_end`.
- **Generated**: o valor é a saída do gerador registrado
  `(generator_id, generator_version)` (§10) para `params`. Ao ler: gerador
  desconhecido é erro `UnknownGenerator`; `output_len(params)` tem de ser igual
  a `logical_len`; com verificação, `BLAKE3(saída) == digest`. Nenhum objeto é
  gravado.
- **Tombstone**: marca de exclusão, gravada só quando o histórico está ligado;
  lê-se como chave ausente. Tombstones podem ir para `history`.
- `revision` vem de `next_revision`: única e crescente no banco inteiro, não
  por chave. `source_id`, quando presente, referencia `sources` (§12).

Exemplo — Chunks sem fonte (52 bytes):

```text
off  bytes                     campo
  0  01                        manifest_version 1
  1  02 01 00 00 00 00 00 00   revision = 258
  9  e8 03 00 00 00 00 00 00   logical_len = 1000
 17  01                        kind = Chunks
 18  00                        flags (sem fonte)
 19  02                        varint count = 2
 20  00 02 00 00 00 00 00 00   chunk 0: logical_end = 512
 28  01 00 00 00 00 00 00 00            object_id = 1
 36  e8 03 00 00 00 00 00 00   chunk 1: logical_end = 1000
 44  02 00 00 00 00 00 00 00            object_id = 2
```

Com fonte, `flags = 01` e os 8 bytes de `source_id` entram no offset 19; o
restante se desloca 8 bytes. Um Tombstone da revisão 12 tem 19 bytes:
`01 0c00000000000000 0000000000000000 03 00`.

## 7. Varint

LEB128 sem sinal: 7 bits por byte, grupo menos significativo primeiro, bit
0x80 = "continua". Regras do decodificador:

- no máximo 10 bytes; no 10º byte o payload é ≤ 1 e não há continuação
  (valores até 2^64 − 1);
- canônico: o último byte só pode ser `00` se for o único byte (`80 00` para 0
  é rejeitado);
- truncado (continuação sem próximo byte) é erro.

| valor | bytes |
|---|---|
| 0 | `00` |
| 127 | `7f` |
| 128 | `80 01` |
| 300 | `ac 02` |
| 16 383 | `ff 7f` |
| 16 384 | `80 80 01` |
| 2^32 − 1 | `ff ff ff ff 0f` |
| 2^64 − 1 | `ff ff ff ff ff ff ff ff ff 01` |

Rejeitados, por exemplo: `80 00`, `ff 00`, `81 80 00` (não canônicos),
`ff…ff 02` (estouro no 10º byte), `80…80 80 01` com 11 bytes, `80` (truncado).

## 8. Codecs

`(codec_id, codec_version)` identifica o formato do corpo. **Os ids são
permanentes**: nunca renumerados, nunca reutilizados, nunca derivados da ordem
de um enum. Mudar o formato de um corpo exige nova versão ou novo id; leitores
mantêm os antigos.

| id | versão | nome | dependência (`aux_id`) |
|---|---|---|---|
| `0x01` | 1 | RawV1 | nenhuma (0) |
| `0x02` | 1 | RepeatV1 | nenhuma (0) |
| `0x03` | 1 | ArithmeticU64V1 | nenhuma (0) |
| `0x10` | 1 | Lz4V1 | nenhuma (0) |
| `0x11` | 1 | ZstdV1 | 0 = sem dicionário; ≠ 0 = param `ZSTD_DICT` |
| `0x20` | 1 | BabelAffineV1 | nenhuma (0) |
| `0x30` | 1 | TemplatePatchV1 | sempre um param `TEMPLATE` |

Todos os outros pares são desconhecidos e produzem o erro `UnknownCodec` na
decodificação. `0x40` não é codec: "Generated" é um kind de manifesto (§6).
Uma dependência exigida e ausente produz `MissingDependency` (inclusive
`TemplatePatchV1` com `aux_id = 0`). Um decodificador precisa produzir
exatamente `raw_len` bytes ou falhar, validando comprimentos antes de alocar.

Corpos:

- **RawV1** — os próprios bytes; `body_len == raw_len`.
- **RepeatV1** — `period` u32 LE seguido de `motif` com exatamente `period`
  bytes; `1 ≤ period ≤ raw_len`; saída `out[k] = motif[k mod period]` para
  `k < raw_len` (a última repetição pode ser parcial). Ex.: `02000000 6162`
  com `raw_len = 5` → `ababa`.
- **ArithmeticU64V1** — 24 bytes: `start`, `step`, `count` (u64 LE cada);
  `raw_len == 8 × count`; saída: os valores `start + i·step` (i = 0..count) em
  u64 LE, sem estouro (passo sem sinal: só sequências não decrescentes cujo
  último valor cabe em u64). Ex.: `(1, 2, 3)` → `01..00 03..00 05..00`.
- **Lz4V1** — um bloco LZ4 independente (formato de bloco, sem frame, sem
  prefixo de tamanho); o tamanho decodificado vem de `raw_len` e tem de ser
  exato. Ex.: `30 61 62 63` → `abc`.
- **ZstdV1** — exatamente um frame Zstandard (RFC 8878) cujo cabeçalho declara
  `Frame_Content_Size == raw_len`. Com `aux_id ≠ 0`, o dicionário são os bytes
  do param `aux_id` (kind `ZSTD_DICT`), carregados em modo automático
  (começando pelo magic `0xEC30A437`: dicionário formatado; senão, conteúdo
  bruto). A dependência é identificada só por `aux_id`; o `Dictionary_ID` do
  frame, se houver, é informativo. Ex.: `28b52ffd 20 03 190000 616263` → `abc`.
- **BabelAffineV1** — bijeção afim sobre inteiros de `n = raw_len` bytes:

  ```text
  M = 2^(8n)          x = inteiro big-endian dos n bytes originais
  codificar: seed = ((x − 1) · 5⁻¹) mod M
  decodificar: x  = (5 · seed + 1) mod M
  ```

  O corpo é a seed com exatamente `n` bytes, big-endian (`body_len == raw_len`);
  `n = 0` é a seed vazia. Como 5 é ímpar, o mapa é inversível módulo 2^k: cada
  valor tem uma única seed, **do mesmo tamanho do valor**, logo este codec não
  economiza espaço por construção. Vetores: `48 69` ("Hi") ↔ `db 48`;
  `61 62 63` ("abc") ↔ `79 e0 7a`; `00` ↔ `33`; `01` ↔ `00`; `ff` ↔ `66`.
  Construção didática: não é o algoritmo do site da Biblioteca de Babel nem
  criptografia.
- **TemplatePatchV1** — saída reconstruída a partir do template imutável do
  param `aux_id` (kind `TEMPLATE`) e de operações exatas (cópias do template e
  literais); decodificar só copia bytes. O layout do corpo está definido na
  documentação do módulo [`src/codec/template_patch.rs`](../src/codec/template_patch.rs),
  que é a definição normativa deste codec.

## 9. Params (dependências compartilhadas)

Valor da tabela `params`: `kind` u8 | `version` u16 LE | bytes (o resto).

| kind | nome | bytes |
|---|---|---|
| 1 | `ZSTD_DICT` | dicionário Zstandard (1 byte a 16 MiB) |
| 2 | `TEMPLATE` | template de `TemplatePatchV1` |

`version` deve ser 1; kind ou versão desconhecidos são erro. Exemplo: um
template `"tpl"` é `02 0100 74706c`.

Params são imutáveis e referenciados pelo `aux_id` de envelopes em `objects`,
de envelopes inline em `records` **e em `history`**, e por
`meta.active_zstd_dict` / `meta.active_template`. Um param só pode ser removido
quando nada o referencia; o envelope que o usa e o param são necessários para a
recuperação e entram na contabilidade (§17).

## 10. Geradores

Um gerador é código versionado do binário que expande `params` em uma sequência
exata de bytes com acesso aleatório. A saída depende só de `(params, offset,
comprimento)`: nunca de hora, plataforma ou PRNG não especificado. **Um par
`(id, versão)` publicado fica congelado**: mudar qualquer byte de saída exige
nova versão, e a antiga continua registrada.

| id | versão | nome | params | saída |
|---|---|---|---|---|
| 1 | 1 | `ARITH_U64` | 24 bytes: `start` u64 LE, `step` u64 LE, `count` u64 LE | `count` valores `start + i·step` em u64 LE; estouro é erro; `count = 0` é válido |
| 2 | 1 | `BLAKE3_XOF` | 40 bytes: `key[32]`, `len` u64 LE | os primeiros `len` bytes do XOF do BLAKE3 com chave `key` sobre a mensagem vazia |
| 3 | 1 | `REPEAT` | `total_len` u64 LE, depois `motif` (1 a 64 KiB, o resto) | `total_len` bytes, `out[k] = motif[k mod len(motif)]`; `total_len = 0` é válido |

Params inválidos são erro, nunca bytes errados. Num manifesto, `params` também
respeita `MAX_PARAMS_LEN` (um `REPEAT` com motivo de 64 KiB não cabe). O código
dos geradores faz parte da representação armazenada e entra na contabilidade.

## 11. Chaves de `history`

```text
history_key(chave, revisão) = escape(chave) ++ 00 00 ++ revisão (u64 BE)
escape: cada byte 00 vira 00 FF; os demais bytes ficam iguais
```

Como todo `00` da parte escapada é seguido de `FF`, o primeiro `00 00` é o
terminador. O parser rejeita `00` seguido de qualquer outro byte, chave sem
terminador e revisão que não tenha exatamente 8 bytes.

Propriedade de ordem: `history_key(k1, r1) < history_key(k2, r2)` se e só se
`(k1, r1) < (k2, r2)` (chave comparada byte a byte, prefixo primeiro; depois a
revisão numericamente). Motivo: se `k1` é prefixo próprio de `k2`, o
terminador `00 00` é menor que a continuação de `k2` (`c ≥ 01`, ou `00 FF`); se
diferem num byte, a menor das formas escapadas é a do byte menor; se as chaves
são iguais, decide a revisão em BE. Todas as revisões de uma chave são
exatamente as entradas com prefixo `escape(chave) ++ 00 00`.

| chave | revisão | chave de histórico |
|---|---|---|
| `a` | 1 | `61 0000 0000000000000001` |
| (vazia) | 0x0102030405060708 | `0000 0102030405060708` |
| `a\0b` | 5 | `61 00ff 62 0000 0000000000000005` |
| `\xff\0\xff` | 2 | `ff 00ff ff 0000 0000000000000002` |
| `\0\0` | 2^64 − 1 | `00ff 00ff 0000 ffffffffffffffff` |

O valor é o manifesto (qualquer kind, inclusive Tombstone) daquela revisão: a
revisão da chave é igual à do manifesto e menor que a do registro atual da
mesma chave. O histórico só é gravado com `keep_history` ligado.

## 12. Fontes (`sources`)

Proveniência opcional compartilhada por registros importados. Nunca substitui o
conteúdo armazenado — uma importação confirmada continua legível depois que a
fonte desaparece — e nunca guarda credenciais.

```text
u8  version = 1
u8  kind              1 LOCAL_FILE, 2 GENERATOR, 3 EXTERNAL (informativo)
u16 adapter_version   LE
varint len, location  len bytes UTF-8 válidos
u8  has_last          0 ou 1
se has_last = 1:
  u64 revision        LE, revisão publicada pela última importação
  u64 bytes           LE, bytes importados
  [32] digest         BLAKE3 dos bytes importados
  u64 unix_ms         LE, horário da importação
```

Versão ≠ 1, UTF-8 inválido, `has_last` fora de {0, 1}, campo truncado ou bytes
sobrando são erro. O `kind` é informativo (não afeta a reconstrução) e não é
validado. Exemplo sem última importação (16 bytes):
`01 01 0100 0a 646174612f612e62696e 00` (local `data/a.bin`).

## 13. Importações pendentes (`pending_imports`)

Uma importação grava blocos em transações intermediárias (`Deferred`): cada uma
armazena ou deduplica objetos, toma uma referência provisória por bloco e
acrescenta os ids à lista `pending_imports[import_id]` (o id vem de
`next_import_id`). A transação final (`Immediate`) publica o manifesto — as
referências provisórias passam a ser dele —, remove a linha pendente e atualiza
`last_import` da fonte. Nada fica visível antes dela.

Uma importação interrompida deixa a linha pendente e as referências dela. A
limpeza da própria importação ou a manutenção (`gc`) as libera: decrementa uma
referência por ocorrência de id e remove a linha.

## 14. Regra de vida dos objetos e deduplicação

Para todo objeto `o`:

```text
refcounts[o] = ocorrências de o nas listas de chunks dos manifestos de records
             + ocorrências de o nas listas de chunks dos manifestos de history
             + ocorrências de o nas listas de pending_imports
```

- A linha de `refcounts` existe se e só se o objeto existe, com valor ≥ 1.
- Ao chegar a zero, na mesma transação, são removidos o objeto, o seu id na
  lista de `hash_candidates` (a chave inteira, se a lista esvaziar) e a linha
  de `refcounts`.
- Envelopes inline nunca entram em `objects` nem em `hash_candidates`.
- Deduplicação (só no modo Adaptive): um candidato de `hash_candidates` só é
  reutilizado depois que os seus bytes decodificados são comparados byte a
  byte com o bloco novo. Um digest igual nunca é prova de igualdade.
- No modo BabelPure a deduplicação está desligada: `hash_candidates` fica
  vazia e toda unidade armazenada é `BabelAffineV1` sem `aux_id`.

`verify` recalcula essas contagens; `gc` corrige desvios e recolhe
importações abandonadas, objetos órfãos, candidatos pendentes e params sem uso.

## 15. Caminho de leitura

`chave → records → manifesto` (um lookup), depois conforme o kind: Inline
decodifica o envelope embutido; Chunks localiza por busca binária os chunks que
intersectam o intervalo pedido, lê só esses objetos, decodifica e verifica
comprimento e BLAKE3; Generated chama o gerador para o intervalo; Tombstone ou
ausência é "não encontrado". O índice de hashes não participa da leitura.

## 16. Regras de compatibilidade

- `meta.format_version ≠ 1`: o banco não é aberto.
- Versões, flags, kinds e campos reservados desconhecidos em envelope,
  manifesto, param e descritor de fonte são erro — nunca ignorados.
- Codec desconhecido é erro `UnknownCodec`; gerador desconhecido,
  `UnknownGenerator`; dependência ausente, `MissingDependency`.
- São permanentes e nunca renumerados nem reaproveitados: ids de codec, ids de
  gerador, kinds de param, kinds de fonte, kinds de manifesto, nomes de tabela
  e de entradas de `meta`, o magic `BBO1`.
- Qualquer mudança de layout exige uma versão nova da estrutura afetada; se um
  leitor v1 pudesse interpretar os bytes novos de outro jeito, exige também um
  novo `FORMAT_VERSION`. Escritores nunca ligam bits desconhecidos; leitores
  nunca "consertam" dados em silêncio.
- **Leitores v1 continuam funcionando.** Os bancos congelados
  `tests/data/v1_adaptive.redb` e `tests/data/v1_babel_pure.redb` (com os
  valores esperados em `tests/data/v1_expected.txt`) nunca são regenerados; o
  teste `open_v1_fixtures` precisa passar em toda versão futura.

## 17. Contabilidade de espaço

Há dois níveis, e as duas medidas são reportadas separadamente:

- **payload** — o que o motor entrega ao backend: Σ (chave + valor) de cada
  tabela (`Stats::payload_bytes`);
- **arquivo** — tamanho aparente e **alocado** dos arquivos do backend. É a
  verdade de chão: inclui páginas, cabeçalhos de página, fragmentação e espaço
  livre interno. Remover linhas não encolhe o arquivo sem `compact`.

Tudo o que é necessário para recuperar os dados conta:

```text
total = seeds / receitas / corpos de codec
      + 64 bytes de cabeçalho por envelope (objetos e inline)
      + manifestos e chaves (records)
      + índices: hash_candidates, refcounts, pending_imports
      + dicionários e templates (params)
      + histórico (manifestos retidos e os objetos que só eles mantêm)
      + fontes (sources) e meta
      + espaço interno do backend (páginas, fragmentação, páginas livres)
      + código de codecs e geradores atribuível no binário
      + qualquer descrição mantida pelo cliente fora do banco
```

Nenhum ganho é declarado comparando só corpos. No modo BabelPure o corpo tem
exatamente o tamanho do valor e cada unidade ainda paga 64 bytes de envelope: o
total nunca fica abaixo do tamanho dos dados e coincide com o de "motor + Raw".
Qualquer economia vem do modo Adaptive e é medida em bytes alocados.

## 18. Vetores de teste

`tests/format_compat.rs` contém, em hexadecimal escrito campo a campo: os
envelopes deste documento, manifestos dos quatro kinds (Chunks com e sem
fonte), params, descritores de fonte com e sem `last_import`, chaves de
histórico com bytes `00` e `FF`, chaves de candidatos, listas de ids e varints
nas bordas; testes de rejeição para cada regra das §§ 5–12; testes de
propriedade (ida e volta, codificação canônica, nenhum pânico com bytes
arbitrários ou mutados, ordem das chaves de histórico); um validador
independente das tabelas (§§ 3, 4, 13, 14); e os bancos congelados da §16.
