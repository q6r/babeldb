# Benchmarks do babeldb — método, ambiente e reprodução

> **Estado (27/09/2026, tarde):** motor integrado; a seção 9 traz os primeiros resultados
> medidos (1 repetição por combinação, commit `c6a92f5`, cache de arquivos do Windows quente,
> máquina sem outras cargas). São medições de uma execução, não medianas de repetições: use
> `scripts/bench-matrix.ps1 -Reps 5` para intervalos.

Base: especificação §11 (benchmark) e §4 (critérios de comparação). Código:
`src/datasets.rs` (cenários), `src/sys.rs` (métricas do SO), `benches/engine.rs` (harness),
`benchdata/manifest.json` (cenários + digests), `scripts/` (repetições e resumo).

## 1. Objetivo

Medir, para cada variante de armazenamento e cada cenário:

- **espaço**: bytes aparentes **e** alocados dos arquivos do backend, decompostos pelo
  `stats()` do motor (manifestos, objetos, índices, params, histórico);
- **latência por operação** (p50/p95/p99/p99.9/máx) e **vazão** de: carga em lote, `put`
  durável, `get`, `get` de chave ausente, `get_range` curto, "últimas 50 mensagens do canal"
  (S3), leitores concorrentes e mistura leitura/escrita;
- **custo de processo**: CPU usuário/kernel, working set (RSS), bytes privados, page faults,
  bytes/operações de I/O pedidos ao SO;
- **amplificação de leitura**: bytes reconstruídos ÷ bytes pedidos (contadores do motor).

O caso de uso que orienta os parâmetros padrão é o de um chat "estilo Discord": muitos
registros pequenos, leitura das mensagens mais recentes de um canal por varredura reversa
de prefixo, leitores concorrentes.

## 2. Variantes

| `--variant` | configuração | papel |
|---|---|---|
| `raw-backend` | sem motor: chave/valor gravados direto em `Table::Records` pelo trait `Store` (uma transação por lote/put, commit `Immediate`) | referência do backend |
| `engine-raw` | `Config::raw_only()` (só `RawV1`, sem dedupe) | custo do formato (envelope 64 B + manifesto) |
| `babel-pure` | `Config::babel_pure()` (`BabelAffineV1`, sem dedupe) | endereçamento reversível; hipótese §11: bytes ≥ Raw |
| `lz4` | Adaptive com `CodecPolicy{lz4}` + Raw, sem dedupe | efeito isolado do LZ4 |
| `zstd` | Adaptive com `CodecPolicy{zstd}` (sem dicionário) + Raw, sem dedupe | efeito isolado do Zstd |
| `adaptive-nodedupe` | política padrão, `dedupe = false` | Adaptive sem dedupe |
| `adaptive` | política padrão com dedupe verificado byte a byte | Adaptive completo |

`--block-size`, `--inline-max`, `--cache-bytes` e `--backend-cache-bytes` valem para todas
as variantes do motor (padrões de `Config::default()`: 16 KiB, 1024 B, 64 MiB, 64 MiB).
Backends: `--backend redb` (padrão), `--backend lmdb` (exige `--features lmdb`) e
`--backend mem` (só teste de fumaça do harness; o `MemStore` copia todas as tabelas a cada
escrita e **não** é benchmark).

Não coberto pelo harness: a variante "Adaptive com `put_generated`" da §11 (outra API; exige
geradores registrados e parâmetros por registro).

## 3. Cenários (hipóteses explícitas no lugar de um corpus de produção)

Não há corpus real disponível. Cada cenário é uma **hipótese declarada**; resultados sobre
eles não prevêem resultados sobre dados reais. Todos são funções puras de
`(cenário, seed, i, value_size)` — aritmética inteira, splitmix64 e BLAKE3 (XOF) —, portanto
idênticos em qualquer plataforma; S6 depende também da saída da libzstd 1.5.7 (nível 3).
Os primeiros `n` registros de um dataset maior são o dataset de `n` registros.
`benchdata/manifest.json` guarda descrições, parâmetros e digests BLAKE3 para 10 000
registros × {64, 256, 1 Ki, 4 Ki, 16 Ki, 64 Ki} B (seed 42); o load de cada execução grava o
digest do que carregou (`dataset_blake3`).

| id | conteúdo | hipótese | chave |
|---|---|---|---|
| S1 | `i` par: zeros; `i` ímpar: motivo aleatório de 2..64 B repetido | repetição é capturada por `RepeatV1`/compressores; BabelPure fica do tamanho do Raw | `s1-repetitive/{i:012}` |
| S2 | termos u64 LE de progressão aritmética (início < 2^48, passo 1..65536) | contadores/offsets reconhecidos por `ArithmeticU64V1` (corpo constante) | `s2-sequences/{i:012}` |
| S3 | JSON de mensagem estilo Discord: `id` snowflake (época Discord, 42 bits de ms ≪ 22 \| worker ≪ 17 \| process ≪ 12 \| incremento), `channel_id`, `author{id, username}`, `content` com palavras de lista fixa de 256, `timestamp` ISO-8601, `mentions []`, `pinned false`; 1000 canais com popularidade Zipf (expoente 1); 10 000 autores; 100 ms entre mensagens em média | carga de chat: registros pequenos com template fixo e vocabulário pequeno; varredura reversa por prefixo do canal devolve as mensagens mais recentes | `ch/{canal:08}/msg/{snowflake:020}` |
| S4 | exatamente 3 de cada 10 registros consecutivos (nunca o 0) copiam o valor de um registro anterior sorteado em `[0, i)`; os demais são BLAKE3-XOF | dedupe byte a byte remove duplicatas; compressão não ajuda | `s4-duplicates/{i:012}` |
| S5 | BLAKE3-XOF com seed desconhecida do planner | dado incompressível: todo candidato cai em Raw e o overhead é custo puro | `s5-high-entropy/{i:012}` |
| S6 | um frame zstd nível 3 de texto JSON-lines estilo S3 por registro, com tamanho ajustado a `value_size` (busca por secante, ≤ 8 compressões) | payload já comprimido: recomprimir não ganha nada e custa CPU | `s6-compressed/{i:012}` |

Consequências que o leitor precisa ter em mente:

- **S3 tem tamanho mínimo**: o template sem conteúdo ocupa ~207–221 B (registro 0: 213 B);
  com `--value-size` menor que isso os valores ficam maiores que o pedido (o manifest
  registra `min/max_value_len`). A partir daí o valor tem exatamente `value_size` bytes.
- **S4 e dedupe**: valores com `len <= inline_max` ficam inline no manifesto e **não**
  participam de dedupe (spec §6.2). Com os padrões (`inline_max` = 1024) o efeito do dedupe
  só aparece com `--value-size` > 1024.
- **S6**: tamanhos variam alguns por cento em torno de `value_size` (1 KiB: 971–1062 B;
  64 KiB: 64 298–66 811 B no manifest).
- **S2** com `value_size` não múltiplo de 8 deixa um termo parcial no fim, que
  `ArithmeticU64V1` não reconhece (por projeto).

## 4. Fases e definições das métricas

Uma execução = (backend, variante, cenário), em diretório próprio sob `--dir`
(padrão `bench-results/tmp`, ignorado pelo git), apagado no fim (salvo `--keep`).

| fase | o que faz | métrica principal |
|---|---|---|
| `load` (a) | `n` registros em lotes de `--batch` via `write_batch` (um commit durável por lote; no `raw-backend`, uma transação por lote) | MB/s e registros/s sobre a soma dos tempos de commit; percentis por lote |
| `space` | após o load: arquivos do diretório (aparente = fim de arquivo; alocado = `FILE_STANDARD_INFO.AllocationSize`), bytes do usuário, `stats()` do motor | bytes alocados, amplificação alocado ÷ (chaves + valores do usuário) |
| `put-durable` (b) | `--durable-puts` inserções novas, uma por commit `Immediate` | p50/p95/p99/máx |
| `get` (c) | `--ops` gets de registros carregados, distribuição `--dist` | percentis; ops/s = n ÷ Σ latências |
| `miss` (d) | gets de chaves ausentes que ordenam logo após uma chave real ("near miss") | percentis |
| `range` (e) | `get_range` de `--range-len` (100) B em offset aleatório | percentis |
| `latest` (f, só S3) | `ScanOptions::prefix(channel_prefix(c)).reverse(true).limit(50).with_values(true)`; canal uniforme ou Zipf pela popularidade dos dados | percentis; itens por varredura |
| `mt-get` (g) | `T` threads (`--readers`, aceita lista) fazendo gets | ops/s agregado = total ÷ (liberação da barreira → fim da última thread); percentis de todas as amostras |
| `mixed` (h) | `T` threads com 95/5 ou 50/50 (`--mix`) de gets e `put` duráveis de registros novos | ops/s; percentis de leitura e de escrita separados |
| `space-final` | contabilidade após todas as fases + tamanhos após fechar o banco | idem `space` |

Regras de medição:

- **Só a operação é cronometrada.** Geração de dados, chaves das fases de leitura
  (pré-computadas num buffer contíguo), verificação completa e `stats()` ficam fora dos
  cronômetros. Cada latência é um `u64` em ns (`std::time::Instant` = `QueryPerformanceCounter`
  no Windows; neste computador os valores observados são múltiplos de 100 ns).
- **Percentis por posto mais próximo** (*nearest rank*) com aritmética inteira: pXX é a menor
  amostra com pelo menos XX % das amostras ≤ ela (p99,9 de 1000 amostras = 999ª).
- **Reabertura**: antes de cada fase de leitura o banco é fechado e reaberto (caches do
  processo vazios; o cache de arquivos do SO continua quente — ver §5). `open_ns` registra o
  tempo de abertura. `--no-reopen` mantém o banco aberto entre fases.
- **Aquecimento**: `--warmup` operações da mesma distribuição (padrão = `--ops`), não
  registradas, antes de cada fase de leitura (`--warmup 0` mede caches de processo frios).
- **Distribuições**: `uniform`; `zipf` (Gray et al./YCSB, θ = 0,99, *scrambled*: os postos
  quentes são espalhados pelo espaço de chaves por uma bijeção); `latest` (Zipf sobre
  recência: os registros mais novos são os mais lidos).
- **Verificação**: todo valor lido tem o comprimento conferido; a cada `--verify-every` (64)
  leituras o valor é comparado byte a byte com o dataset regenerado (fora do cronômetro);
  `latest` confere contagem, prefixo, ordem decrescente e a mensagem mais nova. Divergência
  aborta a execução com uma linha `error`.
- **Contadores do motor por fase**: `stats()` após cada fase reaberta; cobrem
  aquecimento + fase. `read_amplification` = `bytes_reconstructed ÷ bytes_requested`.
  Com `--no-reopen` não são coletados por fase (o `stats()` varre todas as tabelas e
  poluiria os caches da fase seguinte).
- **Métricas do processo** (`src/sys.rs`): no load e no put durável são somadas apenas as
  diferenças medidas em volta de cada commit (excluem a geração de dados); nas fases de
  leitura cobrem o laço inteiro (inclui as conferências de comprimento). Windows:
  `GetProcessMemoryInfo` (working set, pico do working set **desde o início do processo**,
  `PrivateUsage`, page faults = soft + hard), `GetProcessTimes` (CPU com granularidade do tick,
  tipicamente 15,6 ms: fases curtas podem registrar 0), `GetProcessIoCounters` (chamadas de
  leitura/escrita, **incluindo as servidas pelo cache do SO**; leituras via mmap — LMDB — não
  entram). Não é I/O físico de disco.

Contabilidade de espaço (fórmula da §11) e onde cada termo aparece no JSON:

| termo da §11 | campo |
|---|---|
| seeds/receitas/corpos + envelopes | `engine.object_bytes`, `engine.inline_envelope_bytes`, `engine.per_codec[]` |
| manifestos | `engine.manifest_bytes` (inclui envelopes inline), `engine.key_bytes` |
| índices | `engine.candidate_bytes`, `engine.refcount_bytes` |
| dicionários/templates | `engine.param_bytes` |
| histórico | `engine.history_bytes` |
| espaço livre interno + páginas do backend | diferença entre `file_allocated_bytes` e `engine.payload_bytes` |
| binário (geradores/codecs) | só contexto: `env.bench_exe_bytes` (tamanho do executável do benchmark) |
| descrições mantidas pelo cliente | não se aplica aos cenários S1–S6 |

## 5. O que é e o que não é controlado

- **Cache de arquivos do Windows: NÃO controlado.** Logo após o load os dados estão no cache
  do SO (lista *standby*/*modified*). "Processo novo" ou "banco reaberto" ≠ "disco frio":
  as fases de leitura medem caches de processo frios sobre cache do SO quente, salvo se os
  dados excederem a RAM livre. Aproximar disco frio exige ação manual fora do harness
  (reiniciar o computador; ou esvaziar a lista standby com privilégio de administrador,
  p.ex. RAMMap → *Empty Standby List*), e deve ser registrado em `--note`.
- **Durabilidade `Immediate`**: no redb, `Durability::Immediate` chama `File::sync_data`, que
  no Windows é `FlushFileBuffers`. Se o disco honra o flush com cache de escrita volátil não
  foi verificado: `Get-StorageAdvancedProperty` falhou (código 40001) sem elevação, então
  `IsDeviceCacheEnabled`/`IsPowerProtected` estão **indeterminados**.
- **Orçamento de RAM** (§11: volumes 0,25× e 4× do orçamento): `--mem-limit B` coloca o
  processo num *job object* com `JOB_OBJECT_LIMIT_PROCESS_MEMORY`. Isso limita só a carga de
  *commit* (memória privada): **não** limita o cache de arquivos do SO nem as páginas de
  arquivos mapeados (LMDB). Exceder o limite aborta o processo (falha de alocação), o que é
  registrado como execução falha.
- **Espaço em disco no NTFS** (medido por `cargo test --lib sys` + inspeção, cluster 4096 B):
  arquivos de até ~700 B ficam residentes na MFT e relatam alocação arredondada a 8 B
  (1 B → 8, 100 → 104, 700 → 704); a partir de ~800 B, clusters inteiros (800 → 4096,
  4097 → 8192, 10 000 → 12 288); um arquivo que cresceu para fora da MFT mantém o cluster ao
  encolher (5000 B → 300 B continua com 4096 alocados). Os arquivos de banco são grandes;
  isso só afeta arquivos minúsculos (p.ex. `lock.mdb`).
- **LMDB no Windows** (hipótese a verificar quando o backend existir): o tamanho do *map*
  pode ser refletido no tamanho aparente/alocado de `data.mdb`; `--lmdb-map-size` tem padrão
  4 × (estimativa de dados) + 64 MiB e é registrado em `params.lmdb_map_size`.
- **Outras cargas na máquina**: não controladas. Durante o desenvolvimento havia ~12
  compilações paralelas e a RAM disponível no início das execuções variou de 2,5 a 10,5 GB
  (campo `env.ram_available_bytes_at_start`). Para medições, fechar cargas pesadas.
- **CPU**: frequência/turbo não fixados; plano de energia "Alto desempenho". Threads não são
  fixadas em núcleos.
- **Antivírus**: Microsoft Defender com proteção em tempo real desligada
  (`RealTimeProtectionEnabled = False`, `AMRunningMode = Not running`); outros produtos não
  verificados. NTFS: atualização de último acesso "gerenciada pelo sistema, habilitada"
  (`DisableLastAccess = 2`).

## 6. Ambiente (medido em 27/09/2026)

| item | valor | fonte |
|---|---|---|
| CPU | AMD Ryzen 7 5700, 8 núcleos / 16 threads, `MaxClockSpeed` 3701 MHz | `Get-CimInstance Win32_Processor` |
| RAM | 34 139 103 232 B visíveis (31,8 GiB); módulos: 34 359 738 368 B (32 GiB) | `Win32_ComputerSystem`, `Win32_PhysicalMemory` |
| SO | Windows 11 Pro 10.0.26200 (`ver`: 10.0.26200.9457) | `Win32_OperatingSystem`, `cmd /C ver` |
| disco | KINGSTON SNV3S1000G, NVMe, SSD, 1 000 204 886 016 B, firmware ERFK1N.3 (disco 0) | `Get-PhysicalDisk` |
| volume | `C:` NTFS, cluster 4096 B, 999 044 411 392 B, ~85–95 GB livres durante o desenvolvimento | `Get-Volume -DriveLetter C`, `sys::volume_info` |
| cache de escrita do disco | indeterminado (erro 40001 sem elevação) | `Get-StorageAdvancedProperty` |
| energia | "Alto desempenho" | `powercfg /getactivescheme` |
| compilador | rustc 1.97.1 (8bab26f4f 2026-07-14), cargo 1.97.1; perfil `bench` = `release` + `debug = "line-tables-only"` | `rust-toolchain.toml`, `Cargo.toml` |
| dependências | redb 4.3.0, heed 0.22.1 (feature `lmdb`), blake3 1.8.7, zstd 0.14.0 (libzstd 1.5.7), lz4_flex 0.14.0 | `Cargo.lock` |

Cada linha JSON repete o ambiente detectado em tempo de execução (`env`): SO e versão, CPU,
núcleos lógicos, RAM total e disponível no início, sistema de arquivos/cluster/espaço livre
do `--dir`, `rustc --version`, perfil, features, commit git (e se há mudanças locais),
BLAKE3 do `Cargo.lock`, versão da libzstd, tamanho do executável e `--note` (use-o para o
modelo do disco e para qualquer ação manual, como esvaziar o cache).

## 7. Reprodução

```powershell
# testes (datasets, métricas do SO) e autotestes do harness (percentis, JSON,
# argumentos, amostradores, execução de fumaça completa sobre o MemStore)
cargo test --lib datasets
cargo test --lib sys
cargo test --bench engine

cargo bench --bench engine -- --help
cargo bench --bench engine -- --list

# regenera benchdata/manifest.json; os digests devem coincidir com os versionados
cargo bench --bench engine -- gen-manifest

# matriz padrão: 7 variantes x 6 cenários, 20k registros de 1 KiB, redb
cargo bench --bench engine -- --tag rep1 --note "KINGSTON SNV3S1000G NVMe; cache do SO quente"

# repetições independentes (um processo novo por repetição/variante/cenário) e resumo
powershell -ExecutionPolicy Bypass -File scripts/bench-matrix.ps1 -Reps 5
powershell -ExecutionPolicy Bypass -File scripts/summarize.ps1 -Path bench-results/engine.jsonl

# LMDB (quando o backend existir)
cargo bench --bench engine --features lmdb -- --backend lmdb --tag rep1
```

Receitas:

```powershell
# chat estilo Discord: mensagens pequenas, leituras enviesadas, leitores concorrentes, escrita durável
cargo bench --bench engine -- --scenario s3 --records 200k --value-size 512 --dist latest `
  --readers 1,4,8,16 --mix 95-5 --variant raw-backend,engine-raw,adaptive-nodedupe,adaptive

# varredura de tamanho de valor (§11: 64 B ... 64 KiB); repetir para 64,256,1k,4k,16k,64k
cargo bench --bench engine -- --value-size 4k --records 20k

# dedupe visível (S4 acima de inline_max)
cargo bench --bench engine -- --scenario s4 --value-size 4k --variant adaptive-nodedupe,adaptive

# orçamento de RAM 128 MiB (caches 32 + 32 MiB) com dados 0,25x (32 MiB) e 4x (512 MiB), valores de 1 KiB
cargo bench --bench engine -- --mem-limit 128m --cache-bytes 32m --backend-cache-bytes 32m --records 32k  --tag ram-0.25x
cargo bench --bench engine -- --mem-limit 128m --cache-bytes 32m --backend-cache-bytes 32m --records 512k --tag ram-4x
```

Protocolo recomendado: ≥ 5 repetições independentes (o script alterna a ordem das variantes a
cada repetição), reportar mediana e mín..máx; registrar em `--note` o estado do cache do SO;
não comparar execuções de máquinas, commits ou `Cargo.lock` diferentes (todos estão em `env`).

## 8. Saída

- **stdout**: uma linha por fase e uma tabela-resumo por invocação (load MB/s, p99 do put
  durável, p50/p99 de get, p99 de miss/range/latest, kops/s e p99 do mt-get, MB alocados,
  amplificação).
- **JSON lines** (`--out`, padrão `bench-results/engine.jsonl`, sempre em modo *append*;
  `--no-out` desliga): uma linha por (variante, cenário, fase), esquema
  `babeldb-bench-engine/1`. Campos comuns: `run_id`, `ts_unix_ms`, `tag`, `backend`, `variant`,
  `scenario`, `phase`, `params` (todas as opções + `engine_config` efetiva), `env`. Campos por
  fase: `latency_ns{count,min,p50,p95,p99,p999,max,mean,sum}` (ou `batch_latency_ns`,
  `read_latency_ns`/`write_latency_ns`), `ops_per_s`, `wall_seconds`, `bytes_returned`,
  `verified_values`, `process_delta`, `process_after`, `open_ns`/`reopened`,
  `engine_counters`, `read_amplification`, `engine_cache`; no `space`: `files[]`,
  `file_apparent_bytes`, `file_allocated_bytes`, `amplification_*`, `engine{...}` (todo o
  `Stats`, com `per_codec` e `planner`). Execuções que falham (erro ou *panic*) geram uma linha
  `phase = "error"` e o harness segue para a próxima.
- `scripts/summarize.ps1` agrupa por backend/variante/cenário/fase/threads e mostra
  mediana (mín..máx) entre repetições; `-Csv` exporta em formato longo.

## 9. Resultados medidos

Ambiente da §6; 1 repetição; commit `c6a92f5`; backend redb; `--note "KINGSTON SNV3S1000G
NVMe; OS file cache warm; machine otherwise idle"`. Saídas brutas: `bench-results/matrix-4k.*`
e `bench-results/chat-200k.*`.

### 9.1 Espaço — payload do motor (MB), 20 000 registros × 4 KiB (`--value-size 4k --records 20k`)

Bytes do usuário ≈ 82,4–82,7 MB por cenário. "Payload" = chaves + valores gravados pelo motor
(manifestos, envelopes de 64 B, índices, refcounts, meta), antes das páginas do redb.

| variante | S1 repetitivo | S2 sequências | S3 chat JSON | S4 duplicatas | S5 alta entropia | S6 comprimido |
|---|---|---|---|---|---|---|
| engine-raw | 84,92 | 84,90 | 85,12 | 84,92 | 84,96 | 85,22 |
| **babel-pure** | 84,92 | 84,90 | 85,12 | 84,92 | 84,96 | 85,22 |
| lz4 | 3,87 | 59,34 | 61,52 | 84,92 | 84,96 | 85,22 |
| zstd | 3,70 | 31,12 | 40,57 | 84,92 | 84,96 | 85,22 |
| adaptive-nodedupe | 3,42 | 3,46 | 40,57 | 84,92 | 84,96 | 85,22 |
| **adaptive** | 2,93 | 4,34 | 41,45 | **60,43** | 85,84 | 86,10 |

- `BabelPure` ocupa exatamente o mesmo que `engine-raw` em todos os cenários: a hipótese da
  §11 (seed do mesmo tamanho + envelope) se confirmou; nenhuma economia.
- `Adaptive` economiza só onde há estrutura: ~28× (S1), ~24× (S2, receita aritmética sem
  dedupe), ~2× (S3, zstd sem dicionário), ~27 % (S4, dedupe de 30 % de duplicatas); em S5/S6
  o índice de dedupe custa ~1 % a mais.
- **Arquivo em disco**: o redb cresce em regiões de potência de 2 (4,21 → 8,43 → 67,38 →
  134,75 → 269,49 MB); sem `compact()`, o arquivo fica bem acima do payload (ex.: 269,49 MB
  para 85 MB em S5). Tratado como gargalo de espaço a otimizar.

### 9.2 Carga de chat tipo Discord — 200 000 mensagens × 512 B (S3), leituras `--dist latest`

`cargo bench --bench engine -- --scenario s3 --records 200k --value-size 512 --dist latest
--readers 1,4,8,16 --mix 95-5`. Latências por operação; durável = commit `Immediate`.

| variante | carga (rec/s) | put durável p50 / p99 | get p50 / p99 | latest-50 p50 / p99 | mt-get x16 (ops/s) | mixed 95/5 x16: escrita p50 |
|---|---|---|---|---|---|---|
| raw-backend (redb puro) | 20133 | 1,79 ms / 2,63 ms | 2,3 µs / 31,3 µs | 18,9 µs / 70,2 µs | 218948 | 33,48 ms |
| engine-raw | 18963 | 2,30 ms / 3,81 ms | 4,2 µs / 49,2 µs | 56,0 µs / 142,2 µs | 380383 | 29,81 ms |
| babel-pure | 24832 | 1,66 ms / 2,48 ms | 2,6 µs / 26,8 µs | 55,0 µs / 123,0 µs | 327747 | 29,64 ms |
| lz4 | 18840 | 3,10 ms / 16,61 ms | 6,3 µs / 155,5 µs | 87,6 µs / 382,9 µs | 311526 | 33,08 ms |
| zstd | 21981 | 1,73 ms / 2,60 ms | 5,6 µs / 43,1 µs | 160,8 µs / 438,2 µs | 258602 | 39,14 ms |
| adaptive-nodedupe | 14933 | 2,36 ms / 3,73 ms | 5,8 µs / 48,1 µs | 211,6 µs / 627,3 µs | 255014 | 40,55 ms |
| adaptive | 12667 | 2,20 ms / 4,99 ms | 7,1 µs / 81,2 µs | 205,3 µs / 1,10 ms | 311941 | 33,00 ms |

Leitura dos números (1 repetição; diferenças pequenas entre variantes estão dentro do ruído):
- O commit durável custa ~1,7–2,4 ms (p50) — é o `FlushFileBuffers` do disco; domina toda
  escrita isolada.
- O formato do motor acrescenta ~2 µs por `get` sobre o redb puro; zstd sem dicionário
  acrescenta mais ~1–3 µs por valor e pesa no `latest-50` (50 decodificações).
- Com 16 threads fazendo escritas duráveis diretas (sem group commit), cada escrita espera
  ~30 ms: todas disputam o único escritor e cada uma paga seu fsync. É o caso que o
  `GroupCommitter` (`src/scale`) resolve; ver a comparação com PostgreSQL/MongoDB.

### 9.3 Metas da §12

- ≥ 20 % menos bytes que `engine-raw` com ≤ 10 % de regressão de p99: atingida em payload
  para S1, S2, S3 e S4; **não** para S5/S6 (sem estrutura). Em p99 de leitura, o zstd custa
  mais que 10 % — a curva espaço × latência está na tabela 9.2.
- BabelPure ≥ Raw em bytes: confirmada (igual ao `engine-raw` + nada de ganho).

## 10. Limitações conhecidas

- Nesta branch o motor, o `RedbStore` e o `HeedStore` são *stubs*: só a variante
  `raw-backend` sobre `--backend mem` executa hoje (autoteste). O restante do harness compila
  contra os contratos (`Db`, `Store`) e foi verificado apenas em compilação.
- `--features lmdb` só compila depois que o `HeedStore` implementar `Store` (branch do backend
  LMDB); a compilação do caminho LMDB do harness foi verificada com um stub temporário, não
  versionado.
- Os caminhos Linux de `src/sys.rs` (`/proc`, `st_blocks`) não foram compilados aqui (só há o
  alvo Windows instalado); os *parsers* de `/proc` são testados em todas as plataformas.
- Contadores do motor por fase incluem o aquecimento; o pico do working set é o do processo
  inteiro; CPU tem granularidade de tick.
- O amostrador Zipf usa `powf` (reprodutível na mesma plataforma, não bit a bit entre
  plataformas); os datasets não dependem dele.
