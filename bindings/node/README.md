# babeldb para Node.js

Binding nativo e embutido do [babeldb](../../README.md) para Node.js, no
estilo de better-sqlite3, lmdb-js e lancedb: o banco roda dentro do seu
processo, sem servidor. É feito com [napi-rs](https://napi.rs) **v3** e usa
a receita de embutir do babeldb (seção "Usar em outro projeto (Rust)" do
README principal): fjall (LSM-tree) + write-ahead log, com as escritas
passando por um *group committer*.

- Chaves e valores aceitam `Buffer`, `Uint8Array` ou `string` (UTF-8); valores
  voltam sempre como `Buffer`.
- Cada método assíncrono roda no threadpool do libuv (`AsyncTask`): o event
  loop nunca bloqueia no banco. Cada um tem uma versão `*Sync` para scripts.
- Escritas concorrentes compartilham o mesmo commit (uma escrita no WAL para
  todas).
- Tipos TypeScript completos em `index.d.ts`.

## Instalação

### A partir do tarball

```sh
npm install ./babeldb-0.1.0.tgz   # o tarball gerado por npm pack (veja abaixo); depois da publicação: npm install babeldb
```

O tarball já traz o binário compilado (`babeldb.win32-x64-msvc.node`), então
não precisa de Rust para instalar.

### Compilando

Requisitos: Rust (a versão do `rust-toolchain.toml` da raiz do repositório,
1.97), Node.js >= 18 e, no Windows, as Build Tools do Visual Studio (MSVC).

```sh
cd bindings/node
npm install          # instala o @napi-rs/cli (devDependency)
npm run build        # napi build --platform --release
npm pack             # gera babeldb-0.1.0.tgz
```

`npm run build` gera `babeldb.<plataforma>.node` (o addon), `index.js` (o
loader) e `index.d.ts` (os tipos, gerados a partir do Rust com o cabeçalho
`dts-header.d.ts`).

## Exemplo rápido

JavaScript (CommonJS):

```js
const { open, messageKey, channelPrefix, parseMessageKey } = require('babeldb')

async function main() {
  const db = open('./dados/meu-banco') // cria o diretório se não existir
  try {
    const rev = await db.put('usuario:1', JSON.stringify({ nome: 'Ana' }))
    console.log('revisão', rev) // bigint

    const valor = await db.get('usuario:1') // Buffer | null
    console.log(JSON.parse(valor.toString()))

    // Compare-and-set: só grava se ninguém mudou a chave desde `rev`.
    await db.put('usuario:1', JSON.stringify({ nome: 'Ana Maria' }), { ifRevision: rev })

    // Escritas concorrentes compartilham commits.
    await Promise.all([1, 2, 3].map((i) => db.put(`usuario:${i + 1}`, `{"id":${i + 1}}`)))

    for (const [chave, v] of await db.scan({ prefix: 'usuario:', limit: 10 })) {
      console.log(chave.toString(), v.toString())
    }

    // Lote atômico: tudo ou nada, num único commit durável.
    await db.batch([
      { type: 'put', key: 'contador', value: '1' },
      { type: 'del', key: 'usuario:4' },
    ])

    // Chaves de chat (canal, id): as 5 mensagens mais novas do canal 42.
    await db.put(messageKey(42n, 1001n), 'oi')
    const recentes = await db.scan({ prefix: channelPrefix(42n), reverse: true, limit: 5 })
    console.log(recentes.map(([k]) => parseMessageKey(k)))
  } finally {
    db.close()
  }
}

main()
```

TypeScript (ESM):

```ts
import { open, type BabelDbError, type Database } from 'babeldb'

const db: Database = open('./dados/sessoes', { durability: 'buffered' })

try {
  await db.put('sessao:abc', 'dados', { ifAbsent: true })
} catch (e) {
  const err = e as BabelDbError
  if (err.code === 'CONFLICT') console.log('a sessão já existe')
  else throw err
}

const chaves: Buffer[] = await db.keys({ start: 'sessao:', end: 'sessao;' })
console.log(chaves.map(String))
await db.sync() // força a durabilidade das escritas aceitas até aqui
db.close()
```

## Referência da API

Tipos usados abaixo: `Key` e `Value` são `Buffer | Uint8Array | string`;
revisões são `bigint` (onde a API recebe uma revisão, também aceita um
`number` inteiro seguro).

### `open(path, options?) → Database`

Abre (ou cria) o banco no diretório `path`. O WAL fica no arquivo
`<path>.wal`, ao lado do diretório.

- `options.durability`: `'immediate'` (padrão) ou `'buffered'` (veja
  [Modos de durabilidade](#modos-de-durabilidade)).
- Só um handle por diretório por processo: abrir de novo um diretório aberto
  lança na hora um erro com `code === 'ALREADY_OPEN'` (o mesmo vale para um
  diretório aberto por outro processo).

### `Database`

| Método | Resultado | Descrição |
| --- | --- | --- |
| `put(key, value, { ifAbsent?, ifRevision? }?)` | `Promise<bigint>` | Grava e devolve a nova revisão. `ifAbsent: true` só grava se a chave não existe; `ifRevision` só se a revisão atual é exatamente essa. Expectativa falha: rejeita com `CONFLICT` e nada é gravado. `ifAbsent` junto com `ifRevision`: `INVALID_ARGUMENT`. |
| `get(key)` | `Promise<Buffer \| null>` | Valor atual, ou `null` se a chave não existe. |
| `delete(key, { ifRevision? }?)` | `Promise<boolean>` | `true` se apagou um registro, `false` se a chave não existia. Com `ifRevision`, só apaga se a revisão atual é essa (chave inexistente também dá `CONFLICT`). |
| `scan({ prefix?, start?, end?, reverse?, limit? }?)` | `Promise<Array<[Buffer, Buffer]>>` | Pares `[chave, valor]` em ordem de bytes (decrescente com `reverse`). `start` é inclusivo e `end` exclusivo; `prefix` não combina com `start`/`end` (`INVALID_ARGUMENT`). `limit`: no máximo N registros (com `reverse`, os últimos); `0` ou ausente = sem limite; negativo = `INVALID_ARGUMENT`. Lê um snapshot consistente. |
| `keys({ ...mesmas opções })` | `Promise<Buffer[]>` | Só as chaves (os valores não são lidos). |
| `batch(ops)` | `Promise<Array<bigint \| null>>` | Aplica `{ type: 'put', key, value }` e `{ type: 'del', key }` em ordem, de forma atômica, num único commit durável (veja abaixo). |
| `sync()` | `Promise<void>` | Resolve quando toda escrita aceita antes da chamada está durável. |
| `close()` | `void` | Fecha o banco (veja abaixo). |
| `putSync`, `getSync`, `deleteSync`, `scanSync`, `keysSync`, `batchSync` | como acima, sem `Promise` | Mesma operação na thread que chama (bloqueia o event loop: use em scripts). |
| `closed` | `boolean` | `true` depois de `close()`. |
| `path` | `string` | O diretório do banco (absoluto, canônico). |
| `durability` | `'immediate' \| 'buffered'` | O modo com que o banco foi aberto. |

**`batch`** usa `Db::write_batch` do babeldb: tudo ou nada, em ordem (uma
operação vê o efeito das anteriores na mesma chave), durável quando a promise
resolve, nos dois modos de durabilidade. Se qualquer operação for inválida ou
malformada (chave vazia, chave ou valor acima dos limites, `type`
desconhecido...), nada é gravado e a promise rejeita com `INVALID_ARGUMENT`.
O resultado tem uma entrada por operação: para um `put`, a nova revisão; para
um `del`, a revisão do registro apagado, ou `null` se a chave não existia. O
lote é um commit próprio: não entra no group commit de `put`/`delete` (e
também torna duráveis as escritas `buffered` anteriores).

**`close()`** espera as chamadas já feitas terminarem (bloqueando a thread),
esvazia o group committer (escritas `buffered` ficam duráveis), fecha o banco
e libera o diretório, que pode ser reaberto logo em seguida no mesmo
processo. Depois disso todo método lança (sync) ou rejeita (async) com
`code === 'CLOSED'`. Chamar `close()` de novo não faz nada. Um handle coletado
pelo GC sem `close()` é fechado do mesmo jeito.

### Chaves de chat

- `messageKey(channel, id) → Buffer`: 16 bytes, `channel` (u64 big-endian)
  seguido de `id` (u64 big-endian). As chaves ordenam por canal e depois por id.
- `parseMessageKey(key) → [bigint, bigint] | null`: o inverso (`null` se a
  chave não tem 16 bytes).
- `channelPrefix(channel) → Buffer`: os 8 bytes comuns às chaves do canal; as
  N mensagens mais novas são `scan({ prefix: channelPrefix(c), reverse: true, limit: N })`.

`channel` e `id` aceitam `bigint` ou `number` (inteiro seguro), de 0 a 2^64 - 1.

### Erros

Todo erro é um `Error` com um `code` estável:

| `code` | Quando |
| --- | --- |
| `CONFLICT` | Uma expectativa (`ifAbsent` / `ifRevision`) falhou; nada foi gravado. |
| `CLOSED` | O banco está fechado. |
| `ALREADY_OPEN` | O diretório já está aberto (neste processo ou em outro). |
| `INVALID_ARGUMENT` | Argumento malformado ou recusado pelo banco: tipo errado, chave vazia ou com mais de 4096 bytes, valor grande demais, `ifAbsent` com `ifRevision`, `prefix` com `start`/`end`, `limit` negativo, operação de lote malformada, durabilidade desconhecida. |
| `IO`, `BACKEND`, `FORMAT`, `INTEGRITY`, `UNKNOWN_CODEC`, `UNKNOWN_GENERATOR`, `MISSING_DEPENDENCY`, `ID_EXHAUSTED`, `UNSUPPORTED`, `UNKNOWN` | Os outros erros do babeldb, com o nome da variante de `babeldb::Error`. |
| `INTERNAL` | Um bug do binding. |

## Modos de durabilidade

- **`'immediate'`** (padrão): a promise de `put`/`delete` resolve quando a
  escrita está durável (commit com escrita *write-through* no WAL). Chamadas
  concorrentes são agrupadas: enquanto um commit roda, as escritas que chegam
  entram todas no próximo, que paga uma escrita no WAL só.
- **`'buffered'`**: a promise resolve quando a escrita está commitada e
  visível, mas ainda não durável. Ela fica durável em até ~100 ms (ou antes de
  64 MiB de escritas pendentes), em `sync()`, em `close()` e quando o processo
  termina normalmente (inclusive via `process.exit()`): o binding fecha os
  bancos ainda abertos nesse momento. Uma queda ou um `kill` podem perder as
  escritas aceitas nos últimos ~100 ms.

Nos dois modos `batch()` é durável quando resolve, e a leitura vê as próprias
escritas assim que a promise delas resolve.

## Concorrência

- **Um banco por diretório por processo.** Compartilhe o `Database` entre as
  partes do programa; uma segunda chamada a `open()` no mesmo diretório lança
  `ALREADY_OPEN` na hora (não trava). Isso vale também entre worker threads,
  que compartilham o processo.
- **Chamadas concorrentes são seguras**: dezenas ou milhares de promises em
  paralelo (por exemplo com `Promise.all`) funcionam, e as escritas delas são
  agrupadas em poucos commits.
- **Ordem**: chamadas que você não espera (`await`) não têm ordem definida
  entre si. Espere uma escrita antes de uma operação que precisa enxergá-la.
- As threads do libuv só enfileiram `put`/`delete` no committer e voltam; elas
  não ficam esperando o commit, então o pool (4 threads por padrão) não limita
  quantas escritas entram em cada commit.
- Os métodos `*Sync` bloqueiam a thread que chama; evite-os em servidores.

## Limites

- **Só Windows x64 foi compilado e testado até agora.** Ainda não há
  binários para Linux nem macOS (o código não tem nada específico de Windows,
  mas esses alvos não foram compilados nem testados).
- Chaves de 1 a 4096 bytes; valores inteiros em memória (não há streaming).
- `scan`/`keys` devolvem arrays: use `limit` e `start` para paginar
  intervalos grandes.
- Sem iteradores, transações interativas ou snapshots explícitos; `batch` é a
  unidade atômica.
- O WAL é o arquivo `<path>.wal` ao lado do diretório: copie ou apague os dois
  juntos.

## Testes e benchmark

```sh
cd bindings/node
npm run build
npm test            # node --test __test__/babeldb.test.mjs
node bench.mjs      # puts/s e gets/s com valores de 512 B
```

Números medidos com `node bench.mjs` (valores de 512 B, 2 s por fase;
Windows 11, Node 24.15.0, processo em prioridade abaixo do normal). São desta
máquina e deste disco, não uma promessa:

| Modo | Operação | 1 chamador | 8 concorrentes | 64 concorrentes |
| --- | --- | ---: | ---: | ---: |
| `immediate` | `put` | 6.572/s | 31.652/s | 86.359/s |
| `immediate` | `get` | 26.673/s | 100.890/s | 103.520/s |
| `buffered` | `put` | 14.109/s | 78.276/s | 101.880/s |
| `buffered` | `get` | 26.939/s | 99.264/s | 105.322/s |

Síncronos, 1 chamador: `putSync` 6.906/s (`immediate`) e 18.777/s
(`buffered`); `getSync` 192.803/s e 175.936/s. Um `get` assíncrono sozinho
paga a ida e volta pelo threadpool do libuv (~37 µs); com muitas chamadas em
paralelo, ~100 mil operações assíncronas por segundo é o custo fixo de cada
`AsyncTask` no event loop. Um `put` `immediate` sozinho custa o commit no WAL.

Os testes cobrem ida e volta com string, `Buffer` e `Uint8Array`, valores de
1 MiB, CAS, delete, scan (prefixo, reverse, limit, intervalo), keys,
atomicidade do batch, persistência ao reabrir, semântica do `close`, 1000
`put` concorrentes com `Promise.all`, os helpers de chat, a durabilidade de
escritas `buffered` quando o processo termina sem `close()` e o fechamento de
um handle coletado pelo GC.
