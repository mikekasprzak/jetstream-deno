import { createWebSocketStream, WebSocket } from 'npm:ws'

//console.log("test")

type JetStreamElementCommitCreate = {
  did: string,
  time_us: number,
  kind: 'commit',
  commit: {
    rev: string,
    operation: 'create',
    collection: string,
    rkey: string,
    record: unknown,
    cid: string
  }
}

type JetStreamElementCommitUpdate = {
  did: string,
  time_us: number,
  kind: 'commit',
  commit: {
    rev: string,
    operation:  'update',
    collection: string,
    rkey: string,
    record: unknown,
    cid: string
  }
}

type JetStreamElementCommitDelete = {
  did: string,
  time_us: number,
  kind: 'commit',
  commit: {
    rev: string,
    operation: 'delete',
    collection: string,
    rkey: string,
    record: unknown,
    cid: string
  }
}

type JetStreamElementCommit = JetStreamElementCommitCreate | JetStreamElementCommitUpdate | JetStreamElementCommitDelete


type JetStreamElementIdentity = {
  did: string,
  time_us: number,
  kind: 'identity',
  identity: {
  }
}

type JetStreamElementAccount = {
  did: string,
  time_us: number,
  kind: 'account',
  account: {
  }
}

type JetStreamElement = JetStreamElementCommit | JetStreamElementIdentity | JetStreamElementAccount

const endpoint = `wss://jetstream1.us-east.bsky.network/subscribe`
let count = 4

export async function* jetstream(options?: {wantedCollections?: string[], cursor?: number}) {
  let cursorPos: number | undefined = options?.cursor

  while (true) {
    try {
      let args: string[] = 
        options && options.wantedCollections ? 
        options.wantedCollections.map((e) => `wantedCollections=${e}`) : 
        []
      if (cursorPos) {
        args.push(`cursor=${cursorPos}`)
      }
      const newEndPoint = endpoint + (args.length ? `?${args.join('&')}` : '')
      console.log(newEndPoint)

      const ws = new WebSocket(newEndPoint)
      const stream = createWebSocketStream(ws, { readableObjectMode: true })

      for await (const bytes of stream) {
        const data = JSON.parse(bytes)
        cursorPos = data.time_us+1
        yield data as JetStreamElement
      }
    } catch(e) {
    }
  }
}

for await (const element of jetstream({ 
  wantedCollections: ["app.bsky.feed.post", "app.bsky.feed.repost"], 
  cursor: 1749043869955643
})) {
  console.log(element)
  if (!count--){
    break
  }
}
