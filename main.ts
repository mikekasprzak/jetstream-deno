import { createWebSocketStream, WebSocket } from 'npm:ws'

type JetStreamRecordPost = {
  '$type': 'app.bsky.feed.post',
  createdAt: string,
  embed: unknown
  langs: string[],
  text: string,
}

type JetStreamRecordRepost = {
  '$type': 'app.bsky.feed.repost',
  createdAt: string,
  subject: {
    cid: string,
    uri: string
  }
}

type JetStreamRecordGeneric = {
  '$type': string,
  createdAt: string,
}

type JetStreamRecord = JetStreamRecordPost | JetStreamRecordRepost | JetStreamRecordGeneric

type JetStreamCommitCreate = {
  rev: string,
  operation: 'create',
  collection: string,
  rkey: string,
  record: JetStreamRecord,
  cid: string
}

type JetStreamCommitUpdate = {
  rev: string,
  operation: 'update',
  collection: string,
  rkey: string,
  record: JetStreamRecord,
  cid: string
}

type JetStreamCommitDelete = {
  rev: string,
  operation: 'delete',
  collection: string,
  rkey: string,
}

type JetStreamCommit = JetStreamCommitCreate | JetStreamCommitDelete | JetStreamCommitUpdate

type JetStreamElementCommit = {
  did: string,
  time_us: number,
  kind: 'commit',
  commit: JetStreamCommit
}

type JetStreamElementIdentity = {
  did: string,
  time_us: number,
  kind: 'identity',
  identity: {
    did: string,
    handle: string,
    seq: number,
    time: string
  }
}

type JetStreamElementAccount = {
  did: string,
  time_us: number,
  kind: 'account',
  account: {
    active: boolean,
    did: string,
    seq: number,
    time: string
  }
}

type JetStreamElement = JetStreamElementCommit | JetStreamElementIdentity | JetStreamElementAccount

const endpoint = `wss://jetstream1.us-east.bsky.network/subscribe`
let count = 10

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
  //if (element.kind === 'account' /*&& element.commit.operation === 'update'*/) {
    console.log(element)
  //}
  if (!count--) {
    break
  }
}
