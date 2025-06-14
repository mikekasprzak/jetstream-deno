import { createWebSocketStream, WebSocket } from 'npm:ws'

type JetStreamRecordPost = {
  '$type': 'app.bsky.feed.post',
  createdAt: string,
  embed: unknown
  langs: string[],
  text: string,
  reply?: {
    parent: {
      cid: string,
      uri: string,
    },
    root: {
      cid: string,
      uri: string
    }
  }
}

type JetStreamRecordRepost = {
  '$type': 'app.bsky.feed.repost',
  createdAt: string,
  subject: {
    cid: string,
    uri: string
  }
}

type JetStreamCommitCreate = {
  rev: string,
  operation: 'create' | 'update',
  collection: string,
  rkey: string,
  record: JetStreamRecordPost | JetStreamRecordRepost,
  cid: string
}

type JetStreamCommitDelete = {
  rev: string,
  operation: 'delete',
  collection: string,
  rkey: string,
}

type JetStreamElementCommit = {
  did: string,
  time_us: number,
  kind: 'commit',
  commit: JetStreamCommitCreate | JetStreamCommitDelete
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
      const args: string[] =
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
    } catch(nop) {
      nop
    }
  }
}

for await (const element of jetstream({ 
  wantedCollections: ["app.bsky.feed.post" /*, "app.bsky.feed.repost"*/], 
  /*cursor: 1749043869955643*/
})) {
  if (element?.kind === 'commit' && element.commit?.operation === 'create' && element.commit.record?.['$type'] === 'app.bsky.feed.post' && !element.commit.record.reply) {
    console.log(element)
    if (!count--) {
      break
    }
  }
}
