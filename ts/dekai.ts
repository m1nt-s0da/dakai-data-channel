import { EventEmitter } from "./events";
import { DekaiDataChannelMessaging, type Mode } from "./messaging";
import { createChunkId, deferred, sha256Hex, uuid7 } from "./utils";

const CHUNK_ID_MASK = 0x000f_ffff_ffff_ffffn;

type StartReceivingHandler = [receiving: TransferReceive];
type ReceivedHandler = [data: string | Uint8Array];
type ProgressHandler = [receivedBytes: number, totalBytes: number];

type ReceiverEvents = {
  progress: ProgressHandler;
};

type TransferEvents = {
  received: ReceivedHandler;
};

type DataChannelEvents = {
  start_receiving: StartReceivingHandler;
  received: ReceivedHandler;
};

type RequestedChunk = {
  sessionId: string;
  receive: TransferReceive;
  offset: number;
  timeoutId: number;
};

type SendPhase = "awaiting_request_chunk" | "awaiting_final_response";

type SendingSession = {
  payload: Uint8Array;
  response: Promise<Record<string, unknown> | null>;
  reject: (reason?: unknown) => void;
  phase: SendPhase;
  timeoutId: number | null;
};

class AsyncQueue<T> {
  private values: T[] = [];
  private waiters: Array<(value: T) => void> = [];

  push(value: T): void {
    const waiter = this.waiters.shift();
    if (waiter !== undefined) {
      waiter(value);
      return;
    }
    this.values.push(value);
  }

  shift(): Promise<T> {
    const value = this.values.shift();
    if (value !== undefined) {
      return Promise.resolve(value);
    }
    return new Promise<T>((resolve) => {
      this.waiters.push(resolve);
    });
  }
}

export class ReceiverBase extends EventEmitter<ReceiverEvents> {
  constructor(private readonly transfer: TransferReceive) {
    super();
  }

  get receive(): TransferReceive {
    return this.transfer;
  }

  notifyProgress(receivedBytes: number, totalBytes: number): void {
    this.emit("progress", receivedBytes, totalBytes);
  }
}

export class StreamReceiver extends ReceiverBase implements AsyncIterable<Uint8Array> {
  [Symbol.asyncIterator](): AsyncIterableIterator<Uint8Array> {
    return this;
  }

  async next(): Promise<IteratorResult<Uint8Array>> {
    await this.receive.requestNextChunk();
    try {
      return { value: await this.receive.nextChunk(), done: false };
    } catch (error) {
      if (error instanceof StopAsyncIterationError) {
        return { value: undefined as never, done: true };
      }
      throw error;
    }
  }
}

export class BufferedReceiver extends ReceiverBase {
  private readonly streamReceiver: StreamReceiver;

  constructor(receive: TransferReceive) {
    super(receive);
    this.streamReceiver = new StreamReceiver(receive);
  }

  async join(): Promise<Uint8Array> {
    const parts: Uint8Array[] = [];
    for await (const chunk of this.streamReceiver) {
      parts.push(chunk);
    }

    const size = parts.reduce((sum, chunk) => sum + chunk.byteLength, 0);
    const output = new Uint8Array(size);
    let offset = 0;
    for (const chunk of parts) {
      output.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return output;
  }
}

class StopAsyncIterationError extends Error {}

export class TransferReceive extends EventEmitter<TransferEvents> {
  readonly byteLength: number;
  readonly hash: string;
  readonly mode: Mode;

  private receiver: BufferedReceiver | StreamReceiver | null = null;
  private readonly queue = new AsyncQueue<Uint8Array | null | Error>();
  private readonly bufferedData: Uint8Array[] = [];
  private nextRequestOffset = 0;
  private receivedByteLength = 0;
  private done = false;

  constructor(
    byteLength: number,
    hash: string,
    mode: Mode,
    private readonly frameBodySize: number,
    private readonly requestChunkHandler: (byteOffset: number, byteLength: number) => Promise<void>,
    private readonly complete: () => void,
    private readonly failHandler: (error: Error) => void,
  ) {
    super();
    this.byteLength = byteLength;
    this.hash = hash;
    this.mode = mode;
  }

  get hasReceiver(): boolean {
    return this.receiver !== null;
  }

  stream(): StreamReceiver {
    if (this.receiver === null) {
      this.receiver = new StreamReceiver(this);
    } else if (!(this.receiver instanceof StreamReceiver)) {
      throw new Error("Cannot switch to stream mode after buffering has started.");
    }
    return this.receiver;
  }

  buffered(): BufferedReceiver {
    if (this.receiver === null) {
      this.receiver = new BufferedReceiver(this);
    } else if (!(this.receiver instanceof BufferedReceiver)) {
      throw new Error("Cannot switch to buffered mode after streaming has started.");
    }
    return this.receiver;
  }

  async requestNextChunk(): Promise<void> {
    if (this.done || this.nextRequestOffset >= this.byteLength) {
      return;
    }
    const byteOffset = this.nextRequestOffset;
    const byteLength = Math.min(this.frameBodySize, this.byteLength - byteOffset);
    this.nextRequestOffset += byteLength;
    await this.requestChunkHandler(byteOffset, byteLength);
  }

  async nextChunk(): Promise<Uint8Array> {
    const chunk = await this.queue.shift();
    if (chunk instanceof Error) {
      throw chunk;
    }
    if (chunk === null) {
      throw new StopAsyncIterationError();
    }
    return chunk;
  }

  pushChunk(offset: number, chunk: Uint8Array): void {
    if (this.done) {
      throw new Error("Transfer has already completed.");
    }
    if (offset !== this.receivedByteLength) {
      throw new Error("Chunk offset does not match the expected receive position.");
    }
    if (offset + chunk.byteLength > this.byteLength) {
      throw new Error("Chunk exceeds the announced transfer size.");
    }

    this.receivedByteLength += chunk.byteLength;
    this.bufferedData.push(chunk.slice());
    this.receiver?.notifyProgress(this.receivedByteLength, this.byteLength);
    this.queue.push(chunk.slice());
    void this.finalizeIfReady();
  }

  fail(error: Error): void {
    if (this.done) {
      return;
    }
    this.done = true;
    this.queue.push(error);
    this.failHandler(error);
  }

  private async finalizeIfReady(): Promise<void> {
    if (this.done || this.receivedByteLength !== this.byteLength) {
      return;
    }

    const data = new Uint8Array(this.byteLength);
    let offset = 0;
    for (const chunk of this.bufferedData) {
      data.set(chunk, offset);
      offset += chunk.byteLength;
    }

    const digest = await sha256Hex(data);
    if (digest !== this.hash) {
      this.fail(new Error("Received data hash does not match the announced digest."));
      return;
    }

    this.done = true;
    this.queue.push(null);
    this.emit("received", this.mode === "text" ? new TextDecoder().decode(data) : data);
    this.complete();
  }
}

export class DekaiDataChannel extends EventEmitter<DataChannelEvents> {
  private readonly messaging: DekaiDataChannelMessaging;
  private readonly frameBodySize: number;
  private readonly receiving = new Map<string, { receive: TransferReceive; resolve: () => void; reject: (reason?: unknown) => void }>();
  private readonly sending = new Map<string, SendingSession>();
  private readonly chunkIds = new Map<bigint, RequestedChunk>();
  private readonly textEncoder = new TextEncoder();

  constructor(
    readonly channel: RTCDataChannel,
    chunkSize = 16_384,
    private readonly timeoutSeconds = 30,
    private readonly finalResponseTimeoutSeconds = Math.max(timeoutSeconds * 3, timeoutSeconds),
  ) {
    super();
    this.frameBodySize = chunkSize - 8;
    this.messaging = new DekaiDataChannelMessaging(channel);

    this.messaging.on("start_session", (sessionId, byteLength, hash, mode) => this.onStartSession(sessionId, byteLength, hash, mode));
    this.messaging.on("request_chunk", (sessionId, chunkId, byteOffset, byteLength) => this.onRequestChunk(sessionId, chunkId, byteOffset, byteLength));
    this.messaging.on("chunk_content", (chunkId, data) => this.onChunkContent(chunkId, data));
    this.messaging.on("timeout", (sessionId) => this.onTimeout(sessionId));
  }

  private traceSession(
    event: string,
    details: {
      sessionId: string;
      chunkId?: bigint;
      byteOffset?: number;
      byteLength?: number;
      phase?: SendPhase;
    },
  ): void {
    console.debug("Dekai trace", {
      event,
      sessionId: details.sessionId,
      chunkId: details.chunkId?.toString(),
      byteOffset: details.byteOffset,
      byteLength: details.byteLength,
      phase: details.phase,
    });
  }

  private dropRequestedChunk(chunkId: bigint, reason: string): RequestedChunk | undefined {
    const chunk = this.chunkIds.get(chunkId);
    if (chunk === undefined) {
      console.debug("Dekai trace", { event: "chunk_id.missing", chunkId: chunkId.toString(), reason });
      return undefined;
    }

    this.chunkIds.delete(chunkId);
    this.traceSession("chunk_id.drop", {
      sessionId: chunk.sessionId,
      chunkId,
      byteOffset: chunk.offset,
    });
    console.debug("Dekai trace", { chunkId: chunkId.toString(), reason });
    return chunk;
  }

  private sendTimeoutMs(phase: SendPhase): number {
    return (phase === "awaiting_request_chunk" ? this.timeoutSeconds : this.finalResponseTimeoutSeconds) * 1000;
  }

  private armSendTimeout(sessionId: string, phase: SendPhase): void {
    const session = this.sending.get(sessionId);
    if (session === undefined) {
      return;
    }

    if (session.timeoutId !== null) {
      window.clearTimeout(session.timeoutId);
    }
    session.phase = phase;
    session.timeoutId = window.setTimeout(() => {
      this.traceSession("timeout.send", { sessionId, phase });
      this.messaging.notify("timeout", { session_id: sessionId });
      session.reject(new Error(`Send session timed out (${phase}): ${sessionId}`));
    }, this.sendTimeoutMs(phase));
  }

  async send(data: string | Uint8Array, mode: Mode): Promise<void> {
    const payload = typeof data === "string" ? this.textEncoder.encode(data) : data.slice();
    const sessionId = uuid7();
    const digest = await sha256Hex(payload);
    const gate = deferred<Record<string, unknown> | null>();

    this.sending.set(sessionId, {
      payload,
      response: gate.promise,
      reject: gate.reject,
      phase: "awaiting_request_chunk",
      timeoutId: null,
    });
    this.traceSession("start_session.send", { sessionId, byteLength: payload.byteLength });
    const rpcPromise = this.messaging.call("start_session", {
      session_id: sessionId,
      byte_length: payload.byteLength,
      sha256: digest,
      mode,
    });
    this.armSendTimeout(sessionId, "awaiting_request_chunk");

    rpcPromise.then(gate.resolve, gate.reject);

    try {
      await gate.promise;
    } finally {
      const session = this.sending.get(sessionId);
      if (session !== undefined && session.timeoutId !== null) {
        window.clearTimeout(session.timeoutId);
      }
      this.sending.delete(sessionId);
    }
  }

  private async onStartSession(sessionId: string, byteLength: number, hash: string, mode: Mode): Promise<void> {
    this.traceSession("start_session.recv", { sessionId, byteLength });
    const gate = deferred<void>();
    let receive!: TransferReceive;
    receive = new TransferReceive(
      byteLength,
      hash,
      mode,
      this.frameBodySize,
      (byteOffset, byteLengthValue) => this.requestChunk(sessionId, receive, byteOffset, byteLengthValue),
      gate.resolve,
      gate.reject as (error: Error) => void,
    );
    receive.on("received", (data: string | Uint8Array) => this.emit("received", data));
    this.receiving.set(sessionId, { receive, resolve: gate.resolve, reject: gate.reject });

    this.emit("start_receiving", receive);
    queueMicrotask(() => {
      if (!receive.hasReceiver) {
        void receive.buffered().join().catch((error) => {
          console.error("Default buffered receiver failed", { sessionId, error });
        });
      }
    });

    try {
      await gate.promise;
    } finally {
      this.cancelRequestedChunks(sessionId);
      this.receiving.delete(sessionId);
    }
  }

  private async requestChunk(sessionId: string, receive: TransferReceive, byteOffset: number, byteLength: number): Promise<void> {
    let chunkId = createChunkId();
    while (this.chunkIds.has(chunkId)) {
      chunkId = (chunkId + 1n) & CHUNK_ID_MASK;
    }

    const timeoutId = window.setTimeout(() => {
      this.dropRequestedChunk(chunkId, "chunk_content_timeout");
      this.failReceiveSession(sessionId, new Error(`Timed out waiting for chunk content: ${chunkId.toString()}`));
    }, this.timeoutSeconds * 1000);

    this.chunkIds.set(chunkId, { sessionId, receive, offset: byteOffset, timeoutId });
    this.traceSession("request_chunk.send", { sessionId, chunkId, byteOffset, byteLength });
    this.messaging.notify("request_chunk", {
      session_id: sessionId,
      chunk_id: chunkId,
      byte_offset: byteOffset,
      byte_length: byteLength,
    });
  }

  private async onRequestChunk(sessionId: string, chunkId: bigint, byteOffset: number, byteLength: number): Promise<void> {
    const session = this.sending.get(sessionId);
    if (session === undefined) {
      throw new Error(`Unknown session: ${sessionId}`);
    }
    this.traceSession("request_chunk.recv", { sessionId, chunkId, byteOffset, byteLength, phase: session.phase });

    if (byteOffset < 0 || byteLength < 0) {
      throw new Error("Chunk offset and length must be non-negative.");
    }
    if (byteOffset + byteLength > session.payload.byteLength) {
      throw new Error("Requested chunk range exceeds the transfer size.");
    }

    const nextPhase: SendPhase = byteOffset + byteLength === session.payload.byteLength
      ? "awaiting_final_response"
      : "awaiting_request_chunk";
    this.messaging.sendChunk(chunkId, session.payload.slice(byteOffset, byteOffset + byteLength));
    this.traceSession("chunk_content.send", { sessionId, chunkId, byteOffset, byteLength, phase: nextPhase });
    this.armSendTimeout(sessionId, nextPhase);
  }

  private onChunkContent(chunkId: bigint, data: Uint8Array): void {
    const requestedChunk = this.dropRequestedChunk(chunkId, "chunk_content_received");
    if (requestedChunk === undefined) {
      console.debug("Dekai trace", { event: "chunk_content.missing", chunkId: chunkId.toString() });
      return;
    }

    window.clearTimeout(requestedChunk.timeoutId);
    this.traceSession("chunk_content.recv", {
      sessionId: requestedChunk.sessionId,
      chunkId,
      byteOffset: requestedChunk.offset,
      byteLength: data.byteLength,
    });
    try {
      requestedChunk.receive.pushChunk(requestedChunk.offset, data);
    } catch (error) {
      console.error("Failed to process received chunk", {
        sessionId: requestedChunk.sessionId,
        chunkId: chunkId.toString(),
        error,
      });
      this.failReceiveSession(requestedChunk.sessionId, normalizeError(error));
    }
  }

  private onTimeout(sessionId: string): void {
    this.traceSession("timeout.recv", { sessionId });
    this.failReceiveSession(sessionId, new Error(`Sender abandoned transfer due to timeout: ${sessionId}`));
  }

  private failReceiveSession(sessionId: string, error: Error): void {
    const session = this.receiving.get(sessionId);
    if (session === undefined) {
      return;
    }

    this.cancelRequestedChunks(sessionId);
    session.receive.fail(error);
  }

  private cancelRequestedChunks(sessionId: string): void {
    for (const [chunkId, requestedChunk] of this.chunkIds.entries()) {
      if (requestedChunk.sessionId !== sessionId) {
        continue;
      }
      window.clearTimeout(requestedChunk.timeoutId);
      this.dropRequestedChunk(chunkId, "receive_session_cancelled");
    }
  }
}

function normalizeError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

export type { Mode };