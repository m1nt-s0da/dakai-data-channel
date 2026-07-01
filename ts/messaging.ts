import { EventEmitter } from "./events";
import { concatBytes, createChunkId, frameChunkId, normalizeMessageData, parseChunkId, uuid7 } from "./utils";

type Mode = "text" | "binary";

type JsonRpcRequest = {
  id: string | number | null;
  method: string;
  params: Record<string, unknown>;
  jsonrpc: "2.0";
};

type JsonRpcResponse = {
  id: string | number;
  result: Record<string, unknown> | null;
  jsonrpc: "2.0";
};

type JsonRpcError = {
  id: string | number;
  error: {
    code: number;
    message: string;
    data?: Record<string, unknown> | null;
  };
  jsonrpc: "2.0";
};

type MessageHandlers = {
  start_session: [sessionId: string, byteLength: number, sha256: string, mode: Mode];
  request_chunk: [sessionId: string, chunkId: bigint, byteOffset: number, byteLength: number];
  chunk_content: [chunkId: bigint, data: Uint8Array];
  timeout: [sessionId: string];
};

type MessageHandler = (...args: any[]) => Promise<unknown> | unknown;

export class DekaiDataChannelMessaging extends EventEmitter<MessageHandlers> {
  private readonly channel: RTCDataChannel;
  private readonly futures = new Map<
    string | number,
    {
      resolve: (
        value:
          | Record<string, unknown>
          | PromiseLike<Record<string, unknown> | null>
          | null,
      ) => void;
      reject: (reason?: unknown) => void;
    }
  >();
  private readonly textDecoder = new TextDecoder();

  constructor(channel: RTCDataChannel) {
    super();
    this.channel = channel;
    this.channel.addEventListener("message", (event) => {
      void this.onMessage(event.data as string | Blob | ArrayBuffer | ArrayBufferView);
    });
  }

  call(method: "start_session", params: { session_id: string; byte_length: number; sha256: string; mode: Mode }): Promise<Record<string, unknown> | null>;
  call(method: "request_chunk", params: { session_id: string; chunk_id: bigint; byte_offset: number; byte_length: number }): Promise<Record<string, unknown> | null>;
  call(method: string, params: Record<string, unknown>): Promise<Record<string, unknown> | null> {
    const requestId = uuid7();
    const payload = this.encodeJson({
      id: requestId,
      method,
      params: this.serializeParams(params),
      jsonrpc: "2.0",
    });

    const promise = new Promise<Record<string, unknown> | null>((resolve, reject) => {
      this.futures.set(requestId, { resolve, reject });
    });
    this.channel.send(payload);
    return promise;
  }

  notify(method: "timeout", params: { session_id: string }): void;
  notify(method: "request_chunk", params: { session_id: string; chunk_id: bigint; byte_offset: number; byte_length: number }): void;
  notify(method: string, params: Record<string, unknown>): void {
    const payload = this.encodeJson({
      id: null,
      method,
      params: this.serializeParams(params),
      jsonrpc: "2.0",
    });
    this.channel.send(payload);
  }

  sendChunk(chunkId: bigint, data: Uint8Array): void {
    const message = concatBytes([frameChunkId(chunkId), data]);
    const normalized = new Uint8Array(message.byteLength);
    normalized.set(message);
    this.channel.send(normalized);
  }

  private async onMessage(rawData: string | Blob | ArrayBuffer | ArrayBufferView): Promise<void> {
    const data = await normalizeMessageData(rawData);
    if (typeof data === "string") {
      this.processJsonMessage(data);
      return;
    }

    if (data[0] === 0x7b) {
      this.processJsonMessage(this.textDecoder.decode(data));
      return;
    }

    if (data[0] === 0x66) {
      const chunkId = parseChunkId(data.subarray(0, 8));
      const chunk = data.subarray(8);
      await this.emitAsync("chunk_content", chunkId, chunk);
    }
  }

  private processJsonMessage(text: string): void {
    const message = JSON.parse(text) as JsonRpcRequest | JsonRpcResponse | JsonRpcError;
    if ("method" in message) {
      void this.processRequest(message);
      return;
    }

    const future = this.futures.get(message.id);
    if (future === undefined) {
      return;
    }
    this.futures.delete(message.id);

    if ("error" in message) {
      future.reject(new Error(message.error.message));
      return;
    }

    future.resolve(message.result ?? null);
  }

  private async processRequest(request: JsonRpcRequest): Promise<void> {
    try {
      const result = await this.dispatchRequest(request.method, request.params);
      if (request.id === null) {
        return;
      }

      this.channel.send(this.encodeJson({
        id: request.id,
        result: (result as Record<string, unknown> | null) ?? null,
        jsonrpc: "2.0",
      }));
    } catch (error) {
      if (request.id === null) {
        return;
      }

      this.channel.send(this.encodeJson({
        id: request.id,
        error: {
          code: -32000,
          message: error instanceof Error ? error.message : String(error),
          data: null,
        },
        jsonrpc: "2.0",
      }));
    }
  }

  private dispatchRequest(method: string, params: Record<string, unknown>): Promise<unknown> {
    switch (method) {
      case "start_session":
        return this.emitAsync(
          "start_session",
          String(params.session_id),
          Number(params.byte_length),
          String(params.sha256),
          params.mode as Mode,
        );
      case "request_chunk":
        return this.emitAsync(
          "request_chunk",
          String(params.session_id),
          this.normalizeChunkId(params.chunk_id),
          Number(params.byte_offset),
          Number(params.byte_length),
        );
      case "timeout":
        return this.emitAsync("timeout", String(params.session_id));
      default:
        throw new Error(`Unknown method: ${method}`);
    }
  }

  private async emitAsync<EventName extends keyof MessageHandlers>(
    event: EventName,
    ...args: MessageHandlers[EventName]
  ): Promise<unknown> {
    let result: unknown = null;
    const eventHandlers = this.getHandlers(event) as Set<MessageHandler> | undefined;
    if (eventHandlers === undefined || eventHandlers.size === 0) {
      return result;
    }

    for (const handler of eventHandlers) {
      result = await handler(...args);
    }
    return result;
  }

  private encodeJson(payload: JsonRpcRequest | JsonRpcResponse | JsonRpcError): string {
    return JSON.stringify(payload);
  }

  private serializeParams(params: Record<string, unknown>): Record<string, unknown> {
    const serialized: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(params)) {
      serialized[key] = typeof value === "bigint" ? value.toString() : value;
    }
    return serialized;
  }

  private normalizeChunkId(value: unknown): bigint {
    if (typeof value === "bigint") {
      return value;
    }
    if (typeof value === "number") {
      return BigInt(value);
    }
    if (typeof value === "string") {
      return BigInt(value);
    }
    return createChunkId();
  }
}

export type { Mode };