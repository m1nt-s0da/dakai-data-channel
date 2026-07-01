import { beforeAll, describe, expect, test } from "vitest";

import { DekaiDataChannel } from "./dekai";

const TEST_TIMEOUT_MS = 1000;

class FakeMessageEvent<T> extends Event {
  constructor(type: string, readonly data: T) {
    super(type);
  }
}

class FakeRTCDataChannel extends EventTarget {
  peer: FakeRTCDataChannel | null = null;
  sendFilter: ((message: string | BufferSource) => boolean) | null = null;
  sendDelay: ((message: string | BufferSource) => number) | null = null;

  send(message: string | BufferSource): void {
    if (this.peer === null) {
      throw new Error("Peer channel is not connected.");
    }
    if (this.sendFilter !== null && !this.sendFilter(message)) {
      return;
    }
    const delay = this.sendDelay?.(message) ?? 0;
    if (delay <= 0) {
      this.peer.dispatchEvent(new FakeMessageEvent("message", message));
      return;
    }
    setTimeout(() => {
      this.peer?.dispatchEvent(new FakeMessageEvent("message", message));
    }, delay);
  }
}

function createChannelPair(): [FakeRTCDataChannel, FakeRTCDataChannel] {
  const left = new FakeRTCDataChannel();
  const right = new FakeRTCDataChannel();
  left.peer = right;
  right.peer = left;
  return [left, right];
}

function isFrameMessage(message: string | BufferSource): boolean {
  if (typeof message === "string") {
    return false;
  }

  if (message instanceof ArrayBuffer) {
    return new Uint8Array(message)[0] === 0x66;
  }

  return new Uint8Array(message.buffer, message.byteOffset, message.byteLength)[0] === 0x66;
}

function isStartSessionResponseMessage(message: string | BufferSource): boolean {
  if (typeof message !== "string") {
    return false;
  }
  return message.includes('"result":null') && !message.includes('"method":"start_session"');
}

function parseJsonMessage(message: string | BufferSource): Record<string, unknown> | null {
  if (typeof message !== "string") {
    return null;
  }
  return JSON.parse(message) as Record<string, unknown>;
}

beforeAll(() => {
  Object.assign(globalThis, { window: globalThis });
});

describe("DekaiDataChannel (TypeScript)", () => {
  test("sends text to buffered receiver", async () => {
    const [senderChannel, receiverChannel] = createChannelPair();
    const sender = new DekaiDataChannel(senderChannel as unknown as RTCDataChannel, 32, 0.2);
    const receiver = new DekaiDataChannel(receiverChannel as unknown as RTCDataChannel, 32, 0.2);

    const received = new Promise<string>((resolve, reject) => {
      receiver.on("start_receiving", (receiving) => {
        void receiving.buffered().join().then((data) => {
          resolve(new TextDecoder().decode(data));
        }, reject);
      });
    });

    await Promise.race([
      sender.send("hello buffered world", "text"),
      timeoutPromise(TEST_TIMEOUT_MS, "send timed out"),
    ]);

    await expect(Promise.race([received, timeoutPromise(TEST_TIMEOUT_MS, "receive timed out")])).resolves.toBe(
      "hello buffered world",
    );
  });

  test("emits progress while streaming binary data", async () => {
    const [senderChannel, receiverChannel] = createChannelPair();
    const sender = new DekaiDataChannel(senderChannel as unknown as RTCDataChannel, 24, 0.2);
    const receiver = new DekaiDataChannel(receiverChannel as unknown as RTCDataChannel, 24, 0.2);

    const payload = new Uint8Array(Array.from({ length: 32 }, (_, index) => index));
    const progress: Array<[number, number]> = [];
    const chunks: Uint8Array[] = [];

    const completed = new Promise<void>((resolve, reject) => {
      receiver.on("start_receiving", (receiving) => {
        const stream = receiving.stream();
        stream.on("progress", (receivedBytes, totalBytes) => {
          progress.push([receivedBytes, totalBytes]);
        });

        void (async () => {
          try {
            for await (const chunk of stream) {
              chunks.push(chunk);
            }
            resolve();
          } catch (error) {
            reject(error);
          }
        })();
      });
    });

    await Promise.race([
      sender.send(payload, "binary"),
      timeoutPromise(TEST_TIMEOUT_MS, "send timed out"),
    ]);
    await Promise.race([completed, timeoutPromise(TEST_TIMEOUT_MS, "stream timed out")]);

    expect(progress).toEqual([
      [16, 32],
      [32, 32],
    ]);
    expect(concatChunks(chunks)).toEqual(payload);
  });

  test("fails receiver when chunk content times out", async () => {
    const [senderChannel, receiverChannel] = createChannelPair();
    senderChannel.sendFilter = (message) => !isFrameMessage(message);

    const sender = new DekaiDataChannel(senderChannel as unknown as RTCDataChannel, 24, 0.2);
    const receiver = new DekaiDataChannel(receiverChannel as unknown as RTCDataChannel, 24, 0.05);

    receiver.on("start_receiving", (receiving) => {
      void receiving.buffered().join().catch(() => undefined);
    });

    await expect(
      Promise.race([
        sender.send(new Uint8Array(Array.from({ length: 32 }, (_, index) => index)), "binary"),
        timeoutPromise(TEST_TIMEOUT_MS, "send timed out"),
      ]),
    ).rejects.toThrow(/Timed out waiting for chunk content/);
  });

  test("notifies receiver when sender session times out", async () => {
    const [senderChannel, receiverChannel] = createChannelPair();
    senderChannel.sendFilter = (message) => !isFrameMessage(message);

    const sender = new DekaiDataChannel(senderChannel as unknown as RTCDataChannel, 24, 0.05);
    const receiver = new DekaiDataChannel(receiverChannel as unknown as RTCDataChannel, 24, 0.2);

    const receivedError = new Promise<string>((resolve, reject) => {
      receiver.on("start_receiving", (receiving) => {
        void receiving.buffered().join().then(
          () => reject(new Error("buffered receive unexpectedly succeeded")),
          (error) => resolve(error instanceof Error ? error.message : String(error)),
        );
      });
    });

    await expect(
      Promise.race([
        sender.send(new Uint8Array(Array.from({ length: 32 }, (_, index) => index)), "binary"),
        timeoutPromise(TEST_TIMEOUT_MS, "send timed out"),
      ]),
    ).rejects.toThrow(/Send session timed out/);

    await expect(
      Promise.race([receivedError, timeoutPromise(TEST_TIMEOUT_MS, "receiver error timed out")]),
    ).resolves.toMatch(/Sender abandoned transfer due to timeout/);
  });

  test("waits longer for the final start_session response", async () => {
    const [senderChannel, receiverChannel] = createChannelPair();
    receiverChannel.sendDelay = (message) => isStartSessionResponseMessage(message) ? 100 : 0;

    const sender = new DekaiDataChannel(senderChannel as unknown as RTCDataChannel, 24, 0.05, 0.2);
    const receiver = new DekaiDataChannel(receiverChannel as unknown as RTCDataChannel, 24, 0.05, 0.2);

    const payload = new Uint8Array(Array.from({ length: 32 }, (_, index) => index));
    const received = new Promise<Uint8Array>((resolve) => {
      receiver.on("received", (data) => {
        resolve(data as Uint8Array);
      });
    });

    await expect(
      Promise.race([
        sender.send(payload, "binary"),
        timeoutPromise(TEST_TIMEOUT_MS, "send timed out"),
      ]),
    ).resolves.toBeUndefined();

    await expect(
      Promise.race([received, timeoutPromise(TEST_TIMEOUT_MS, "receive timed out")]),
    ).resolves.toEqual(payload);
  });

  test("sends request_chunk chunk_id as a JSON number", async () => {
    const [senderChannel, receiverChannel] = createChannelPair();
    let serializedChunkIdType: string | null = null;

    receiverChannel.sendFilter = (message) => {
      const json = parseJsonMessage(message);
      if (json?.method !== "request_chunk") {
        return true;
      }
      const params = json.params as Record<string, unknown>;
      serializedChunkIdType = typeof params.chunk_id;
      return true;
    };

    const sender = new DekaiDataChannel(senderChannel as unknown as RTCDataChannel, 32, 0.2);
    const receiver = new DekaiDataChannel(receiverChannel as unknown as RTCDataChannel, 32, 0.2);

    receiver.on("start_receiving", (receiving) => {
      void receiving.buffered().join().catch(() => undefined);
    });

    await expect(
      Promise.race([
        sender.send("hello buffered world", "text"),
        timeoutPromise(TEST_TIMEOUT_MS, "send timed out"),
      ]),
    ).resolves.toBeUndefined();

    expect(serializedChunkIdType).toBe("number");
  });
});

function timeoutPromise(timeoutMs: number, message: string): Promise<never> {
  return new Promise((_, reject) => {
    setTimeout(() => reject(new Error(message)), timeoutMs);
  });
}

function concatChunks(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0);
  const output = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return output;
}