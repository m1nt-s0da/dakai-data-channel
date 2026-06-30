export type EventHandler<Args extends unknown[]> = (...args: Args) => void;

export class EventEmitter<EventMap extends Record<string, unknown[]>> {
  protected handlers = new Map<keyof EventMap, Set<EventHandler<any>>>();

  on<EventName extends keyof EventMap>(
    event: EventName,
    handler: EventHandler<EventMap[EventName]>,
  ): void {
    let eventHandlers = this.handlers.get(event);
    if (eventHandlers === undefined) {
      eventHandlers = new Set();
      this.handlers.set(event, eventHandlers);
    }
    eventHandlers.add(handler as EventHandler<any>);
  }

  off<EventName extends keyof EventMap>(
    event: EventName,
    handler: EventHandler<EventMap[EventName]>,
  ): void {
    this.handlers.get(event)?.delete(handler as EventHandler<any>);
  }

  protected emit<EventName extends keyof EventMap>(
    event: EventName,
    ...args: EventMap[EventName]
  ): void {
    const eventHandlers = this.handlers.get(event);
    if (eventHandlers === undefined) {
      return;
    }

    for (const handler of eventHandlers) {
      handler(...args);
    }
  }

  protected getHandlers<EventName extends keyof EventMap>(
    event: EventName,
  ): Set<EventHandler<EventMap[EventName]>> | undefined {
    return this.handlers.get(event) as Set<EventHandler<EventMap[EventName]>> | undefined;
  }
}