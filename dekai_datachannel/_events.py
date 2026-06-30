from typing import Callable


class EventTarget:
    def __init__(self):
        self.__event_handlers: dict[str, list[Callable]] = {}

    def on(self, event: str, handler: Callable):
        if event not in self.__event_handlers:
            self.__event_handlers[event] = []
        self.__event_handlers[event].append(handler)

    def off(self, event: str, handler: Callable):
        if event in self.__event_handlers:
            self.__event_handlers[event].remove(handler)

    def _emit(self, event: str, *args, **kwargs):
        if event in self.__event_handlers:
            for callback in self.__event_handlers[event]:
                callback(*args, **kwargs)
