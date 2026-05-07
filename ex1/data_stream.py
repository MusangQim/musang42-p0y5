#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any


class DataStream(ABC):
    def __init__(self) -> None:
        self._processors = []
    

    def register_processor(self, proc: DataProcessor) -> None:
        self._proc = proc.append(DataProcessor)


    def process_stream(self, stream: list[typing.Any]) -> None:
        for element in stream:
            routed = False
            for processor in _processor

    def print_processors_stats(self) -> None:


def main() -> None:
    print("=== Code Nexus - Data Stream ===\n")
    stream = DataStream()
    print("Initialize Data Stream...")
    print("== DataStream statisctics ==")
    stream.print_processor_stats()

    print("Registering Numeric Processor\n")
    stream.register_processor(NumericProcessor())
    n1_batch = ['Hello world', [3.14, -1, 2.71],
                [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'},
                 {'log_level': 'INFO', 'log_message': 'User wil is connected'}],
                 42, ['Hi', 'five']]
    print(f"Send first batch of data on stream: {n1_batch}")
    stream.process_stream(n1_batch)
    print("== DataStream statistics ==")
    stream.print_processors_stats()

    
if __name__ == "__main__":
    main()