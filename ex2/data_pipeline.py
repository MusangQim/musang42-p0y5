#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[tuple[int, str]] = []
        self._rank = 0

    # check kalau data appropriate tak for current data processor
    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    # process the input data
    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    # output ingested data
    def output(self) -> tuple[int, str]:
        item_saved: tuple[int, str] = self._storage.pop(0)
        return item_saved


# ingest int, float and lists of both types(include mix-type list)
class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, int):
            return True
        elif isinstance(data, float):
            return True
        elif isinstance(data, list):
            for datalist in data:
                check_int = isinstance(datalist, int)
                check_float = isinstance(datalist, float)
                if not check_int and not check_float:
                    return False
            return True
        else:
            return False

    def ingest(self, data: Any) -> None:
        if self.validate(data):
            if isinstance(data, list):
                for item in data:
                    self._storage.append((self._rank, str(item)))
                    self._rank += 1
                else:
                    self._storage.append((self._rank, str(data)))
                    self._rank += 1
        else:
            raise TypeError("Improper numeric data")


# ingest str and lists of str
class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list):
            for textlist in data:
                if not isinstance(textlist, str):
                    return False
            return True
        else:
            return False

    def ingest(self, data: Any) -> None:
        if self.validate(data):
            if isinstance(data, list):
                for item in data:
                    self._storage.append((self._rank, str(item)))
                    self._rank += 1
                else:
                    self._storage.append((self._rank, str(data)))
                    self._rank += 1
        else:
            raise TypeError("Improper text data")


# ingest dict and lists of dict
class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return True
        elif isinstance(data, list):
            for loglist in data:
                if not isinstance(loglist, dict):
                    return False
            return True
        else:
            return False

    def ingest(self, data: Any) -> None:
        if self.validate(data):
            if isinstance(data, dict):
                format_dict = f"{data['log_level']}: {data['log_message']}"
                self._storage.append((self._rank, format_dict))
                self._rank += 1
            elif isinstance(data, list):
                for item in data:
                    format_list = f"{item['log_level']}: {item['log_message']}"
                    self._storage.append((self._rank, format_list))
                    self._rank += 1
        else:
            raise TypeError("Improper log data")


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            routed = False
            for processor in self._processors:
                if processor.validate(element) is True:
                    processor.ingest(element)
                    routed = True
                    break
            if not routed:
                print(f"DataStream error - Can't process"
                      f" element in stream: {element}")

    def print_processors_stats(self) -> None:
        if not self._processors:
            print("No processor found, no data")
        for processor in self._processors:
            name = type(processor).__name__
            total = processor._rank
            remaining = len(processor._storage)
            print(f"{name}: total {total} items processed,"
                  f" remaining {remaining} on processor")


def main() -> None:
    print("=== Code Nexus - Data Stream ===\n")
    stream = DataStream()
    print("Initialize Data Stream...\n")
    print("== DataStream statisctics ==")
    stream.print_processors_stats()
# === Processors running.... ===
    print("\nRegistering Processors\n")
    stream.register_processor(NumericProcessor())
    stream.register_processor(TextProcessor())
    stream.register_processor(LogProcessor())
    log_batch = [{'log_level': 'WARNING',
                  'log_message': 'Telnet access! Use ssh instead'},
                 {'log_level': 'INFO',
                  'log_message': 'User wil is connected'}
                 ]
    n1_batch = [
        'Hello world',
        [3.14, -1, 2.71],
        log_batch,
        42,
        ['Hi', 'five']
    ]
    print(f"Send first batch of data on stream: {n1_batch}\n")
    stream.process_stream(n1_batch)
    print("== DataStream statistics ==")
    stream.print_processors_stats()


if __name__ == "__main__":
    main()
