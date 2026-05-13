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


def main() -> None:
    print("=== Code Nexus - Data Processor ===\n")
    # === NUMERIC ===
    print("Testing Numeric Processor...")
    num_only = NumericProcessor()
    print(f" Trying to validate input '42': {num_only.validate(42)}")
    print(f" Trying to validate input 'Hello': {num_only.validate("Hello")}")
    print(" Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num_only.ingest("foo")
    except TypeError as e:
        print(f" Got exception: {e}")
    print(" Processing data: [1, 2, 3, 4, 5]")
    num_only.ingest([1, 2, 3, 4, 5])
    print(" Extracting 3 values...")
    for i in range(3):
        result_num = num_only.output()
        print(f" Numeric value {result_num[0]}: {result_num[1]}")
    print()
    # === TEXT ===
    print("Testing Text Processor...")
    text_only = TextProcessor()
    print(f" Trying to validate input '42': {text_only.validate(42)}")
    print(" Processing data: ['Hello', 'Nexus', 'World']")
    text_only.ingest(["Hello", "Nexus", "World"])
    print(" Extracting 1 value...")
    for i in range(1):
        result_text = text_only.output()
        print(f" Text value {result_text[0]}: {result_text[1]}")
    print()
    # === LOG ===
    print("Testing Log Processor...")
    log_only = LogProcessor()
    print(f" Trying to validate input 'Hello': {log_only.validate('Hello')}")
    print(" Processing data: [{'log_level': 'NOTICE',"
          " 'log_message': 'Connection to server'},"
          " {'log_level': 'ERROR',"
          " 'log_message': 'Unauthorized access!!'}]")
    log_only.ingest([{'log_level': 'NOTICE',
                      'log_message': 'Connection to server'},
                     {'log_level': 'ERROR',
                      'log_message': 'Unauthorized access!!'}])
    print(" Extracting 2 values...")
    for i in range(2):
        result_log = log_only.output()
        print(f" Log value {result_log[0]}: {result_log[1]}")
    print()


if __name__ == "__main__":
    main()
