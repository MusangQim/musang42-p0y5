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
        item_saved = self._storage.pop(0)
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
            self._storage.append((self._rank, str(data)))
            self._rank += 1
        else:
            raise TypeError("Improper numeric data")


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
            self._storage.append((self._rank, str(data)))
            self._rank += 1
        else:
            raise TypeError("Improper text data")


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
        data_1 = {'log_level': 'NOTICE', 'log_message': 'Connection to server'}
        data_2 = {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
        if self.validate(data):
            self._storage.append((self._rank, str(data)))
            self._rank += 1
        else:
            raise TypeError("Improper log data")


def main() -> None:
    print("=== Code Nexus - Data Processor ===\n")


if __name__ == "__main__":
    main()
