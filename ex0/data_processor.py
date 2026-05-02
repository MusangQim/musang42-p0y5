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
        try:
            raise TypeError("Improper numeric data")
        except TypeError as e:
            print(f"Test invalid ingestion of string {data} without prior validation: {e}")


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        else:
            return False
    
    def ingest(self, data: Any) -> None:
        return super().ingest(data)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        return super().validate(data)
    
    def ingest(self, data: Any) -> None:
        return super().ingest(data)
    
    def output(self) -> tuple[int, str]:
        return super().output()


def main() -> None:
    print("=== Code Nexus - Data Processor ===\n")



if __name__ == "__main__":
    main()
