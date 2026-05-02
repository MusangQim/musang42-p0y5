#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    # check kalau data appropriate tak for current data processor
    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass
    # process the input data
    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass
    # output ingested data
    @abstractmethod
    def output(self) -> tuple[int, str]:
        pass


# ingest int, float and lists of both types(include mix-type list)
class NumericProcessor(DataProcessor):
    def ingest(self, data: Any) -> None:
        return super().ingest(data)
    
    def output(self) -> tuple[int, str]:
        return super().output()


class TextProcessor(DataProcessor):
    def ingest(self, data: Any) -> None:
        return super().ingest(data)
    
    def output(self) -> tuple[int, str]:
        return super().output()


class LogProcessor(DataProcessor):
    def ingest(self, data: Any) -> None:
        return super().ingest(data)
    
    def output(self) -> tuple[int, str]:
        return super().output()


def main() -> None:
    print("=== Code Nexus - Data Processor ===\n")



if __name__ == "__main__":
    main()
