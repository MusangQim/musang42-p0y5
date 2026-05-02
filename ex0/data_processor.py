#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self, storage: list) -> None:
        self._storage = storage
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
        return pass


# ingest int, float and lists of both types(include mix-type list)
class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, int):
            return True
        elif isinstance(data,float):
            return True
        elif isinstance(data, list):
            for datalist in data:
                if datalist != int and datalist != float:
                    return False
                else:
                    return True
        else:
            return False

    def ingest(self, data: Any) -> None:
        return super().ingest(data)
    
    def output(self) -> tuple[int, str]:
        return super().output()


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        return super().validate(data)
    
    def ingest(self, data: Any) -> None:
        return super().ingest(data)
    
    def output(self) -> tuple[int, str]:
        return super().output()


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
