#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any


class DataStream(ABC):
    def __init__(self) -> None:
    
    def register_processor(self, proc: DataProcessor) -> None:

    def process_stream(self, streamL list[typing.Any]) -> None:

    def print_processors_stats(self) -> None:


def main() -> None:

if __name__ == "__main__":
    main()