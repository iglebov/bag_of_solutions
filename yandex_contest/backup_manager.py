"""
У вас есть некая система, которая позволяет сохранить бэкап и после скачать его в виде потока байтов.
Перед тем, как положить бекап на хранение, его надо сжать и зашифровать. Есть ресурсоемкий алгоритм
сжатия/шифрования, позволяющий (относительно медленно) сжимать/шифровать поток данных. В итоге данные
сжимаются, примерно, в 1.5-3 раза. Сжатый и шифрованный бэкап может занимать до 10 TiB.

Для хранения сжатого и зашифрованного бэкапа используется холодное сетевое хранилище (например, S3),
которое позволяет сохранять наш бекап, но у хранилища есть ограничения на размер файла – размер одного
файла в хранилище не может превышать 100 MiB. Для каждого нового бекапа создается фолдер в хранилище.
Фолдер может содержать только 1 бэкап.

  +---+-----+-----+-----+-----+-----+-----+      +-----+  +-----+
  | 7 |  6  |  5  |  4  |  3  |  2  |  1  |----->|  1  |  |  5  |
  +---+-----+-----+-----+-----+-----+-----+      +-----+  +-----+
                                 |               +-----+  +-----+
                                 +-------------->|  2  |  |  6  |
                                                 +-----+  +-----+
                                                 +-----+  +---+
                                                 |  3  |  | 7 |
                                                 +-----+  +---+
                                                 +-----+
                                                 |  4  |
                                                 +-----+

Необходимо реализовать функции для сохранения бэкапа в сетевое хранилище и восстановления обратно.
"""

import asyncio
import concurrent.futures
import io
import time
from abc import ABC, abstractmethod
from datetime import datetime

import pytest


class Processor(ABC):
    @abstractmethod
    def compress_and_encrypt(self, data: bytes) -> bytes: ...

    @abstractmethod
    def decrypt_and_uncompress(self, data: bytes) -> bytes: ...


class Folder(ABC):
    MAX_FILE_SIZE = 100 * 1024 * 1024

    @abstractmethod
    async def write_file(self, name: str, data: bytes): ...

    @abstractmethod
    async def read_file(self, name: str) -> bytes: ...

    @abstractmethod
    async def list_files(self) -> list[str]: ...


class BackupManager:
    def __init__(self, folder: Folder, processor: Processor):
        self._folder = folder
        self._processor = processor

    async def backup(self, in_stream: io.BufferedIOBase) -> None:
        loop = asyncio.get_running_loop()
        tasks = []
        counter = 0
        with concurrent.futures.ProcessPoolExecutor() as pool:
            while data := in_stream.read(self._folder.MAX_FILE_SIZE):
                timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
                name = f"{timestamp}_{counter:06d}"
                counter += 1
                task = asyncio.create_task(self.process_chunk(loop, pool, data, name))
                tasks.append(task)

            if tasks:
                await asyncio.gather(*tasks)

    async def process_chunk(self, loop, pool, data, name):
        encrypted_data = await loop.run_in_executor(
            pool, self._processor.compress_and_encrypt, data
        )
        await self._folder.write_file(name, encrypted_data)

    async def restore(self, out_stream: io.BufferedIOBase) -> None:
        files_names = await self._folder.list_files()
        for file_name in sorted(files_names):
            data = await self._folder.read_file(file_name)
            decrypted_data = self._processor.decrypt_and_uncompress(data)
            out_stream.write(decrypted_data)


class StubFolder(Folder):
    MAX_FILE_SIZE = 1024

    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.delay = 0.02

    async def write_file(self, name: str, data: bytes) -> None:
        if len(data) > self.MAX_FILE_SIZE:
            raise ValueError(
                f"File {name} is too large: {len(data)} bytes > {self.MAX_FILE_SIZE} bytes"
            )
        await asyncio.sleep(self.delay)
        self.files[name] = data

    async def read_file(self, name: str) -> bytes:
        if name not in self.files:
            raise FileNotFoundError(f"File {name} not found")
        await asyncio.sleep(self.delay)
        return self.files[name]

    async def list_files(self) -> list[str]:
        await asyncio.sleep(self.delay)
        return list(reversed(self.files.keys()))


class StubProcessor(Processor):
    def __init__(self, encryption_key: int = 0x55):
        self.encryption_key = encryption_key
        self.delay = 0.02

    def compress_and_encrypt(self, data: bytes) -> bytes:
        if not data:
            return b""

        time.sleep(self.delay)
        return bytes(b ^ self.encryption_key for b in data)

    def decrypt_and_uncompress(self, data: bytes) -> bytes:
        if not data:
            return b""

        time.sleep(self.delay)
        return bytes(b ^ self.encryption_key for b in data)


# Тесты


@pytest.mark.asyncio
async def test_something():
    stream = io.BytesIO(b"abcdef")
    stream_2 = io.BytesIO()
    f = StubFolder()
    p = StubProcessor()
    b_manager = BackupManager(f, p)

    await b_manager.backup(stream)
    await b_manager.restore(stream_2)

    assert stream.getvalue() == stream_2.getvalue()


@pytest.mark.asyncio
async def test_many_files():
    f = StubFolder()
    p = StubProcessor()
    b_manager = BackupManager(f, p)

    stream = io.BytesIO(b"abcde" * f.MAX_FILE_SIZE * 3)
    stream_2 = io.BytesIO()

    await b_manager.backup(stream)
    await b_manager.restore(stream_2)

    assert stream.getvalue() == stream_2.getvalue()
