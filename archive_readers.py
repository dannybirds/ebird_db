from abc import ABC, abstractmethod
import csv
import gzip
import io
import tarfile
from typing import IO, Any, Generator
import zipfile


class ArchiveMemberReader(ABC):
    def __init__(self):
        self.last_bytes_read = 0

    @property
    @abstractmethod
    def file_name(self) -> str:
        pass

    @property
    @abstractmethod
    def file_size(self) -> int:
        pass

    @property
    def last_bytes_read(self) -> int:
        return self._last_bytes_read
    
    @last_bytes_read.setter
    def last_bytes_read(self, value: int):
        self._last_bytes_read = value

    @abstractmethod
    def lines(self) -> Generator[dict[str, str|None], None, None]:
        pass

    @abstractmethod
    def close(self):
        pass

    def __enter__(self):
        return self
    
    def __exit__(self, *args: list[str], **kwargs: dict[str,Any]):
        self.close()

class TarMemberReader(ArchiveMemberReader):
    def __init__(self, tar_file: tarfile.TarFile, suffix: str):
        self.tar_file = tar_file
        self.member_file: IO[bytes] | None = None
        for member in tar_file.getmembers():
            if member.name.endswith(suffix):
                self.member_info = member
                self.member_file = tar_file.extractfile(member)
                
        if self.member_file is None:
              raise ValueError(f"No member with suffix {suffix} found in tar file")
        
        self.dict_reader = csv.DictReader(io.TextIOWrapper(gzip.GzipFile(fileobj=self.member_file)), delimiter='\t')
        self.current_offset = self.member_file.tell()
        self.effective_file_size = self.member_info.size - self.current_offset
        self.last_bytes_read = 0

    @property
    def file_name(self) -> str:
        return self.member_info.name

    @property
    def file_size(self) -> int:
        return self.effective_file_size

    def lines(self) -> Generator[dict[str, str|None], None, None]:
        assert self.member_file is not None
        for line in self.dict_reader:
            self.last_bytes_read = self.member_file.tell() - self.current_offset
            self.current_offset = self.member_file.tell()
            yield line

    def close(self):
        self.tar_file.close()

class ZipReader(ArchiveMemberReader):
    def __init__(self, zip_file: zipfile.ZipFile, suffix: str):
        self.zip_file = zip_file
        self.member_file: IO[bytes] | None = None
        for member in zip_file.infolist():
            print(member)
            if member.filename.endswith(suffix):
                self.member_info = member
                self.member_file = zip_file.open(member)

        if self.member_file is None:
            raise ValueError(f"No member with suffix {suffix} found in zip file")

        self.dict_reader = csv.DictReader(io.TextIOWrapper(self.member_file), delimiter='\t')
        self.current_offset = self.member_file.tell()
        self.last_bytes_read = 0

    @property
    def file_name(self) -> str:
        return self.member_info.filename

    @property
    def file_size(self) -> int:
        return self.member_info.file_size

    def lines(self) -> Generator[dict[str, str|None], None, None]:
        assert self.member_file is not None
        for line in self.dict_reader:
            self.last_bytes_read = self.member_file.tell() - self.current_offset
            self.current_offset = self.member_file.tell()
            yield line
    
    def close(self):
        self.zip_file.close()

def get_archive_member_reader(archive_path: str, suffix: str) -> ArchiveMemberReader:
    if archive_path.endswith('.tar'):
        return TarMemberReader(tarfile.open(archive_path, 'r'), suffix)
    elif archive_path.endswith('.zip'): 
        return ZipReader(zipfile.ZipFile(archive_path, 'r'), suffix)
    else:
        raise ValueError(f"Unsupported archive format: {archive_path}")
    
def get_sampling_file_archive_member_reader(archive_path: str) -> ArchiveMemberReader:
    if archive_path.endswith('.tar'):
        return get_archive_member_reader(archive_path, '_sampling.txt.gz')
    elif archive_path.endswith('.zip'):
        return get_archive_member_reader(archive_path, '_sampling.txt')
    else:
        raise ValueError(f"Unsupported archive format: {archive_path}")
    

def get_observations_file_archive_member_reader(archive_path: str) -> ArchiveMemberReader:
    filebase = f'{archive_path.split('-')[-1].split('.')[0]}'
    if archive_path.endswith('.tar'):
        return get_archive_member_reader(archive_path, f'{filebase}.txt.gz')
    elif archive_path.endswith('.zip'):
        return get_archive_member_reader(archive_path, f'{filebase}.txt')
    else:
        raise ValueError(f"Unsupported archive format: {archive_path}")  
