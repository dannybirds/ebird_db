import csv
import gzip
import io
import tarfile
from typing import IO, Generator


class TarMemberReader:
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
        self.last_bytes_read = 0

    def lines(self) -> Generator[dict[str, str|None], None, None]:
        assert self.member_file is not None
        for line in self.dict_reader:
            self.last_bytes_read = self.member_file.tell() - self.current_offset
            self.current_offset = self.member_file.tell()
            yield line
        
    