import tarfile
import gzip
import io
import unittest
from main import lines_from_tar_member_with_suffix

class TestMain(unittest.TestCase):
    def test_lines_from_tar_member_with_suffix(self):
        # Make some compressed test data.
        sample_data = "col1\tcol2\nval1\tval2\nval3\tval4\n"
        test_file = io.BytesIO(gzip.compress(sample_data.encode('utf-8')))
        # Add it to an in-memory tar file in a BytesIO stream.
        in_memory_tar = io.BytesIO()
        tar_info = tarfile.TarInfo(name='test_suffix.gz')
        tar_info.size = len(test_file.getvalue())
        tar = tarfile.TarFile(fileobj=in_memory_tar, mode='w')
        tar.addfile(tar_info, test_file)
        tar.close()
        in_memory_tar.seek(0)

        # Open a new tar file from the in-memory data and test it.
        test_tar = tarfile.open(fileobj=in_memory_tar, mode='r')        
        result = list(lines_from_tar_member_with_suffix(test_tar, 'suffix.gz'))

        # Expected results are the two lines from the test file, with the header
        # line column names as keys.
        expected_result = [
            {'col1': 'val1', 'col2': 'val2'},
            {'col1': 'val3', 'col2': 'val4'}
        ]

        # Results should match.
        self.assertEqual(result, expected_result)
        # Just to be nice.
        test_tar.close()

        # Now, try with a non-matching suffix.
        in_memory_tar.seek(0)
        test_tar = tarfile.open(fileobj=in_memory_tar, mode='r')        
        result = list(lines_from_tar_member_with_suffix(test_tar, 'does_not_match'))
        # Results should be empty.
        self.assertEqual(result, [])
        # Just to be nice.
        test_tar.close()


if __name__ == '__main__':
    unittest.main()