import tarfile
import gzip
import io
import unittest
from archive_readers import TarMemberReader
from unittest.mock import patch, MagicMock
from main import make_species_code_map

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
        reader = TarMemberReader(test_tar, 'suffix.gz')
        result = list(reader.lines())

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
        with self.assertRaises(ValueError):
            reader = TarMemberReader(test_tar, 'does_not_match')       
        # Just to be nice.
        test_tar.close()

    def test_lines_from_tar_member_with_suffix_no_files(self):
        # Create an empty tar file.
        in_memory_tar = io.BytesIO()
        tar = tarfile.TarFile(fileobj=in_memory_tar, mode='w')
        tar.close()
        in_memory_tar.seek(0)

        # Open a new tar file from the in-memory data and test it.
        test_tar = tarfile.open(fileobj=in_memory_tar, mode='r')
        with self.assertRaises(ValueError):
            _ = TarMemberReader(test_tar, 'suffix.gz')
        
        # Just to be nice.
        test_tar.close()

    def test_lines_from_tar_member_with_suffix_multiple_files(self):
        # Make some compressed test data.
        sample_data1 = "col1\tcol2\nval1\tval2\nval3\tval4\n"
        test_file1 = io.BytesIO(gzip.compress(sample_data1.encode('utf-8')))
        sample_data2 = "col1\tcol2\nval5\tval6\nval7\tval8\n"
        test_file2 = io.BytesIO(gzip.compress(sample_data2.encode('utf-8')))
        
        # Add them to an in-memory tar file in a BytesIO stream.
        in_memory_tar = io.BytesIO()
        tar = tarfile.TarFile(fileobj=in_memory_tar, mode='w')
        
        tar_info1 = tarfile.TarInfo(name='test_suffix1.gz')
        tar_info1.size = len(test_file1.getvalue())
        tar.addfile(tar_info1, test_file1)
        
        tar_info2 = tarfile.TarInfo(name='test_suffix2.gz')
        tar_info2.size = len(test_file2.getvalue())
        tar.addfile(tar_info2, test_file2)
        
        tar.close()
        in_memory_tar.seek(0)

        # Open a new tar file from the in-memory data and test it.
        test_tar = tarfile.open(fileobj=in_memory_tar, mode='r')
        reader = TarMemberReader(test_tar, 'suffix2.gz')
        result = list(reader.lines())

        # Expected results are the two lines from the second test file, 
        # with the header line column names as keys.
        expected_result = [
            {'col1': 'val5', 'col2': 'val6'},
            {'col1': 'val7', 'col2': 'val8'}
        ]

        # Results should match.
        self.assertEqual(result, expected_result)
        # Just to be nice.
        test_tar.close()


    @patch('main.open_connection')
    def test_make_species_code_map(self, mock_open_connection: MagicMock):
        # Mock the db connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_open_connection.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Make a fake map of names to code.
        mock_cursor.fetchall.return_value = [
            ('Scientific Name 1', 'species_code_1'),
            ('Scientific Name 2', 'species_code_2')
        ]

        # Call the function
        result = make_species_code_map()

        # Expected result
        expected_result = {
            'Scientific Name 1': 'species_code_1',
            'Scientific Name 2': 'species_code_2'
        }

        # Assert the result matches the expected result
        self.assertEqual(result, expected_result)

        # Ensure the correct SQL query was used
        mock_cursor.execute.assert_called_once_with("SELECT scientific_name, species_code FROM species")

if __name__ == '__main__':
    unittest.main()