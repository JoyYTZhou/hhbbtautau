import unittest
from unittest.mock import MagicMock, patch
from src.utils.filesysutil import XRootDHelper

class TestXRootDHelper(unittest.TestCase):

    @patch('src.utils.filesysutil.client.FileSystem')
    def setUp(self, MockFileSystem):
        self.mock_fs = MockFileSystem.return_value
        self.helper = XRootDHelper()
        self.helper.xrdfs_client = self.mock_fs

    def setUpMockDirList(self, filenames):
        filelist = []
        for name in filenames:
            mock_file = MagicMock()
            mock_file.name = name
            filelist.append(mock_file)
        self.mock_fs.dirlist.return_value = (MagicMock(ok=True), MagicMock(dirlist=filelist))

    def test_glob_files_all_files(self):
        self.setUpMockDirList(['file1.txt', 'file2.txt'])
        self.mock_fs.stat.return_value = (MagicMock(ok=True), None)

        files = self.helper.glob_files('/some/dir')

        self.mock_fs.dirlist.assert_called_once_with('/some/dir')

        self.assertTrue(all(isinstance(file, str) for file in files))
        self.assertEqual(files, ['file1.txt', 'file2.txt'])

    def test_glob_files_pattern(self):
        self.setUpMockDirList(['file1.txt', 'file2.log'])
        self.mock_fs.stat.return_value = (MagicMock(ok=True), None)

        files = self.helper.glob_files('/some/dir', '*.txt')

        self.mock_fs.dirlist.assert_called_once_with('/some/dir')
        self.assertEqual(files, ['file1.txt'])
    
    def test_check_path_exists(self):
        self.mock_fs.stat.return_value = (MagicMock(ok=True), None)

        result = self.helper.check_path('/some/dir')

        self.mock_fs.stat.assert_called_once_with('/some/dir')
        self.assertTrue(result)
    
    def test_check_path_not_exists_create(self):
        self.mock_fs.stat.return_value = (MagicMock(ok=False), None)
        self.mock_fs.mkdir.return_value = (MagicMock(ok=True), None)

        result = self.helper.check_path('/some/dir') 

        self.mock_fs.stat.assert_called_once_with('/some/dir')
        self.mock_fs.mkdir.assert_called_once_with('/some/dir')
        self.assertFalse(result)
    
    def test_check_path_not_exists_raise_error(self):
        self.mock_fs.stat.return_value = (MagicMock(ok=False), None)

        with self.assertRaises(FileNotFoundError):
            self.helper.check_path('/some/dir', raiseError=True)
        
    def test_remove_all_files(self):
        self.setUpMockDirList(['file1.txt', 'file2.txt'])

        self.mock_fs.rm.return_value = (MagicMock(ok=True), None)
        self.mock_fs.stat.return_value = (MagicMock(ok=True), None)

        self.helper.remove_files('/some/dir')

        self.mock_fs.dirlist.assert_called_once_with('/some/dir')
        self.mock_fs.rm.assert_any_call('/some/dir/file1.txt')
        self.mock_fs.rm.assert_any_call('/some/dir/file2.txt')
    
    def test_remove_files_no_files(self):
        self.mock_fs.stat.return_value = (MagicMock(ok=False), None)
        self.helper.remove_files('/some/dir', "*.txt")
        
        self.mock_fs.dirlist.assert_not_called()
        self.mock_fs.rm.assert_not_called()
    
    def test_remove_files_with_patterns(self):
        self.setUpMockDirList(['file1.txt', 'file2.log', 'file3.txt'])
        self.mock_fs.rm.return_value = (MagicMock(ok=True), None)
        self.mock_fs.stat.return_value = (MagicMock(ok=True), None)
    
        self.helper.remove_files('/some/dir', pattern='*.txt')
    
        self.mock_fs.dirlist.assert_called_once_with('/some/dir')
    
        self.mock_fs.rm.assert_any_call('/some/dir/file1.txt')
        self.mock_fs.rm.assert_any_call('/some/dir/file3.txt')
    
        calls = [call[0][0] for call in self.mock_fs.rm.call_args_list]
        self.assertNotIn('/some/dir/file2.log', calls)

    @patch('src.utils.filesysutil.glob.glob')
    def test_transfer_files_copy(self, mock_glob):
        mock_glob.return_value = ['/local/dir/file1.txt', '/local/dir/file2.txt']
        self.mock_fs.stat.return_value = (MagicMock(ok=True), None)
        self.mock_fs.copy.return_value = (MagicMock(ok=True), None)

        self.helper.transfer_files('/local/dir', '/store/user/dir', '*.txt', remove=False)

        self.assertEqual(self.mock_fs.copy.call_count, 2)
        self.mock_fs.copy.assert_any_call('/local/dir/file1.txt', '/store/user/dir/file1.txt', force=False)
        self.mock_fs.copy.assert_any_call('/local/dir/file2.txt', '/store/user/dir/file2.txt', force=False)

        self.helper.transfer_files('/local/dir', '/store/user/dir', '*.txt', remove=False, overwrite=True)

        self.assertEqual(self.mock_fs.copy.call_count, 4)  # Called twice more
        self.mock_fs.copy.assert_any_call('/local/dir/file1.txt', '/store/user/dir/file1.txt', force=True)
        self.mock_fs.copy.assert_any_call('/local/dir/file2.txt', '/store/user/dir/file2.txt', force=True)

    def test_transfer_files_source_path_error(self):
        with self.assertRaises(ValueError):
            self.helper.transfer_files('/store/user/dir', '/store/user/dir')

   
if __name__ == '__main__':
    unittest.main()