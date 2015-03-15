<?php

namespace jimbglenn\webHDFSClientBundle\Tests\HDFS;

use jimbglenn\webHDFSClientBundle\HDFS\HDFSClient;
use Symfony\Bundle\FrameworkBundle\Test\KernelTestCase;

/**
 * Class HDFSClientTest
 */
class HDFSClientTest extends KernelTestCase
{

    /** @var  HDFSClient $hdfs */
    private $hdfs;

    /** @var  string $directory */
    private $directory;

    /** @var string $remoteDir */
    private $remoteDir;

    /** var string $localFile */
    private $localFile;

    /** var string $remoteFile */
    private $remoteFile;

    /**
     * setup the test
     * get container and setup the directory for tests
     */
    public function setUp()
    {
        self::bootKernel();
        $this->directory = "tests";

        $this->hdfs = static::$kernel->getContainer()->get('web_hdfs_client');

        if (!$this->hdfs) {
            $this->fail("Can't connect to HDFS, no reason to run tests");
        }

        // setup test file paths
        $this->remoteDir = "user/" . $this->hdfs->getUser() . "/" . $this->directory;
        $this->localFile = dirname(__FILE__) . "/testFile.txt";
        $this->remoteFile = $this->remoteDir . "/new-file.txt";


        $this->hdfs->mkdirs($this->remoteDir);

    }

    /**
     * tear down
     */
    public function tearDown()
    {
        // cleanup if we had access to the HDFS
        if ($this->hdfs) {
            $this->hdfs->delete($this->remoteDir, true);
        }
    }

    /**
     * test hdfs is online function
     */
    public function testIsHDFSOnline()
    {
        // in container aware code, it should be:
        // $hdfs = $this->get('web_hdfs_client');
        // for the tests, it's setup in the setUp() function
        /** @var HDFSClient $hdfs */
        $hdfs = $this->hdfs;
        $this->assertTrue($hdfs->isHDFSOnline());

    }

    /**
     * test create and write file function
     */
    public function testCreateAndWriteAFile()
    {
        // setup
        /** @var HDFSClient $hdfs */
        $hdfs = $this->hdfs;

        // test
        $result = $hdfs->create($this->remoteFile, $this->localFile);

        // cleanup
        $hdfs->delete($this->remoteFile);

        // asserts
        $this->assertEquals($result, true, "Could not create/write a remote file");

    }

    /**
     * test append to file
     */
    public function testAppendToFile()
    {
        // setup
        /** @var HDFSClient $hdfs */
        $hdfs = $this->hdfs;
        $hdfs->create($this->remoteFile, $this->localFile);
        $appendData = file_get_contents($this->localFile);

        // test
        $result = $hdfs->append($this->remoteFile, $appendData);

        // cleanup
        $hdfs->delete($this->remoteFile);

        // asserts
        $this->assertEquals($result, 1, "Could not append to remote file");

    }

    /**
     * test concat file
     * We get an error from Hadoop of
     * {"RemoteException":{"exception":"HadoopIllegalArgumentException",
     *                      "javaClassName":"org.apache.hadoop.HadoopIllegalArgumentException",
     *                      "message":"The last block in /user/hadoop/test/concat-file.txt is not full;
     *                      last block size = 3276 but file block size = 134217728"}}
     *
     * Research shows this may come back:
     * https://issues.apache.org/jira/browse/HDFS-6641
     *
     * Therefore, it does not seem like a useful function and has been commented out
     */
    public function NOTRUNNINGtestConcatFile()
    {
        // setup
        /** @var HDFSClient $hdfs */
        $hdfs = $this->hdfs;
        $secondRemoteFile = $this->remoteDir . "/new-file2.txt";
        $concatFile = $this->remoteDir . "/concat-file.txt";
        $hdfs->create($this->remoteFile, $this->localFile);
        $hdfs->create($secondRemoteFile, $this->localFile);
        $inputFileList = "/" . $this->remoteFile . ",/" .  $secondRemoteFile;

        // test
        $result = $hdfs->concat($concatFile, $inputFileList);

        // cleanup
        $hdfs->delete($this->remoteFile);
        $hdfs->delete($secondRemoteFile);
        $hdfs->delete($concatFile);

        // asserts
        $this->assertEquals($result, 1, "Could not concat remote files");
    }

    /**
     * test open read file
     */
    public function testOpenReadFile()
    {
        // setup
        /** @var HDFSClient $hdfs */
        $hdfs = $this->hdfs;
        $hdfs->create($this->remoteFile, $this->localFile);
        $expectedResult = file_get_contents($this->localFile);

        // test
        $result = $hdfs->open($this->remoteFile);

        // cleanup
        $hdfs->delete($this->remoteFile);

        // asserts
        $this->assertEquals($result, $expectedResult, "Local File and Remote File were not the same.");
    }

    /**
     * test make directory
     */
    public function testMakeDirectory()
    {
        // setup
        /** @var HDFSClient $hdfs */
        $hdfs = $this->hdfs;
        $testDir = $this->remoteDir . "/makeDirectoryTest";

        // test
        $result = $hdfs->mkdirs($testDir);

        // cleanup
        $hdfs->delete($testDir);

        // asserts
        $this->assertEquals($result, true, "Could not make directory");
    }

    /**
     * test create sym link
     * Symlink functionality appears to no longer work in Hadoop or WebHDFS
     * We get an error from Hadoop of
     * {"RemoteException":{"exception":"UnsupportedOperationException",
     *                     "javaClassName":"java.lang.UnsupportedOperationException",
     *                      "message":"Symlinks not supported"}}
     *
     * Research shows this may come back:
     * https://issues.apache.org/jira/browse/HDFS-4933
     *
     * Therefore, we've renamed this test so it doesn't run but left code in case it
     * is fixed in the future.
     */
    /*
    public function testCreateSymbolicLink()
    {
        // setup
        $hdfs = $this->hdfs;
        $desiredFolderPath = "/" . $hdfs->getUser() . "/symLinkedDir";
        $desiredFilePath = "/" . $this->remoteDir . "/symLinkedFile";

        // test
        $folderResult = $hdfs->createSymLink($this->remoteDir, $desiredFolderPath);
        $fileResult = $hdfs->createSymLink($this->remoteFile, $desiredFilePath);

        // cleanup
        $hdfs->delete($desiredFolderPath);
        $hdfs->delete($fileResult);

        // asserts
        $this->assertEquals($folderResult, true, "Could not symlink directory");
        $this->assertEquals($fileResult, true, "Could not symlink file");
    }
    */

    /**
     * test rename file
     */
    public function testRenameFileDirectory()
    {
        // setup
        /** @var HDFSClient $hdfs */
        $hdfs = $this->hdfs;
        $desiredFolderPath = "user/" . $hdfs->getUser() . "/renamedDir";
        $desiredFilePath = $this->remoteDir . "/renamedFile.txt";
        $hdfs->create($this->remoteFile, $this->localFile);


        // test file
        $fileResult = $hdfs->rename($this->remoteFile, "/" . $desiredFilePath);

        // cleanup test file
        $hdfs->delete($desiredFilePath);

        // test folder
        $folderResult = $hdfs->rename($this->remoteDir, "/" . $desiredFolderPath);

        // cleanup test folder
        $hdfs->rename($desiredFolderPath, "/" . $this->remoteDir);


        // asserts
        $this->assertEquals($folderResult, true, "Could not rename directory");
        $this->assertEquals($fileResult, true, "Could not rename file");
    }

    /**
     * test delete file directory
     */
    public function testDeleteFileDirectory()
    {
        // setup
        /** @var HDFSClient $hdfs */
        $hdfs = $this->hdfs;
        $desiredFolderPath = "user/" . $hdfs->getUser() . "/deleteDirTest";
        $desiredFilePath = $this->remoteDir . "/deleteFileTest";

        $hdfs->mkdirs($desiredFolderPath);
        $hdfs->create($desiredFilePath, $this->localFile);

        // test
        $folderResult = $hdfs->delete($desiredFolderPath);
        $fileResult = $hdfs->delete($desiredFilePath);

        // asserts
        $this->assertEquals($folderResult, true, "Could not delete directory");
        $this->assertEquals($fileResult, true, "Could not delete file");
    }

    /**
     * test status file directory
     */
    public function testStatusFileDirectory()
    {
        // setup
        /** @var HDFSClient $hdfs */
        $hdfs = $this->hdfs;
        $hdfs->create($this->remoteFile, $this->localFile);

        // test file
        $fileResult = $hdfs->getFileStatus($this->remoteFile);

        // cleanup test file
        $hdfs->delete($this->remoteFile);

        // test folder
        $folderResult = $hdfs->getFileStatus($this->remoteDir);

        // cleanup test folder


        // asserts
        $this->assertEquals($folderResult["FileStatus"]["type"], "DIRECTORY", "Could not get file status for a directory");
        $this->assertEquals($fileResult["FileStatus"]["type"], "FILE", "Could not get file status for a file");
    }

    /**
     * test whether file/directory exists
     */
    public function testDoesExists()
    {
        // setup
        /** @var HDFSClient $hdfs */
        $hdfs = $this->hdfs;
        $hdfs->create($this->remoteFile, $this->localFile);

        // tests
        $fileResult = $hdfs->doesExists($this->remoteFile, "file");
        $fileResult2 = $hdfs->doesExists($this->remoteFile);
        $folderResult = $hdfs->doesExists($this->remoteDir, "directory");
        $folderResult2 = $hdfs->doesExists($this->remoteDir);
        $folderResult3 = $hdfs->doesExists($this->remoteDir . "/asdfasfd");

        // cleanup test file
        $hdfs->delete($this->remoteFile);

        // asserts
        $this->assertTrue($fileResult, "Could not get file exists for file");
        $this->assertTrue($fileResult2, "Could not get second file exists for file");
        $this->assertTrue($folderResult, "Could not get file exists for folder");
        $this->assertTrue($folderResult2, "Could not get second file exists for folder");
        $this->assertNotTrue($folderResult3, "Could not get third file exists for folder");
    }

    /**
     * test list directory
     */
    public function testListDirectory()
    {
        // setup
        /** @var HDFSClient $hdfs */
        $hdfs = $this->hdfs;
        $hdfs->create($this->remoteFile, $this->localFile);

        // test
        $folderResult = $hdfs->listStatus($this->remoteDir);

        // cleanup
        $hdfs->delete($this->remoteFile);


        // asserts
        $this->assertEquals($folderResult["FileStatuses"]["FileStatus"][0]["type"], "FILE", "Could not get list directory");
    }

    /**
     * test content summary directory
     */
    public function testGetContentSummaryDirectory()
    {
        // setup
        /** @var HDFSClient $hdfs */
        $hdfs = $this->hdfs;
        $hdfs->create($this->remoteFile, $this->localFile);

        // test
        $folderResult = $hdfs->getContentSummary($this->remoteDir);

        // cleanup
        $hdfs->delete($this->remoteFile);


        // asserts
        $this->assertEquals($folderResult["ContentSummary"]["fileCount"], 1, "Could not get directory summary");
    }

    /**
     * Verify getHomeDirectory() works correctly
     */
    public function testGetHomeDirectory()
    {
        // setup
        /** @var HDFSClient $hdfs */
        $hdfs = $this->hdfs;
        $expected = "/user/" . $hdfs->getUser();

        // test
        $result = $hdfs->getHomeDirectory();

        // asssert
        $this->assertEquals($result, $expected, "Home directory did not match expected path.");
    }

    /**
     * test set permission
     */
    public function testSetPermssion()
    {
        // setup
        /** @var HDFSClient $hdfs */
        $hdfs = $this->hdfs;
        $hdfs->create($this->remoteFile, $this->localFile);

        // test
        $folderResult = $hdfs->setPermission($this->remoteFile, "777");

        // cleanup
        $hdfs->delete($this->remoteFile);


        // asserts
        $this->assertTrue($folderResult, "Could not set permission");
    }

    /**
     * test set owner
     */
    public function testSetOwner()
    {
        // setup
        /** @var HDFSClient $hdfs */
        $hdfs = $this->hdfs;
        $hdfs->create($this->remoteFile, $this->localFile);

        // test
        $folderResult = $hdfs->setOwner($this->remoteFile, $hdfs->getUser());

        // cleanup
        $hdfs->delete($this->remoteFile);


        // asserts
        $this->assertTrue($folderResult, "Could not set permission");
    }

    /**
     * test set replication factor
     */
    public function testSetReplicationFactor()
    {
        // setup
        /** @var HDFSClient $hdfs */
        $hdfs = $this->hdfs;
        $hdfs->create($this->remoteFile, $this->localFile);

        // test
        $folderResult = $hdfs->setReplication($this->remoteFile, 1);

        // cleanup
        $hdfs->delete($this->remoteFile);


        // asserts
        $this->assertEquals($folderResult, true, "Could not set replication factor");
    }

    /**
     * test set accesss/modified time
     * access time requires additional configuration in hdfs config
     */
    public function testSetAccessModifiedTime()
    {
        // setup
        /** @var HDFSClient $hdfs */
        $hdfs = $this->hdfs;
        $hdfs->create($this->remoteFile, $this->localFile);

        // test
        $folderResult = $hdfs->setTimes($this->remoteFile, time());

        // cleanup
        $hdfs->delete($this->remoteFile);


        // asserts
        $this->assertTrue($folderResult, "Could not set access or modified time");
    }
}