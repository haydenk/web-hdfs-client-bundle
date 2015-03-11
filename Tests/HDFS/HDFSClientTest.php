<?php

namespace jimbglenn\webHDFSClientBundle\Tests\HDFS;

use jimbglenn\webHDFSClientBundle\HDFS\HDFSClient;
use Symfony\Bundle\FrameworkBundle\Test\KernelTestCase;

/**
 * Class HDFSClientTest
 */
class HDFSClientTest extends KernelTestCase
{

    private $container;
    private $directory;

    /**
     * setup the test
     * get container and setup the directory for tests
     */
    public function setUp()
    {
        self::bootKernel();
        $this->container = static::$kernel->getContainer();
        $this->directory = "tests";

    }

    /**
     * test hdfs is online function
     */
    public function testIsHDFSOnline()
    {
        // in container aware code, it should be:
        // $hdfs = $this->get('web_hdfs_client');
        // for the test, it's:
        $hdfs = $this->container->get('web_hdfs_client');
        $this->assertTrue($hdfs->isHDFSOnline());

    }

    /**
     * test create and write file function
     */
    public function testCreateAndWriteAFile()
    {

        // in container aware code, it should be:
        // $hdfs = $this->get('web_hdfs_client');
        // for the test, it's:
        $hdfs = $this->container->get('web_hdfs_client');

        // setup test file paths
        $remoteDir = "user/" . $hdfs->getUser() . "/" . $this->directory;
        $localFile = dirname(__FILE__) . "/testFile.txt";
        $remoteFile = $remoteDir . "/new-file.txt";

        // prep
        $hdfs->mkdirs($remoteDir);

        // test
        $result = json_decode($hdfs->create($remoteFile, $localFile), true);
        $this->assertEquals($result, 1, "Could not create/write a remote file");

        // cleanup
        $hdfs->delete($remoteFile);
        $hdfs->delete($remoteDir);
    }

    public function appendToFile()
    {

    }

    public function concatFile()
    {

    }

    public function OpenReadFile()
    {

    }

    public function makeDirectory()
    {

    }

    public function createSymbolicLink()
    {

    }

    public function renameFileDirectory()
    {

    }

    public function deleteFileDirectory()
    {

    }

    public function statusFileDirectory()
    {

    }

    public function listDirectory()
    {

    }

    public function getContentSummaryDirectory()
    {

    }

    /**
     * Verify getHomeDirectory() works correctly
     */
    public function testGetHomeDirectory()
    {
        // in container aware code, it should be:
        // $hdfs = $this->get('web_hdfs_client');
        // for the test, it's:
        $hdfs = $this->container->get('web_hdfs_client');

        $result = json_decode($hdfs->getHomeDirectory(), true);
        $expected = "/user/" . $hdfs->getUser();

        $this->assertEquals($result["Path"], $expected, "Home directory did not match expected path.");
    }

    public function setPermssion()
    {

    }

    public function setOwner()
    {

    }

    public function setReplicationFactor()
    {

    }

    public function setAccessModifiedTime()
    {

    }
}