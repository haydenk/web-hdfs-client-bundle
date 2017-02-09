<?php

namespace jimbglenn\webHDFSClientBundle\HDFS;

use jimbglenn\webHDFSClientBundle\HDFS\Curl as Curl;

/**
 * Class HDFSClient
 */
class HDFSClient
{

    private $serverName;
    private $port;
    private $user;

    /**
     * Construct
     *
     * @param string|null $serverName
     * @param string|null $port
     * @param string|null $user
     */
    public function __construct($serverName = null, $port = null, $user = null)
    {

        $this->serverName = $serverName;
        $this->port = $port;
        $this->user = $user;

        if (!$this->isHDFSOnline()) {
            throw new \RuntimeException("Could not connect to HDFS");
        }
    }

    /**
     * @param string $serverName
     */
    public function setServerName($serverName)
    {
        $this->serverName = $serverName;
    }

    /**
     * @return null|string
     */
    public function getServerName()
    {
        return $this->serverName;
    }

    /**
     * @param string $port
     */
    public function setPort($port)
    {
        $this->port = $port;
    }

    /**
     * @return null|string
     */
    public function getPort()
    {
        return $this->port;
    }

    /**
     * @param string $user
     */
    public function setUser($user)
    {
        $this->user = $user;
    }

    /**
     * @return null|string
     */
    public function getUser()
    {
        return $this->user;
    }
    /**
     * Create a file at path
     *
     * @param string $path
     * @param string $filename
     *
     * @return bool
     */
    public function create($path, $filename)
    {
        if (!file_exists($filename)) {
            return false;
        }

        $url = $this->_buildUrl($path, array('op' => 'CREATE'));
        $redirectUrl = Curl::putLocation($url);

        return Curl::putFile($redirectUrl, $filename);
    }

    /**
     * Do some basic tests to verify we are online before we loose info
     *
     * @return bool
     */
    public function isHDFSOnline()
    {
        if ($this->serverName === null) {
            return false;
        }
        if ($this->user === null) {
            return false;
        }

        if (! $this->ping($this->serverName)) {
            return false;
        }

        if (! $this->verifyRootExists()) {
            return false;
        }

            return true;

    }

    /**
     * Verify the root exists (and that we can get to it)
     *
     * @return bool
     */
    private function verifyRootExists()
    {
        $fileStatus = $this->getFileStatus("/");
        if (!is_array($fileStatus) || !array_key_exists("FileStatus", $fileStatus)) {
            return false;
        } else {
            $root = $fileStatus["FileStatus"];
        }

        return (strtolower($root["type"]) == "directory");
    }

    /**
     * Verify we can ping a servername
     *
     * @param $serverName
     * @return bool
     */
    private function ping($serverName)
    {
        exec(sprintf('ping -c 1 -W 5 %s', escapeshellarg($serverName)), $res, $rval);

        return $rval === 0;
    }

    /**
     * Append to a file
     *
     * @param string $path
     * @param string $string
     * @param string $bufferSize
     *
     * @return bool
     */
    public function append($path, $string, $bufferSize = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'APPEND', 'buffersize' => $bufferSize));
        $redirectUrl = Curl::postLocation($url);

        return Curl::postString($redirectUrl, $string);
    }

    /**
     * Concat sources at path
     * @param string $path
     * @param string $sources
     * @return mixed
     *
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
    /*
    public function concat($path, $sources)
    {
        $url = $this->_buildUrl($path, array('op' => 'CONCAT', 'sources' => $sources));

        return Curl::post($url);
    }
    */

    /**
     * Open a file
     *
     * @param string $path
     * @param string $offset
     * @param string $length
     * @param string $bufferSize
     * @return mixed
     */
    public function open($path, $offset = '', $length = '', $bufferSize = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'OPEN', 'offset' => $offset, 'length' => $length, 'buffersize' => $bufferSize));

        return Curl::getWithRedirect($url);
    }

    /**
     * Make directories
     *
     * @param string $path
     * @param string $permission
     * @return boolean
     */
    public function mkdirs($path, $permission = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'MKDIRS', 'permission' => $permission));
        $result = json_decode(Curl::put($url), true);

        return $result["boolean"];
    }

    /**
     * Create a symbolic link
     *
     * @param string $path
     * @param string $destination
     * @param string $createParent
     * @return mixed
     *
     * Symlink functionality appears to no longer work in Hadoop or WebHDFS
     * We get an error from Hadoop of
     * {"RemoteException":{"exception":"UnsupportedOperationException",
     *                     "javaClassName":"java.lang.UnsupportedOperationException",
     *                      "message":"Symlinks not supported"}}
     *
     * Research shows this may come back:
     * https://issues.apache.org/jira/browse/HDFS-4933
     *
     * Therefore, we've commented it out but left code in case it
     * is fixed in the future.
     */
    /*public function createSymLink($path, $destination, $createParent = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'CREATESYMLINK', 'destination' => $destination, 'createParent' => $createParent));

        return Curl::put($url);
    }
    */

    /**
     * Rename a path
     *
     * @param string $path
     * @param string $destination
     * @return boolean
     */
    public function rename($path, $destination)
    {
        $url = $this->_buildUrl($path, array('op' => 'RENAME', 'destination' => $destination));
        $result = json_decode(Curl::put($url), true);

        return $result["boolean"];
    }

    /**
     * Delete a path
     *
     * @param string $path
     * @param string $recursive
     * @return boolean
     */
    public function delete($path, $recursive = '')
    {
        if ($recursive == true) {
            $recursive = "true";
        }
        $url = $this->_buildUrl($path, array('op' => 'DELETE', 'recursive' => $recursive));
        $result = json_decode(Curl::delete($url), true);

        return ($result["boolean"] == 1);
    }

    /**
     * Get file status
     *
     * @param string $path
     * @return array
     */
    public function getFileStatus($path)
    {
        $url = $this->_buildUrl($path, array('op' => 'GETFILESTATUS'));

        return json_decode(Curl::get($url), true);
    }

    /**
     * List status
     *
     * @param string $path
     * @return array
     */
    public function listStatus($path)
    {
        $url = $this->_buildUrl($path, array('op' => 'LISTSTATUS'));

        return json_decode(Curl::get($url), true);
    }

    /**
     * Get content summary
     *
     * @param string $path
     * @return array
     */
    public function getContentSummary($path)
    {
        $url = $this->_buildUrl($path, array('op' => 'GETCONTENTSUMMARY'));

        return  json_decode(Curl::get($url), true);
    }

    /**
     * Get file's checksum
     *
     * @param string $path
     * @return array
     */
    public function getFileChecksum($path)
    {
        $url = $this->_buildUrl($path, array('op' => 'GETFILECHECKSUM'));

        return json_decode(Curl::getWithRedirect($url), true);
    }

    /**
     * Get home directory
     *
     * @return string
     */
    public function getHomeDirectory()
    {
        $url = $this->_buildUrl('', array('op' => 'GETHOMEDIRECTORY'));
        $result =  json_decode(Curl::get($url), true);

        return $result["Path"];
    }

    /**
     * check if fille/folder exists
     * @param string $path
     * @param string $flag exists|file|folder
     *
     * @return boolean
     */
    public function doesExists($path, $flag = "exists")
    {
        $result = $this->getFileStatus($path);

        if (strtolower($flag) == "file") {
            return (array_key_exists("FileStatus", $result) && $result["FileStatus"]["type"] == "FILE");
        } else if (strtolower($flag) == "directory") {
            return (array_key_exists("FileStatus", $result) && $result["FileStatus"]["type"] == "DIRECTORY");
        };
        if ((array_key_exists("FileStatus", $result) && $result["FileStatus"]["type"] == "FILE") ||
            (array_key_exists("FileStatus", $result) && $result["FileStatus"]["type"] == "DIRECTORY")) {
            return true;
        }

        return false;
    }

    /**
     * Set Permission on a path
     *
     * @param string $path
     * @param string $permission
     *
     * @return boolean
     */
    public function setPermission($path, $permission)
    {
        $url = $this->_buildUrl($path, array('op' => 'SETPERMISSION', 'permission' => $permission));

        return Curl::putWithReturnSuccess($url);
    }

    /**
     * Set owner for a path
     *
     * @param string $path
     * @param string $owner
     * @param string $group
     *
     * @return boolean
     */
    public function setOwner($path, $owner = '', $group = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'SETOWNER', 'owner' => $owner, 'group' => $group));

        return Curl::putWithReturnSuccess($url);
    }

    /**
     * Set Replication for a path
     *
     * @param string $path
     * @param string $replication
     *
     * @return boolean
     */
    public function setReplication($path, $replication)
    {
        $url = $this->_buildUrl($path, array('op' => 'SETREPLICATION', 'replication' => $replication));
        $result =  json_decode(Curl::put($url), true);

        return $result["boolean"];
    }

    /**
     * Set mofication and accesstime for a path
     * WARNING: Access time requires additional setup in hdfs config
     *
     * @param string $path
     * @param string $modificationTime
     * @param string $accessTime
     *
     * @return boolean
     */
    public function setTimes($path, $modificationTime = '', $accessTime = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'SETTIMES', 'modificationtime' => $modificationTime, 'accesstime' => $accessTime));

        return Curl::putWithReturnSuccess($url);
    }

    /**
     * Private function to build the URL to match the current WDFS api
     *
     * @param string $path
     * @param string $queryData
     *
     * @return string
     */
    private function _buildUrl($path, $queryData)
    {
        if (!empty($path) && $path[0] == '/') {
            $path = substr($path, 1);
        }

        $queryData['user.name'] = $this->user;

        return 'http://' . $this->serverName . ':' . $this->port . '/webhdfs/v1/' . $path . '?' . http_build_query(array_filter($queryData));
    }

}
