<?php

namespace SimpleEnergy\WebHDFS;

//use SimpleEnergy\WebHDFS\Curl as Curl;

/**
 * Class HDFS
 */
class HDFS
{

    /**
     * construct
     *
     * @param string $host
     * @param string $port
     * @param string $user
     */
    public function __construct($host, $port, $user)
    {
        $this->host = $host;
        $this->port = $port;
        $this->user = $user;
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
     */
    public function concat($path, $sources)
    {
        $url = $this->_buildUrl($path, array('op' => 'CONCAT', 'sources' => $sources));

        return Curl::post($url);
    }

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
     * @return mixed
     */
    public function mkdirs($path, $permission = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'MKDIRS', 'permission' => $permission));

        return Curl::put($url);
    }

    /**
     * Create a symbolic link
     *
     * @param string $path
     * @param string $destination
     * @param string $createParent
     * @return mixed
     */
    public function createSymLink($path, $destination, $createParent = '')
    {
        $url = $this->_buildUrl($destination, array('op' => 'CREATESYMLINK', 'destination' => $path, 'createParent' => $createParent));

        return Curl::put($url);
    }

    /**
     * Rename a path
     *
     * @param string $path
     * @param string $destination
     * @return mixed
     */
    public function rename($path, $destination)
    {
        $url = $this->_buildUrl($path, array('op' => 'RENAME', 'destination' => $destination));

        return Curl::put($url);
    }

    /**
     * Delete a path
     *
     * @param string $path
     * @param string $recursive
     * @return mixed
     */
    public function delete($path, $recursive = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'DELETE', 'recursive' => $recursive));

        return Curl::delete($url);
    }

    /**
     * Get file status
     *
     * @param string $path
     * @return mixed
     */
    public function getFileStatus($path)
    {
        $url = $this->_buildUrl($path, array('op' => 'GETFILESTATUS'));

        return Curl::get($url);
    }

    /**
     * List status
     *
     * @param string $path
     * @return mixed
     */
    public function listStatus($path)
    {
        $url = $this->_buildUrl($path, array('op' => 'LISTSTATUS'));

        return Curl::get($url);
    }

    /**
     * Get content summary
     *
     * @param string $path
     * @return mixed
     */
    public function getContentSummary($path)
    {
        $url = $this->_buildUrl($path, array('op' => 'GETCONTENTSUMMARY'));

        return Curl::get($url);
    }

    /**
     * Get file's checksum
     *
     * @param string $path
     * @return mixed
     */
    public function getFileChecksum($path)
    {
        $url = $this->_buildUrl($path, array('op' => 'GETFILECHECKSUM'));

        return Curl::getWithRedirect($url);
    }

    /**
     * Get home directory
     *
     * @return mixed
     */
    public function getHomeDirectory()
    {
        $url = $this->_buildUrl('', array('op' => 'GETHOMEDIRECTORY'));

        return Curl::get($url);
    }

    /**
     * Set Permission on a path
     *
     * @param string $path
     * @param string $permission
     *
     * @return mixed
     */
    public function setPermission($path, $permission)
    {
        $url = $this->_buildUrl($path, array('op' => 'SETPERMISSION', 'permission' => $permission));

        return Curl::put($url);
    }

    /**
     * Set owner for a path
     *
     * @param string $path
     * @param string $owner
     * @param string $group
     *
     * @return mixed
     */
    public function setOwner($path, $owner = '', $group = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'SETOWNER', 'owner' => $owner, 'group' => $group));

        return Curl::put($url);
    }

    /**
     * Set Replication for a path
     *
     * @param string $path
     * @param string $replication
     *
     * @return mixed
     */
    public function setReplication($path, $replication)
    {
        $url = $this->_buildUrl($path, array('op' => 'SETREPLICATION', 'replication' => $replication));

        return Curl::put($url);
    }

    /**
     * Set mofication and accesstime for a path
     *
     * @param string $path
     * @param string $modificationTime
     * @param string $accessTime
     *
     * @return mixed
     */
    public function setTimes($path, $modificationTime = '', $accessTime = '')
    {
        $url = $this->_buildUrl($path, array('op' => 'SETTIMES', 'modificationtime' => $modificationTime, 'accesstime' => $accessTime));

        return Curl::put($url);
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
        if ($path[0] == '/') {
            $path = substr($path, 1);
        }

        $queryData['user.name'] = $this->user;

        return 'http://' . $this->host . ':' . $this->port . '/webhdfs/v1/' . $path . '?' . http_build_query(array_filter($queryData));
    }

}
