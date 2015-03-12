<?php

namespace jimbglenn\webHDFSClientBundle\HDFS;

/**
 * Class Curl
 * provide basic curl functions for webhdfs interactions
 */
class Curl
{

    /**
     * @param string $url
     * @return mixed;
     */
    public static function getWithRedirect($url)
    {
        return self::get($url, array(CURLOPT_FOLLOWLOCATION => true));
    }

    /**
     * @param string $url
     * @param array  $options
     * @return mixed
     */
    public static function get($url, $options = array())
    {
        $options[CURLOPT_URL] = $url;
        $options[CURLOPT_RETURNTRANSFER] = true;

        return self::_exec($options);
    }

    /**
     * @param string $url
     * @return mixed
     */
    public static function putLocation($url)
    {
        return self::_findRedirectUrl($url, array(CURLOPT_PUT => true));
    }

    /**
     * @param string $url
     * @return mixed
     */
    public static function postLocation($url)
    {
        return self::_findRedirectUrl($url, array(CURLOPT_POST => true));
    }

    /**
     * @param $url
     * @param $options
     * @return mixed
     */
    private static function _findRedirectUrl($url, $options)
    {
        $options[CURLOPT_URL] = $url;
        $info = self::_exec($options, true);

        return $info['redirect_url'];
    }

    /**
     * @param string $url
     * @param string $filename
     * @return bool
     */
    public static function putFile($url, $filename)
    {
        $options[CURLOPT_URL] = $url;
        $options[CURLOPT_PUT] = true;
        $handle = fopen($filename, "r");
        $options[CURLOPT_INFILE] = $handle;
        $options[CURLOPT_INFILESIZE] = filesize($filename);

        $info = self::_exec($options, true);

        return ('201' == $info['http_code']);
    }

    /**
     * @param string $url
     * @param string $string
     * @return bool
     */
    public static function postString($url, $string)
    {
        $options[CURLOPT_URL] = $url;
        $options[CURLOPT_POST] = true;
        $options[CURLOPT_POSTFIELDS] = $string;

        $info = self::_exec($options, true);

        return ('200' == $info['http_code']);
    }

    /**
     * @param string $url
     * @return mixed
     */
    public static function put($url)
    {
        $options = array();
        $options[CURLOPT_URL] = $url;
        $options[CURLOPT_PUT] = true;
        $options[CURLOPT_RETURNTRANSFER] = true;

        return self::_exec($options);
    }

    /**
     * returns whether put was successful
     *
     * @param string $url
     * @return boolean
     */
    public static function putWithReturnSuccess($url)
    {
        $options = array();
        $options[CURLOPT_URL] = $url;
        $options[CURLOPT_PUT] = true;
        $options[CURLOPT_RETURNTRANSFER] = true;

        $info = self::_exec($options, true);

        return ('200' == $info['http_code']);
    }

    /**
     * @param string $url
     * @return mixed
     */
    public static function post($url)
    {
        $options = array();
        $options[CURLOPT_URL] = $url;
        $options[CURLOPT_POST] = true;
        $options[CURLOPT_RETURNTRANSFER] = true;

        return self::_exec($options);
    }

    /**
     * @param string $url
     * @return mixed
     */
    public static function delete($url)
    {
        $options = array();
        $options[CURLOPT_URL] = $url;
        $options[CURLOPT_CUSTOMREQUEST] = "DELETE";
        $options[CURLOPT_RETURNTRANSFER] = true;

        return self::_exec($options);
    }

    /**
     * @param $options
     * @param bool $returnInfo
     * @return mixed
     */
    private static function _exec($options, $returnInfo = false)
    {
        $ch = curl_init();
        curl_setopt_array($ch, $options);
        $result = curl_exec($ch);

        if ($returnInfo) {
            $result = curl_getinfo($ch);
        }

        curl_close($ch);

        return $result;
    }

}
